//! ScyllaDB 와 주고받는 일. 메시지를 넣고 읽는 일은 전부 이 파일에만 있다.
//!
//! 저장 담당은 넣고(save_message), 채팅 서버는 읽는다(load_...).
//! 계정·입장권·방은 여기 없다. 그것들은 PostgreSQL 에 그대로 있다. (db.rs)
//!
//! ScyllaDB 는 PostgreSQL 과 생각하는 순서가 반대다.
//! - PostgreSQL: 표를 먼저 만들고, 나중에 아무 질문이나 던진다. DB 가 알아서 찾아 준다.
//! - ScyllaDB: 어떤 질문을 던질지 먼저 정하고, 그 질문에 맞게 표를 만든다. 다른 질문은 거의 못 한다.
//!
//! 우리가 던질 질문은 하나다. "이 방의, 이 순번보다 앞의 메시지 50개."
//! 그래서 표를 이렇게 만든다.
//!
//!   PRIMARY KEY ((room_id), seq)
//!
//! - 앞의 (room_id) 는 묶는 기준이다(파티션 키). 같은 방의 메시지를 한 덩어리로 모아 둔다.
//!   방 번호를 알면 그 덩어리가 어디 있는지 바로 안다. 다른 방의 메시지는 들여다보지 않는다.
//! - 뒤의 seq 는 덩어리 안에서 줄 세우는 기준이다(클러스터링 키). 순번 순서대로 나란히 놓인다.
//!   그래서 "가장 최근 50개"는 덩어리의 한쪽 끝에서 50개를 집어 오면 끝난다.
use std::error::Error;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

/// ScyllaDB 주소. docker-compose.yml 에 적은 포트와 같아야 한다.
/// SCYLLA_NODE 환경 변수가 있으면 그 값을 대신 쓴다.
const DEFAULT_SCYLLA_NODE: &str = "127.0.0.1:9042";

/// 한 번에 보내 주는 지난 메시지의 수.
/// 전부 보내면 메시지가 쌓일수록 들어오는 데 오래 걸린다. 한 묶음씩 끊어 보낸다.
const PAGE_SIZE: i32 = 50;

/// 이 파일의 함수들이 실패했을 때 돌려주는 것. 실패의 종류가 여러 가지라 "아무 실패나"로 묶었다.
/// Box<dyn Error> 는 "어떤 종류든 실패 하나를 담은 상자"다. Send + Sync 는 다른 일꾼에게 넘겨도 된다는 표시다.
pub type StoreError = Box<dyn Error + Send + Sync>;

/// 메시지 저장소. 접속과, 미리 준비해 둔 명령 두 개를 들고 있는다.
pub struct Store {
    session: Session,
    /// 메시지 하나를 넣는 명령.
    insert: PreparedStatement,
    /// 한 방에서 어떤 순번보다 앞의 메시지를 읽는 명령.
    select_before: PreparedStatement,
}

impl Store {
    /// ScyllaDB 에 접속하고, 필요한 표가 없으면 만든다.
    pub async fn connect() -> Result<Self, StoreError> {
        let node = std::env::var("SCYLLA_NODE").unwrap_or_else(|_| DEFAULT_SCYLLA_NODE.to_string());
        let session = SessionBuilder::new().known_node(node).build().await?;

        // 키스페이스(keyspace)는 표들을 담는 큰 상자다. PostgreSQL 의 DB 이름(chat)에 해당한다.
        // replication_factor 는 복사본을 몇 대에 둘지다. ScyllaDB 가 한 대뿐이라 1 이다.
        session
            .query_unpaged(
                "CREATE KEYSPACE IF NOT EXISTS chat
                 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
                (),
            )
            .await?;

        // 메시지 표. PRIMARY KEY 의 뜻은 이 파일 맨 위에 적었다.
        // CLUSTERING ORDER BY (seq DESC) 는 덩어리 안에서 순번이 큰 것(최근 것)을 앞에 둔다는 뜻이다.
        // 우리는 늘 최근 것부터 읽으므로 그쪽을 앞에 둔다.
        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS chat.messages (
                    room_id bigint,
                    seq     bigint,
                    name    text,
                    body    text,
                    PRIMARY KEY ((room_id), seq)
                 ) WITH CLUSTERING ORDER BY (seq DESC)",
                (),
            )
            .await?;

        // 자주 쓰는 명령은 미리 준비해 둔다(prepare). DB 가 명령을 한 번만 읽고 해석해 두므로,
        // 그 뒤로는 ? 자리에 들어갈 값만 보내면 된다.
        let insert = session
            .prepare("INSERT INTO chat.messages (room_id, seq, name, body) VALUES (?, ?, ?, ?)")
            .await?;
        // ORDER BY 가 없다. 표를 만들 때 이미 순번이 큰 것부터 놓이게 했으므로 그 순서대로 나온다.
        let select_before = session
            .prepare("SELECT seq, name, body FROM chat.messages WHERE room_id = ? AND seq < ? LIMIT ?")
            .await?;

        Ok(Self { session, insert, select_before })
    }

    /// 메시지 하나를 넣는다.
    ///
    /// 같은 방, 같은 순번의 메시지가 이미 있으면 그 줄을 같은 내용으로 덮어쓴다. 줄이 늘지 않는다.
    /// ScyllaDB 의 INSERT 는 원래 "없으면 넣고 있으면 덮어쓴다"이다.
    /// 그래서 저장 담당이 같은 메시지를 두 번 읽어 두 번 넣어도 한 줄만 남는다.
    pub async fn save_message(&self, room_id: i64, seq: i64, name: &str, body: &str) -> Result<(), StoreError> {
        self.session
            .execute_unpaged(&self.insert, (room_id, seq, name, body))
            .await?;
        Ok(())
    }

    /// 그 방의 가장 최근 메시지 한 묶음을 순번이 작은 것부터 (순번, 이름, 내용) 으로 돌려준다.
    /// 방에 들어올 때 쓴다.
    pub async fn load_recent_messages(&self, room_id: i64) -> Result<Vec<(i64, String, String)>, StoreError> {
        // "가장 큰 수보다 앞의 것"은 곧 "가장 최근 것"이다. i64::MAX 는 i64 가 담을 수 있는 가장 큰 수.
        self.load_messages_before(room_id, i64::MAX).await
    }

    /// 그 방에서 순번이 before 보다 작은 메시지 중 가장 가까운 한 묶음을,
    /// 순번이 작은 것부터 (순번, 이름, 내용) 으로 돌려준다. "이전 대화 더 보기"에 쓴다.
    pub async fn load_messages_before(
        &self,
        room_id: i64,
        before: i64,
    ) -> Result<Vec<(i64, String, String)>, StoreError> {
        let result = self
            .session
            .execute_unpaged(&self.select_before, (room_id, before, PAGE_SIZE))
            .await?
            // 결과에서 줄들을 꺼낼 준비를 한다.
            .into_rows_result()?;

        // 줄마다 (순번, 이름, 내용) 으로 바꿔 모은다. 하나라도 모양이 안 맞으면 실패로 끝낸다.
        let mut rows = Vec::new();
        for row in result.rows::<(i64, String, String)>()? {
            rows.push(row?);
        }
        // 받은 것은 최신이 맨 앞이다. 화면에는 오래된 것부터 보여야 하므로 뒤집는다.
        rows.reverse();
        Ok(rows)
    }
}
