//! 게이트웨이가 gRPC 로 부르는 일: 입장(Join)과 말하기(Say).
//!
//! 무엇을 받고 무엇을 돌려주는지는 proto/chat.proto 에 적혀 있다.
//! 그 파일에서 만들어진 코드가 "Chat 이라는 일 묶음에는 join 과 say 가 있어야 한다"고 정해 주고,
//! 이 파일은 그 두 함수의 속을 채운다.
use std::sync::Arc;

use tonic::{Request, Response, Status};

use chat_server::bus;
use chat_server::db;
use chat_server::kafka::{self, Record};
use chat_server::proto::chat_server::Chat;
use chat_server::store::StoreError;
use chat_server::proto::{ChatMessage, JoinReply, JoinRequest, SayReply, SayRequest};
use chat_server::to_line;

use crate::state::AppState;

/// gRPC 요청을 처리하는 쪽. PostgreSQL, ScyllaDB, NATS, Kafka 를 쓰므로 묶음을 들고 있는다.
pub struct ChatService {
    pub state: Arc<AppState>,
}

/// DB 가 실패했을 때. 자세한 내용은 로그에만 남기고 게이트웨이에는 "안쪽 오류"라고만 알린다.
/// Status 는 gRPC 의 실패 응답이다. HTTP 의 응답 번호(401, 404...)처럼 종류가 정해져 있다.
fn db_error(error: sqlx::Error) -> Status {
    println!("[서버 오류] {error}");
    Status::internal("서버 오류")
}

/// ScyllaDB 가 실패했을 때. 위와 같다.
fn store_error(error: StoreError) -> Status {
    println!("[서버 오류] {error}");
    Status::internal("서버 오류")
}

// 아래 줄은 "ChatService 가 Chat 의 일을 할 줄 안다"는 선언이고, 중괄호 안이 그 내용이다.
// #[tonic::async_trait] 은 이런 선언 안에서 async 함수를 쓸 수 있게 해 주는 표시다.
#[tonic::async_trait]
impl Chat for ChatService {
    /// 입장. 게이트웨이가 WebSocket 연결을 맺기 전에 부른다.
    /// 입장권의 주인과 방을 확인하고, 그 방의 최근 대화를 함께 돌려준다.
    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinReply>, Status> {
        // 요청은 포장(Request)에 싸여 온다. 포장을 벗겨 내용만 꺼낸다.
        let request = request.into_inner();
        let db = &self.state.db;

        // 입장권의 주인이 누구인지 찾는다. 없으면 "누구인지 확인되지 않았다"(HTTP 의 401 에 해당).
        let name = db::find_session_name(db, &request.token)
            .await
            .map_err(db_error)?
            .ok_or_else(|| Status::unauthenticated("로그인이 필요합니다"))?;

        // 그런 방이 정말 있는지 확인한다. 없으면 "그런 것은 없다"(HTTP 의 404 에 해당).
        let room_name = db::find_room_name(db, request.room_id)
            .await
            .map_err(db_error)?
            .ok_or_else(|| Status::not_found("그런 방이 없습니다"))?;

        // 이 방의 최근 대화를 ScyllaDB 에서 읽는다. 전부가 아니라 가장 최근 몇 개만이다.
        // 방금 한 말이 아직 안 들어갔을 수 있다. 저장 담당이 넣기 전이면 여기에 없다.
        let recent: Vec<ChatMessage> = self
            .state
            .store
            .load_recent_messages(request.room_id)
            .await
            .map_err(store_error)?
            .into_iter()
            .map(|(seq, name, body)| ChatMessage { seq, name, body })
            .collect();

        println!(
            "[입장] {name} → {room_name:?} 방 (방 번호 {}), 지난 대화 {}개",
            request.room_id,
            recent.len()
        );
        Ok(Response::new(JoinReply { name, room_name, recent }))
    }

    /// 말하기. 게이트웨이가 브라우저에게서 메시지를 받을 때마다 부른다.
    async fn say(&self, request: Request<SayRequest>) -> Result<Response<SayReply>, Status> {
        let request = request.into_inner();
        let (room_id, name, body) = (request.room_id, request.name, request.body);

        // 순번을 직접 매긴다. DB 에 넣고 받아 오던 것을 이제 기다리지 않는다. (seq.rs)
        let seq = self.state.seq.next();

        // Kafka 에 적는다. 저장소(ScyllaDB)에 넣는 일은 저장 담당이 이것을 읽어서 따로 한다.
        // 여기서 저장소를 기다리지 않으므로, 저장소가 느리거나 꺼져 있어도 말하기는 늦어지지 않는다.
        // 적지 못한 메시지는 내보내지 않는다. 내보내고 나서 남기지 못하면,
        // 지금 본 사람과 나중에 온 사람이 다른 대화를 보게 된다.
        let record = Record { seq, room_id, name, body };
        kafka::append(&self.state.kafka, &record).await.map_err(|error| {
            println!("[말하기] {}: Kafka 에 적지 못해 내보내지 않음: {error}", record.name);
            Status::unavailable("메시지를 남기지 못했습니다")
        })?;
        let Record { name, body, .. } = record;

        // 순번과 보낸 사람을 붙여 JSON 한 줄로 만들어 NATS 에 내보낸다.
        // 모든 게이트웨이가 받아 자기에게 붙은 사람들에게 뿌린다. 말한 본인도 이 길로 자기 말을 받는다.
        match bus::publish(&self.state.nats, room_id, to_line(seq, &name, &body)).await {
            // 성공은 "NATS 로 가는 줄에 세웠다"는 뜻이다. 게이트웨이에 도착했다는 뜻이 아니다.
            Ok(()) => println!("[말하기] {name}: {body:?} → 순번 {seq}, Kafka 에 적고 NATS 에 내보냄"),
            // 적기는 했는데 내보내지 못했다. 지금 접속한 사람은 못 보고, 다시 들어오면 저장소에서 읽어 본다.
            // 남기는 일은 끝났으므로 게이트웨이에는 성공으로 답한다.
            Err(error) => println!("[말하기] {name}: 순번 {seq} 적기는 했으나 NATS 에 내보내지 못함: {error}"),
        }
        Ok(Response::new(SayReply { seq }))
    }
}
