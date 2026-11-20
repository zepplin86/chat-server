//! 서버들 사이의 전달. NATS 와 주고받는 일은 전부 이 파일에만 있다.
//!
//! NATS 는 메시지를 날라 주는 프로그램이다. 누가 "주제"를 정해 메시지를 내보내면(publish),
//! 그 주제를 듣겠다고 한(subscribe) 모두에게 전해 준다. 저장하지 않고 그 자리에서 전하기만 한다.
//!
//! 게이트웨이 안의 방송 채널과 하는 일이 같다. 다른 점은 프로세스 밖에 있다는 것이다.
//! 방송 채널은 한 게이트웨이 안의 연결들을 잇고, NATS 는 프로그램과 프로그램을 잇는다.
//!
//! 내보내는 쪽은 채팅 서버(publish), 받는 쪽은 게이트웨이(relay)다.
use async_nats::Client;
use futures_util::StreamExt;
use tokio::sync::broadcast;

/// NATS 주소. docker-compose.yml 에 적은 포트와 같아야 한다.
/// NATS_URL 환경 변수가 있으면 그 값을 대신 쓴다.
const DEFAULT_NATS_URL: &str = "nats://localhost:4222";

/// NATS 에 접속한다.
pub async fn connect() -> Result<Client, async_nats::ConnectError> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());
    async_nats::connect(url).await
}

/// 메시지 하나를 NATS 에 내보낸다. 채팅 서버가 저장을 마친 뒤 부른다. 주제는 "room.방번호" 다. 예: room.1
pub async fn publish(nats: &Client, room_id: i64, line: String) -> Result<(), async_nats::Error> {
    nats.publish(format!("room.{room_id}"), line.into()).await?;
    Ok(())
}

/// NATS 에서 오는 메시지를 받아 이 게이트웨이의 방송 채널에 넣는 일을 끝없이 한다.
/// 게이트웨이를 시작할 때 한 번 불러, 뒤에서 따로 돌게 둔다.
///
/// 누가 어느 게이트웨이에 붙어서 한 말이든 채팅 서버가 내보내고, 그것이 여기로 온다.
/// 그래서 연결들은 방송 채널만 듣고 있으면 모든 메시지를 받는다.
pub async fn relay(nats: Client, tx: broadcast::Sender<(i64, String)>) {
    // "room.*" 은 room. 뒤에 무엇이 오든 다 듣겠다는 뜻이다. 모든 방의 메시지가 온다.
    let mut incoming = match nats.subscribe("room.*").await {
        Ok(incoming) => incoming,
        Err(error) => {
            println!("[NATS] 듣기를 시작하지 못함: {error}");
            return;
        }
    };

    while let Some(message) = incoming.next().await {
        // 주제 "room.1" 에서 방 번호 1 을 꺼낸다.
        let Some(room_id) = message
            .subject
            .strip_prefix("room.")
            .and_then(|id| id.parse::<i64>().ok())
        else {
            continue;
        };
        // 내용은 바이트 덩어리로 온다. 글자로 바꾼다.
        let Ok(line) = String::from_utf8(message.payload.to_vec()) else {
            continue;
        };
        // 이 게이트웨이의 방송 채널에 넣는다. 이 게이트웨이에 붙은 연결들이 받는다.
        // 듣는 연결이 하나도 없으면 send 가 실패하는데, 그래도 괜찮으니 결과는 버린다.
        let _ = tx.send((room_id, line));
    }
    println!("[NATS] 연결이 끝나 더는 받지 못함");
}
