//! 게이트웨이: 연결 담당. 브라우저의 WebSocket 연결을 붙들고, 메시지를 실어 나르기만 한다.
//!
//! 5단계에서 서버 한 대가 하던 일을 둘로 나눴다. 이쪽은 "붙들고 전하는 일"이다.
//! 누가 누구인지, 그런 방이 있는지, 메시지를 어떻게 저장하는지는 모른다. 채팅 서버에 물어본다.
//! 그래서 DB 에 접속하지 않는다.
//!
//! 하는 일은 네 가지다.
//! 1. 브라우저가 접속하면 화면 한 장을 보여 준다.
//! 2. 화면이 입장권과 방 번호를 들고 WebSocket 으로 연결해 오면, 채팅 서버에 입장(Join)을 부탁한다.
//!    돌아온 최근 대화를 화면에 먼저 보내 준다.
//! 3. 브라우저가 말을 보내면 채팅 서버에 말하기(Say)를 부탁한다. 저장과 알리기는 채팅 서버가 한다.
//! 4. NATS 로 온 메시지를 같은 방에 있는 연결들에게 뿌린다.
//!
//! 이 파일은 조립만 한다. 실제 일은 아래 파일에 있다.
//! - state.rs: 모든 연결이 함께 쓰는 것들 (방송 채널, 채팅 서버로 가는 연결)
//! - ws.rs: WebSocket 연결 하나를 맺고 끝까지 담당하는 코드
//! 두 프로그램이 함께 쓰는 것(NATS, gRPC 약속)은 src/lib.rs 에 있다.

// mod 는 "같은 폴더의 state.rs, ws.rs 를 이 프로그램에 포함한다"는 선언이다.
mod state;
mod ws;

use std::sync::Arc;

use axum::response::Html;
use axum::routing::get;
use axum::Router;
use tonic::transport::Endpoint;

// chat_server 는 이 프로젝트의 src/lib.rs 다. 채팅 서버와 함께 쓰는 것들이 들어 있다.
use chat_server::bus;
use chat_server::proto::chat_client::ChatClient;

use state::AppState;

/// 채팅 서버의 gRPC 주소. CHAT_URL 환경 변수가 있으면 그 값을 대신 쓴다.
const DEFAULT_CHAT_URL: &str = "http://localhost:50051";

#[tokio::main]
async fn main() {
    // 이 게이트웨이가 쓸 포트. PORT 환경 변수가 있으면 그 값을, 없으면 8081 을 쓴다.
    // 게이트웨이를 여러 대 띄우려면 서로 다른 포트를 준다. 예: PORT=8082 cargo run --bin gateway
    // 8080 은 로드밸런서가 쓴다. 브라우저는 8080 으로 들어오고, 로드밸런서가 이쪽으로 넘겨준다.
    let port = std::env::var("PORT").unwrap_or_else(|_| "8081".to_string());

    // 채팅 서버로 가는 gRPC 연결을 준비한다.
    // connect_lazy 는 "지금 맺지 말고, 처음 부를 때 맺어라"는 뜻이다. 끊기면 다음에 부를 때 다시 맺는다.
    // 그래서 채팅 서버가 꺼져 있어도 게이트웨이는 시작하고, 채팅 서버가 다시 켜지면 알아서 다시 붙는다.
    let chat_url = std::env::var("CHAT_URL").unwrap_or_else(|_| DEFAULT_CHAT_URL.to_string());
    let channel = Endpoint::from_shared(chat_url.clone())
        .unwrap_or_else(|error| panic!("채팅 서버 주소 {chat_url} 이(가) 올바르지 않습니다: {error}"))
        .connect_lazy();
    let chat = ChatClient::new(channel);

    // NATS 에 접속한다. 채팅 서버가 내보낸 메시지를 받는 길이다. 꺼져 있으면 시작하지 않는다.
    let nats = bus::connect()
        .await
        .expect("NATS 에 접속할 수 없습니다. docker compose up -d 로 NATS 를 먼저 켜세요");

    // 방송 채널과 채팅 서버로 가는 연결의 묶음을 만든다.
    // Arc 는 값 하나를 여러 곳에서 함께 가리키게 해 주는 포장지다.
    let state = Arc::new(AppState::new(chat, port.clone()));

    // NATS 에서 오는 메시지를 이 게이트웨이의 방송 채널로 옮기는 일을 뒤에서 따로 돌린다.
    tokio::spawn(bus::relay(nats, state.tx.clone()));

    // 주소와 처리 함수를 연결한다. 로그인과 방은 여기 없다. 로드밸런서가 그 요청은 채팅 서버로 보낸다.
    let app = Router::new()
        // "/" 로 오면 화면을 보여 준다. (화면 파일은 빌드할 때 실행 파일 안에 넣어 둔다)
        .route("/", get(|| async { Html(include_str!("../../../static/index.html")) }))
        // "/ws" 로 오면 WebSocket 연결을 맺는다.
        .route("/ws", get(ws::ws_handler))
        // 모든 처리 함수가 같은 묶음을 꺼내 쓸 수 있게 맡겨 둔다.
        .with_state(state);

    // 포트를 열고 접속을 받기 시작한다.
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap_or_else(|error| panic!("{port} 포트를 열 수 없습니다: {error}"));
    println!("게이트웨이: {port} 포트에서 접속을 기다리는 중 (브라우저는 로드밸런서 http://localhost:8080 으로)");
    axum::serve(listener, app).await.expect("게이트웨이 실행 실패");
}
