//! 1단계: 서버 한 대.
//!
//! 지금 하는 일은 두 가지다.
//! 1. 브라우저가 접속하면 화면 한 장을 보여 준다.
//! 2. 화면이 WebSocket 으로 연결해 오면, 받은 말을 보낸 사람에게 그대로 되돌려 준다.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::{Html, Response};
use axum::routing::get;
use axum::Router;

#[tokio::main]
async fn main() {
    // 주소와 처리 함수를 연결한다.
    let app = Router::new()
        // "/" 로 오면 화면을 보여 준다. (화면 파일은 빌드할 때 실행 파일 안에 넣어 둔다)
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }))
        // "/ws" 로 오면 WebSocket 연결을 맺는다.
        .route("/ws", get(ws_handler));

    // 8080 포트를 열고 접속을 받기 시작한다.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("8080 포트를 열 수 없습니다");
    println!("http://localhost:8080 에서 접속을 기다리는 중");
    axum::serve(listener, app).await.expect("서버 실행 실패");
}

/// 접속 요청이 오면 불린다. 일반 HTTP 연결을 WebSocket 으로 바꾼다.
async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    // 전환이 끝나면 chat 함수가 이 연결을 맡는다.
    ws.on_upgrade(chat)
}

/// 연결 하나를 처음부터 끝까지 담당한다.
async fn chat(mut socket: WebSocket) {
    // 이 사람이 뭔가를 보낼 때까지 기다리기를 반복한다.
    // 연결이 끊기면 recv 가 None 을 돌려주고 반복이 끝난다.
    while let Some(incoming) = socket.recv().await {
        match incoming {
            Ok(Message::Text(body)) => {
                // 빈 메시지는 무시한다.
                let body = body.trim();
                if body.is_empty() {
                    continue;
                }
                // 받은 말을 보낸 사람에게 그대로 되돌려 준다.
                // 전달에 실패하면 연결이 끊긴 것이다.
                if socket.send(Message::Text(body.to_string())).await.is_err() {
                    break;
                }
            }
            // 닫겠다고 알려왔거나 오류가 났다: 끝낸다.
            Ok(Message::Close(_)) | Err(_) => break,
            // 그 밖의 프레임은 무시한다.
            Ok(_) => {}
        }
    }
}
