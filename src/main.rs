//! 1단계: 서버 한 대, 방 하나, 메모리에만 저장.
//!
//! 하는 일은 세 가지뿐이다.
//! 1. 브라우저가 접속하면 WebSocket 연결을 맺는다.
//! 2. 접속한 사람에게 지금까지의 대화를 보내 준다.
//! 3. 누가 메시지를 보내면 접속한 모든 사람에게 뿌린다.

use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::{Html, Response};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast;

/// 모든 연결이 함께 쓰는 상태. 서버의 메모리에만 있으므로 서버를 끄면 사라진다.
struct AppState {
    /// 지금까지의 대화. 새로 들어온 사람에게 보여 주기 위해 모아 둔다.
    history: Mutex<Vec<String>>,
    /// 방송 채널. 여기에 메시지를 넣으면 구독 중인 모든 연결이 받는다.
    tx: broadcast::Sender<String>,
}

/// 접속 주소 뒤에 붙는 값: /ws?name=철수
#[derive(Deserialize)]
struct WsQuery {
    name: String,
}

#[tokio::main]
async fn main() {
    // 방송 채널을 만든다. 256 은 느린 연결을 위해 쌓아 둘 수 있는 메시지 수.
    let (tx, _) = broadcast::channel(256);
    let state = Arc::new(AppState {
        history: Mutex::new(Vec::new()),
        tx,
    });

    // 주소와 처리 함수를 연결한다.
    let app = Router::new()
        // "/" 로 오면 테스트 화면을 보여 준다. (화면 파일은 빌드할 때 실행 파일 안에 넣어 둔다)
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }))
        // "/ws" 로 오면 WebSocket 연결을 맺는다.
        .route("/ws", get(ws_handler))
        .with_state(state);

    // 8080 포트를 열고 접속을 받기 시작한다.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("8080 포트를 열 수 없습니다");
    println!("http://localhost:8080 에서 접속을 기다리는 중");
    axum::serve(listener, app).await.expect("서버 실행 실패");
}

/// 접속 요청이 오면 불린다. 일반 HTTP 연결을 WebSocket 으로 바꾼다.
async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    // 전환이 끝나면 chat 함수가 이 연결을 맡는다.
    ws.on_upgrade(move |socket| chat(socket, state, query.name))
}

/// 연결 하나를 처음부터 끝까지 담당한다.
async fn chat(socket: WebSocket, state: Arc<AppState>, name: String) {
    // 소켓을 쓰기용(sink)과 읽기용(stream)으로 나눈다.
    let (mut sink, mut stream) = socket.split();

    // 지금까지의 대화를 복사하고, 방송 채널 구독을 시작한다.
    // 둘을 한 번의 잠금 안에서 해야 그 사이에 온 메시지를 놓치거나 두 번 받지 않는다.
    let (past, mut rx) = {
        let history = state.history.lock().unwrap();
        (history.clone(), state.tx.subscribe())
    };

    // 지난 대화를 먼저 보내 준다.
    for line in past {
        if sink.send(Message::Text(line)).await.is_err() {
            return;
        }
    }

    // 두 가지 일을 동시에 기다린다: 이 사람이 보낸 메시지, 다른 사람이 보낸 메시지.
    loop {
        tokio::select! {
            // (1) 이 사람이 뭔가를 보냈다.
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(body))) => {
                        // 빈 메시지는 무시한다.
                        let body = body.trim();
                        if body.is_empty() {
                            continue;
                        }
                        // 누가 보냈는지 붙여서 JSON 한 줄로 만든다.
                        let line = json!({ "name": name, "body": body }).to_string();
                        // 대화 기록에 추가하고 모두에게 방송한다.
                        // 잠금 안에서 함께 해야 기록 순서와 방송 순서가 같아진다.
                        let mut history = state.history.lock().unwrap();
                        history.push(line.clone());
                        let _ = state.tx.send(line);
                    }
                    // 연결이 끊겼거나, 닫겠다고 알려왔거나, 오류가 났다: 끝낸다.
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    // 그 밖의 프레임은 무시한다.
                    Some(Ok(_)) => {}
                }
            }
            // (2) 누군가(본인 포함) 보낸 메시지가 방송 채널로 왔다.
            outgoing = rx.recv() => {
                match outgoing {
                    // 이 사람에게 전달한다. 전달에 실패하면 연결이 끊긴 것이다.
                    Ok(line) => {
                        if sink.send(Message::Text(line)).await.is_err() {
                            break;
                        }
                    }
                    // 너무 느려서 일부를 놓쳤다. 1단계에서는 그냥 넘어간다.
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    // 방송 채널이 닫혔다 (서버 종료 중).
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}
