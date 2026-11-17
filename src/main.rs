//! 1단계: 서버 한 대.
//!
//! 지금 하는 일은 두 가지다.
//! 1. 브라우저가 접속하면 화면 한 장을 보여 준다.
//! 2. 화면이 WebSocket 으로 연결해 오면, 누가 보낸 말이든 접속한 모든 사람에게 뿌린다.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{Html, Response};
use axum::routing::get;
use axum::Router;
use tokio::sync::broadcast;

/// 로그에서 연결을 구분하기 위한 번호표. 접속할 때마다 1씩 올라간다.
/// 여러 연결이 동시에 건드려도 번호가 겹치지 않는 숫자 타입(Atomic)을 쓴다.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

#[tokio::main]
async fn main() {
    // 방송 채널을 만든다. 여기에 메시지를 넣으면 구독 중인 모든 연결이 받는다.
    // 256 은 느린 연결을 위해 쌓아 둘 수 있는 메시지 수.
    // tx 는 보내는 쪽 손잡이다. 받는 쪽은 연결마다 따로 만들 것이라 여기서는 버린다.
    let (tx, _) = broadcast::channel::<String>(256);

    // 주소와 처리 함수를 연결한다.
    let app = Router::new()
        // "/" 로 오면 화면을 보여 준다. (화면 파일은 빌드할 때 실행 파일 안에 넣어 둔다)
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }))
        // "/ws" 로 오면 WebSocket 연결을 맺는다.
        .route("/ws", get(ws_handler))
        // 모든 처리 함수가 같은 방송 채널을 꺼내 쓸 수 있게 맡겨 둔다.
        .with_state(tx);

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
    // 위에서 맡겨 둔 방송 채널을 꺼낸다.
    State(tx): State<broadcast::Sender<String>>,
) -> Response {
    // 전환이 끝나면 chat 함수가 이 연결을 맡는다.
    ws.on_upgrade(move |socket| chat(socket, tx))
}

/// 연결 하나를 처음부터 끝까지 담당한다.
async fn chat(mut socket: WebSocket, tx: broadcast::Sender<String>) {
    // 이 연결의 번호표를 뽑는다. 로그에서 누구의 일인지 구분하는 데만 쓴다.
    let id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);

    // 방송 채널 구독을 시작한다. 이 뒤로 누가 보낸 메시지든 rx 로 들어온다.
    let mut rx = tx.subscribe();
    println!("[연결 {id}] 접속 (지금 {}명)", tx.receiver_count());

    // 두 가지 일을 동시에 기다린다: 이 사람이 보낸 메시지, 방송 채널로 온 메시지.
    loop {
        tokio::select! {
            // (1) 이 사람이 뭔가를 보냈다.
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(body))) => {
                        // 빈 메시지는 무시한다.
                        let body = body.trim();
                        if body.is_empty() {
                            continue;
                        }
                        // 방송 채널에 넣는다. 구독 중인 모든 연결(본인 포함)이 받는다.
                        // send 는 그 순간 구독 중인 연결 수를 돌려준다.
                        let listeners = tx.send(body.to_string()).unwrap_or(0);
                        println!("[연결 {id}] 받음: {body:?} → {listeners}명에게 방송");
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
                        println!("[연결 {id}] 전달: {line:?}");
                        if socket.send(Message::Text(line)).await.is_err() {
                            break;
                        }
                    }
                    // 너무 느려서 일부를 놓쳤다. 1단계에서는 그냥 넘어간다.
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        println!("[연결 {id}] 느려서 {missed}개를 놓침");
                    }
                    // 방송 채널이 닫혔다 (서버 종료 중).
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    // 반복을 빠져나왔다 = 이 연결이 끝났다. 구독(rx)을 먼저 정리하고 남은 인원을 센다.
    drop(rx);
    println!("[연결 {id}] 종료 (지금 {}명)", tx.receiver_count());
}
