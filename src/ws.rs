//! WebSocket 연결 하나를 맺고, 끝날 때까지 담당한다.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast;

// crate 는 "이 프로젝트의 맨 위"라는 뜻이다. state.rs 에 있는 AppState 를 가져온다.
use crate::db;
use crate::state::AppState;

/// 로그에서 연결을 구분하기 위한 번호표. 접속할 때마다 1씩 올라간다.
/// 여러 연결이 동시에 건드려도 번호가 겹치지 않는 숫자 타입(Atomic)을 쓴다.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// 접속 주소 뒤에 붙는 값: /ws?name=철수
/// Deserialize 를 붙이면 "name=철수" 같은 글자를 이 구조로 바꿔 넣어 준다.
/// pub 이 붙은 ws_handler 가 인자로 받으므로 이것도 pub 이어야 한다.
#[derive(Deserialize)]
pub struct WsQuery {
    name: String,
}

/// 접속 요청이 오면 불린다. 일반 HTTP 연결을 WebSocket 으로 바꾼다.
/// main.rs 가 주소에 연결해야 하므로 pub 이다. 이 파일에서 밖으로 내놓는 함수는 이것 하나다.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    // 주소 뒤의 ?name=... 을 꺼낸다. name 이 없으면 axum 이 400 오류로 거절한다.
    Query(query): Query<WsQuery>,
    // main.rs 에서 맡겨 둔 묶음을 꺼낸다.
    State(state): State<Arc<AppState>>,
) -> Response {
    // 전환이 끝나면 chat 함수가 이 연결을 맡는다.
    ws.on_upgrade(move |socket| chat(socket, state, query.name))
}

/// 연결 하나를 처음부터 끝까지 담당한다.
async fn chat(mut socket: WebSocket, state: Arc<AppState>, name: String) {
    // 이 연결의 번호표를 뽑는다. 로그에서 누구의 일인지 구분하는 데만 쓴다.
    let id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);

    // 방송 채널 구독을 먼저 시작한다. 이 뒤로 누가 보낸 메시지든 rx 로 들어온다.
    let mut rx = state.tx.subscribe();

    // 지금까지의 대화를 DB 에서 읽어 온다.
    // 구독을 먼저 했으므로, 읽는 동안 새로 온 메시지도 놓치지 않는다.
    // 대신 그 짧은 틈에 온 메시지는 DB 에서 한 번, 방송으로 한 번, 두 번 받을 수 있다.
    // (지금은 그대로 둔다. 메시지에 순번을 붙이면 가려낼 수 있다.)
    let past: Vec<String> = match db::load_messages(&state.db).await {
        Ok(rows) => rows
            .into_iter()
            // 화면이 받는 모양인 JSON 한 줄로 바꾼다. 예: {"body":"안녕","name":"철수"}
            .map(|(name, body)| json!({ "name": name, "body": body }).to_string())
            .collect(),
        Err(error) => {
            // DB 에서 읽지 못했다. 대화를 보여 줄 수 없으니 이 연결을 끝낸다.
            drop(rx);
            println!("[연결 {id} {name}] 지난 대화를 읽지 못해 종료: {error}");
            return;
        }
    };
    println!(
        "[연결 {id} {name}] 접속 (지금 {}명), 지난 대화 {}개를 보냄",
        state.tx.receiver_count(),
        past.len()
    );

    // 지난 대화를 먼저 보내 준다. 보내다 실패하면 연결이 끊긴 것이므로 여기서 끝낸다.
    for line in past {
        if socket.send(Message::Text(line)).await.is_err() {
            drop(rx);
            println!("[연결 {id} {name}] 종료 (지금 {}명)", state.tx.receiver_count());
            return;
        }
    }

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
                        // 먼저 DB 에 저장한다. 저장이 끝날 때까지 여기서 기다린다.
                        // 저장에 실패한 메시지는 뿌리지 않는다.
                        // 뿌리고 나서 저장에 실패하면, 지금 본 사람과 나중에 온 사람이 다른 대화를 보게 된다.
                        if let Err(error) = db::save_message(&state.db, &name, body).await {
                            println!("[연결 {id} {name}] 저장 실패, 뿌리지 않음: {error}");
                            continue;
                        }
                        // 누가 보냈는지 붙여서 JSON 한 줄로 만든다.
                        // 예: {"body":"안녕","name":"철수"}
                        let line = json!({ "name": name, "body": body }).to_string();
                        // 방송 채널에 넣는다. 구독 중인 모든 연결(본인 포함)이 받는다.
                        // send 는 그 순간 구독 중인 연결 수를 돌려준다.
                        let listeners = state.tx.send(line).unwrap_or(0);
                        println!("[연결 {id} {name}] 받음: {body:?} → {listeners}명에게 방송");
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
                        println!("[연결 {id} {name}] 전달: {line}");
                        if socket.send(Message::Text(line)).await.is_err() {
                            break;
                        }
                    }
                    // 너무 느려서 일부를 놓쳤다. 1단계에서는 그냥 넘어간다.
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        println!("[연결 {id} {name}] 느려서 {missed}개를 놓침");
                    }
                    // 방송 채널이 닫혔다 (서버 종료 중).
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    // 반복을 빠져나왔다 = 이 연결이 끝났다. 구독(rx)을 먼저 정리하고 남은 인원을 센다.
    drop(rx);
    println!("[연결 {id} {name}] 종료 (지금 {}명)", state.tx.receiver_count());
}
