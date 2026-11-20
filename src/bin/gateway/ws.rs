//! WebSocket 연결 하나를 맺고, 끝날 때까지 담당한다.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast;
use tonic::Code;

use chat_server::proto::{ChatMessage, JoinRequest, SayRequest};
use chat_server::to_line;

// crate 는 "이 프로그램의 맨 위"라는 뜻이다. state.rs 에 있는 AppState 를 가져온다.
use crate::state::AppState;

/// 로그에서 연결을 구분하기 위한 번호표. 접속할 때마다 1씩 올라간다.
/// 여러 연결이 동시에 건드려도 번호가 겹치지 않는 숫자 타입(Atomic)을 쓴다.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// 접속 주소 뒤에 붙는 값: /ws?token=3f2b8c1e-...&room=2
/// Deserialize 를 붙이면 "token=...&room=2" 같은 글자를 이 구조로 바꿔 넣어 준다.
/// pub 이 붙은 ws_handler 가 인자로 받으므로 이것도 pub 이어야 한다.
///
/// 이름을 직접 받지 않는다. 이름을 받으면 남의 이름을 적어 보낼 수 있기 때문이다.
/// 로그인해서 받은 입장권(token)만 받고, 이름은 채팅 서버가 DB 에서 찾는다.
#[derive(Deserialize)]
pub struct WsQuery {
    token: String,
    /// 들어갈 방의 번호. 연결 하나는 방 하나에만 들어간다. 방을 옮기려면 다시 접속한다.
    room: i64,
}

/// 접속 요청이 오면 불린다. 일반 HTTP 연결을 WebSocket 으로 바꾼다.
/// main.rs 가 주소에 연결해야 하므로 pub 이다. 이 파일에서 밖으로 내놓는 함수는 이것 하나다.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    // 주소 뒤의 ?token=...&room=... 을 꺼낸다. 둘 중 하나라도 없으면 axum 이 400 오류로 거절한다.
    Query(query): Query<WsQuery>,
    // main.rs 에서 맡겨 둔 묶음을 꺼낸다.
    State(state): State<Arc<AppState>>,
) -> Response {
    let room_id = query.room;

    // 방송 채널 구독을 먼저 시작한다. 이 뒤로 온 메시지는 rx 에 쌓인다.
    // 채팅 서버가 최근 대화를 읽는 동안 새로 온 메시지를 놓치지 않기 위해서다.
    // 대신 그 짧은 틈에 온 메시지는 최근 대화로 한 번, 방송으로 한 번, 두 번 보내게 된다.
    // 메시지마다 순번이 붙어 있으므로, 화면이 같은 순번을 두 번 받으면 하나를 버린다.
    let rx = state.tx.subscribe();

    // 채팅 서버에 입장을 부탁한다. 입장권과 방을 확인하고 최근 대화까지 한 번에 받아 온다.
    // 함수를 부르는 것처럼 보이지만, 실제로는 요청이 네트워크를 건너 채팅 서버에 갔다가 답이 돌아온다. (gRPC)
    // WebSocket 으로 바꾸기 전에 확인하므로, 입장권이 없는 사람은 연결조차 맺지 못한다.
    let request = JoinRequest { token: query.token, room_id };
    let reply = match state.chat.clone().join(request).await {
        // 답은 포장(Response)에 싸여 온다. 포장을 벗겨 내용만 꺼낸다.
        Ok(reply) => reply.into_inner(),
        // 채팅 서버가 거절했거나 닿지 못했다. gRPC 의 실패 종류를 HTTP 응답 번호로 옮겨 화면에 알린다.
        Err(status) => {
            let (code, message) = match status.code() {
                // 401 은 "누구인지 확인되지 않았다", 404 는 "그런 것은 없다"는 뜻의 HTTP 응답 번호다.
                Code::Unauthenticated => (StatusCode::UNAUTHORIZED, "로그인이 필요합니다"),
                Code::NotFound => (StatusCode::NOT_FOUND, "그런 방이 없습니다"),
                // 채팅 서버가 꺼져 있다. 503 은 "지금은 해 줄 수 없다"는 뜻이다.
                Code::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "채팅 서버에 닿지 못했습니다"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "서버 오류"),
            };
            println!("[접속 거절] 방 번호 {room_id}: {message} ({status})");
            return (code, message).into_response();
        }
    };
    println!("[입장] {} → {:?} 방 (방 번호 {room_id})", reply.name, reply.room_name);

    // 전환이 끝나면 chat 함수가 이 연결을 맡는다.
    ws.on_upgrade(move |socket| chat(socket, state, rx, reply.name, room_id, reply.recent))
}

/// 연결 하나를 처음부터 끝까지 담당한다.
async fn chat(
    mut socket: WebSocket,
    state: Arc<AppState>,
    // 입장하기 전에 시작해 둔 방송 채널 구독. 누가 보낸 메시지든 여기로 들어온다.
    mut rx: broadcast::Receiver<(i64, String)>,
    name: String,
    room_id: i64,
    // 채팅 서버가 입장할 때 함께 준 최근 대화.
    recent: Vec<ChatMessage>,
) {
    // 이 연결의 번호표를 뽑는다. 로그에서 누구의 일인지 구분하는 데만 쓴다.
    let id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);

    // 맨 먼저 이 게이트웨이의 이름표를 알려 준다. 예: {"server":"8081"}
    // 로드밸런서 뒤에 게이트웨이가 여러 대라서, 화면은 자기가 어느 쪽에 붙었는지 알 길이 없다.
    // 대화 메시지가 아니라 안내다. 화면은 server 라는 칸이 있으면 안내로 알아본다.
    let hello = json!({ "server": state.server }).to_string();
    if socket.send(Message::Text(hello)).await.is_err() {
        return;
    }
    println!(
        "[연결 {id} {name}] 접속 (게이트웨이 전체 {}명), 지난 대화 {}개를 보냄",
        state.tx.receiver_count(),
        recent.len()
    );

    // 지난 대화를 먼저 보내 준다. 보내다 실패하면 연결이 끊긴 것이므로 여기서 끝낸다.
    for message in recent {
        let line = to_line(message.seq, &message.name, &message.body);
        if socket.send(Message::Text(line)).await.is_err() {
            drop(rx);
            println!("[연결 {id} {name}] 종료 (게이트웨이 전체 {}명)", state.tx.receiver_count());
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
                        // 채팅 서버에 말하기를 부탁한다. 순번을 매기고, 남기고, NATS 에 내보내는 일은 채팅 서버가 한다.
                        // 여기서 방송 채널에 직접 넣지 않는다. 채팅 서버가 내보낸 것이 NATS 를 거쳐 방송 채널로 돌아온다.
                        let request = SayRequest { room_id, name: name.clone(), body: body.to_string() };
                        match state.chat.clone().say(request).await {
                            Ok(reply) => println!("[연결 {id} {name}] 받음: {body:?} → 채팅 서버가 순번 {} 을 매김", reply.into_inner().seq),
                            // 채팅 서버가 꺼져 있거나 메시지를 남기지 못했다. 이 메시지는 어디에도 남지 않는다.
                            // 연결은 끊지 않는다. 채팅 서버가 돌아오면 다음 메시지부터 다시 된다.
                            Err(status) => println!("[연결 {id} {name}] 보내지 못함: {body:?} ({status})"),
                        }
                    }
                    // 연결이 끊겼거나, 닫겠다고 알려왔거나, 오류가 났다: 끝낸다.
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    // 그 밖의 프레임은 무시한다.
                    Some(Ok(_)) => {}
                }
            }
            // (2) 누군가(본인 포함, 다른 게이트웨이 사람 포함) 보낸 메시지가 방송 채널로 왔다.
            outgoing = rx.recv() => {
                match outgoing {
                    // 이 사람에게 전달한다. 전달에 실패하면 연결이 끊긴 것이다.
                    Ok((to_room, line)) => {
                        // 방송 채널에는 모든 방의 메시지가 지나간다. 다른 방 것은 버린다.
                        if to_room != room_id {
                            continue;
                        }
                        println!("[연결 {id} {name}] 전달: {line}");
                        if socket.send(Message::Text(line)).await.is_err() {
                            break;
                        }
                    }
                    // 너무 느려서 일부를 놓쳤다. 지금은 그냥 넘어간다.
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        println!("[연결 {id} {name}] 느려서 {missed}개를 놓침");
                    }
                    // 방송 채널이 닫혔다 (게이트웨이 종료 중).
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    // 반복을 빠져나왔다 = 이 연결이 끝났다. 구독(rx)을 먼저 정리하고 남은 인원을 센다.
    drop(rx);
    println!("[연결 {id} {name}] 종료 (게이트웨이 전체 {}명)", state.tx.receiver_count());
}
