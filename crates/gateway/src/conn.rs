//! WebSocket 연결 하나의 처리.
//! 앱이 보낸 프레임을 처리하는 "읽기 루프"와, 대기열의 프레임을 소켓에 쓰는 "쓰기 작업"으로 나뉜다.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use common::auth::{self, Claims};
use common::keys;
use common::pb::{CheckJoinRequest, FetchMessagesRequest, SendMessageRequest};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::hub::{Conn, Frame};
use crate::AppState;

/// 앱이 보낼 수 있는 프레임 하나의 최대 크기
const MAX_FRAME_BYTES: usize = 16 * 1024;
/// 연결 하나가 동시에 들어가 있을 수 있는 방의 수
const MAX_ROOMS_PER_CONN: usize = 50;
/// 대기열이 밀려 있을 때 한 번에 모아서 쓰는 프레임 수
const WRITE_BATCH: usize = 64;
/// 접속 중 표시를 갱신하는 주기
const PRESENCE_REFRESH: Duration = Duration::from_secs(20);
/// 접속 중 표시가 유지되는 시간(초). 갱신이 끊기면 이 시간 뒤 자동으로 사라진다.
const PRESENCE_TTL_SECS: u64 = 45;
/// 앱은 25초마다 ping 을 보낸다. 세 번 연속 안 오면 죽은 연결로 보고 닫는다.
const IDLE_TIMEOUT: Duration = Duration::from_secs(75);
/// 서버 교체 시 재접속 요청을 이 시간(밀리초)에 걸쳐 나눠 보낸다.
/// 모든 앱이 같은 순간에 남은 게이트웨이로 몰려드는 것을 막기 위함.
const DRAIN_SPREAD_MS: u64 = 3000;

/// 접속 주소 뒤에 붙는 값: /ws?token=...
#[derive(Deserialize)]
pub struct WsQuery {
    token: String,
}

/// 앱이 보내는 프레임의 종류. JSON 의 "t" 필드 값으로 구분한다.
#[derive(Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum ClientFrame {
    /// 방 입장
    Join {
        room_id: i64,
    },
    /// 방 나가기
    Leave {
        room_id: i64,
    },
    /// 메시지 보내기
    Send {
        room_id: i64,
        /// 앱이 붙인 임시 번호. 재전송 시 중복을 걸러내는 데 쓴다.
        client_msg_id: String,
        body: String,
    },
    /// after_seq 이후의 메시지 요청 (빠진 메시지 채우기)
    Sync {
        room_id: i64,
        after_seq: i64,
        #[serde(default)]
        limit: Option<i32>,
    },
    /// 연결 확인
    Ping,
}

/// 접속 요청이 들어오면 가장 먼저 불린다.
pub async fn ws_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    // 서버가 종료 준비 중이면 새 접속을 받지 않는다. (로드밸런서가 다른 게이트웨이로 보낸다)
    if *state.drain.borrow() {
        return (StatusCode::SERVICE_UNAVAILABLE, "gateway is draining").into_response();
    }
    // 토큰을 검증해 누구인지 확인한다. 틀리면 접속을 거부한다.
    let claims = match auth::verify(&state.jwt_secret, &query.token) {
        Ok(claims) => claims,
        Err(_) => return (StatusCode::UNAUTHORIZED, "invalid token").into_response(),
    };
    // HTTP 연결을 WebSocket 으로 전환하고, 전환이 끝나면 run 을 실행한다.
    ws.max_message_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| run(socket, state, claims))
}

/// 연결 하나의 일생. 접속부터 끊길 때까지 여기서 돈다.
async fn run(socket: WebSocket, state: Arc<AppState>, claims: Claims) {
    // 이 연결에 고유 번호를 매기고, 현재 연결 수를 1 올린다.
    let conn_id = state.next_conn_id.fetch_add(1, Ordering::Relaxed);
    state.hub.connections.fetch_add(1, Ordering::Relaxed);

    // 소켓을 쓰기용(sink)과 읽기용(stream)으로 나눈다.
    let (sink, mut stream) = socket.split();
    // 내보낼 프레임을 쌓아 두는 대기열. 크기 제한이 있어서 느린 사용자를 가려낼 수 있다.
    let (tx, rx) = mpsc::channel::<Frame>(state.send_queue);
    // 이 연결을 강제로 닫을 때 쓰는 스위치.
    let kill = CancellationToken::new();
    // 쓰기 작업을 따로 띄운다. 읽기와 쓰기가 서로를 막지 않게 하기 위함.
    let writer = tokio::spawn(write_loop(sink, rx, kill.clone()));

    let mut session = Session {
        state: state.clone(),
        claims,
        conn_id,
        conn: Conn {
            tx,
            kill: kill.clone(),
        },
        joined: HashSet::new(),
    };
    // "이 사용자는 접속 중"이라고 Redis 에 표시한다.
    session.refresh_presence().await;
    // 앱에게 연결 완료를 알린다. 어느 게이트웨이에 붙었는지도 함께 알려준다.
    session
        .reply(json!({
            "t": "ready",
            "user_id": session.claims.sub,
            "name": session.claims.name,
            "gateway": state.gateway_id,
        }))
        .await;

    // 서버 종료 신호를 받기 위한 수신기
    let mut drain = state.drain.clone();
    // 접속 중 표시를 주기적으로 갱신하기 위한 타이머
    let mut presence = tokio::time::interval(PRESENCE_REFRESH);
    // 앱에게서 마지막으로 뭔가 받은 시각
    let mut last_seen = Instant::now();

    // 읽기 루프: 아래 네 가지 중 먼저 일어나는 일을 처리한다.
    loop {
        tokio::select! {
            // (1) 강제 종료 스위치가 눌렸다 (느린 사용자로 판정됨 등).
            _ = kill.cancelled() => break,
            // (2) 서버가 종료 준비에 들어갔다.
            _ = drain.changed() => {
                // 연결마다 조금씩 다른 시간만큼 기다린 뒤
                tokio::time::sleep(Duration::from_millis(conn_id * 7919 % DRAIN_SPREAD_MS)).await;
                // 앱에게 다시 접속하라고 알린다.
                session.reply(json!({ "t": "reconnect" })).await;
                // 쓰기 작업이 이 프레임을 실제로 내보낼 시간을 잠깐 준다.
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            // (3) 20초 타이머가 울렸다.
            _ = presence.tick() => {
                // 앱에게서 너무 오래 소식이 없으면 죽은 연결로 보고 닫는다.
                if last_seen.elapsed() > IDLE_TIMEOUT {
                    break;
                }
                // 살아 있으면 접속 중 표시를 갱신한다.
                session.refresh_presence().await;
            }
            // (4) 앱이 뭔가를 보냈다.
            frame = stream.next() => {
                // 연결이 끊겼거나 오류가 나면 끝낸다.
                let Some(Ok(message)) = frame else { break };
                last_seen = Instant::now();
                match message {
                    // 글자 프레임: 내용을 해석해서 처리한다. false 가 돌아오면 연결을 닫는다.
                    Message::Text(text) => {
                        if !session.on_text(&text).await {
                            break;
                        }
                    }
                    // 앱이 닫겠다고 알려왔다.
                    Message::Close(_) => break,
                    // 그 밖의 프레임(바이너리, 프로토콜 수준 ping/pong)은 무시한다.
                    _ => {}
                }
            }
        }
    }

    // 정리: 들어가 있던 모든 방에서 뺀다.
    for room_id in session.joined.drain() {
        state.hub.leave(room_id, conn_id);
    }
    // 쓰기 작업을 멈추고, 끝날 때까지 기다린다.
    kill.cancel();
    let _ = writer.await;
    // 현재 연결 수를 1 내린다.
    state.hub.connections.fetch_sub(1, Ordering::Relaxed);
}

/// 쓰기 작업: 대기열에서 프레임을 꺼내 소켓에 쓴다.
/// 여러 개가 밀려 있으면 한꺼번에 써넣고 한 번만 내보낸다(flush).
/// 큰 방에서 서버 부담을 줄이는 핵심이다.
async fn write_loop(
    mut sink: SplitSink<WebSocket, Message>,
    mut rx: mpsc::Receiver<Frame>,
    kill: CancellationToken,
) {
    'outer: loop {
        tokio::select! {
            // 강제 종료 스위치가 눌리면 멈춘다.
            _ = kill.cancelled() => break,
            // 대기열에 프레임이 들어왔다.
            frame = rx.recv() => {
                // 대기열이 닫혔으면(연결 정리 중) 멈춘다.
                let Some(frame) = frame else { break };
                // 첫 프레임을 소켓 버퍼에 넣는다 (아직 내보내지는 않는다).
                if sink.feed(Message::Text(frame.to_string())).await.is_err() {
                    break;
                }
                // 대기열에 더 있으면 기다리지 않고 최대 63개까지 이어서 넣는다.
                for _ in 1..WRITE_BATCH {
                    let Ok(frame) = rx.try_recv() else { break };
                    if sink.feed(Message::Text(frame.to_string())).await.is_err() {
                        break 'outer;
                    }
                }
                // 모은 것을 한 번에 네트워크로 내보낸다.
                if sink.flush().await.is_err() {
                    break;
                }
            }
        }
    }
    // 소켓을 닫는다.
    let _ = sink.close().await;
}

/// 연결 하나의 상태
struct Session {
    state: Arc<AppState>,
    /// 토큰에서 꺼낸 사용자 정보
    claims: Claims,
    /// 이 연결의 번호
    conn_id: u64,
    /// 이 연결의 대기열과 종료 스위치
    conn: Conn,
    /// 이 연결이 들어가 있는 방들
    joined: HashSet<i64>,
}

impl Session {
    /// 이 앱에게 응답을 보낸다 (대기열에 넣는다).
    /// 대기열이 꽉 차 있으면 자리가 날 때까지 기다린다. 그 동안 이 앱의 다음 요청을
    /// 읽지 않게 되므로, 응답을 읽지도 않고 요청만 쏟아내는 앱은 자연히 느려진다.
    async fn reply(&self, value: serde_json::Value) -> bool {
        self.reply_raw(value.to_string()).await
    }

    /// 이미 문자열로 만들어진 프레임을 보낸다. 연결이 닫혔으면 false.
    async fn reply_raw(&self, frame: String) -> bool {
        self.conn.tx.send(Arc::from(frame)).await.is_ok()
    }

    /// 오류 프레임을 보낸다. 어떤 메시지에 대한 오류인지 알 수 있게 임시 번호도 함께 넣는다.
    async fn error(&self, code: &str, message: &str, client_msg_id: Option<&str>) -> bool {
        self.reply(json!({
            "t": "error",
            "code": code,
            "message": message,
            "client_msg_id": client_msg_id,
        }))
        .await
    }

    /// 채팅 서버가 돌려준 gRPC 오류를 앱이 이해하는 오류 코드로 바꿔 보낸다.
    async fn grpc_error(&self, status: tonic::Status, client_msg_id: Option<&str>) -> bool {
        let code = match status.code() {
            tonic::Code::PermissionDenied => "FORBIDDEN",
            tonic::Code::NotFound => "NOT_FOUND",
            tonic::Code::ResourceExhausted => "RATE_LIMITED",
            tonic::Code::InvalidArgument => "INVALID",
            tonic::Code::Aborted => "IN_PROGRESS",
            _ => "UNAVAILABLE",
        };
        self.error(code, status.message(), client_msg_id).await
    }

    /// 접속 중 표시를 갱신한다.
    /// 짧은 만료 시간이 붙은 Redis 키라서, 연결이 살아 있는 동안만 계속 갱신된다.
    /// 연결이나 게이트웨이 전체가 죽으면 갱신이 멈추고 표시가 저절로 사라진다.
    async fn refresh_presence(&self) {
        let mut redis = self.state.redis.clone();
        let result: Result<(), _> = redis
            .set_ex(
                keys::presence(self.claims.sub),
                &self.state.gateway_id,
                PRESENCE_TTL_SECS,
            )
            .await;
        // Redis 가 잠깐 안 되어도 채팅은 계속되어야 하므로 경고만 남긴다.
        if let Err(err) = result {
            tracing::warn!("presence refresh failed: {err}");
        }
    }

    /// 앱이 보낸 글자 프레임 하나를 처리한다. 연결을 닫아야 하면 false 를 돌려준다.
    async fn on_text(&mut self, text: &str) -> bool {
        // JSON 을 해석한다. 모르는 형식이면 오류를 알리고 연결은 유지한다.
        let frame: ClientFrame = match serde_json::from_str(text) {
            Ok(frame) => frame,
            Err(_) => return self.error("INVALID", "unrecognized frame", None).await,
        };
        // 종류에 따라 나눠 처리한다.
        match frame {
            // 연결 확인: 바로 pong 으로 답한다.
            ClientFrame::Ping => self.reply(json!({ "t": "pong" })).await,
            ClientFrame::Join { room_id } => self.on_join(room_id).await,
            ClientFrame::Leave { room_id } => {
                // 들어가 있던 방이면 뺀다.
                if self.joined.remove(&room_id) {
                    self.state.hub.leave(room_id, self.conn_id);
                }
                self.reply(json!({ "t": "left", "room_id": room_id })).await
            }
            ClientFrame::Send {
                room_id,
                client_msg_id,
                body,
            } => self.on_send(room_id, client_msg_id, body).await,
            ClientFrame::Sync {
                room_id,
                after_seq,
                limit,
            } => self.on_sync(room_id, after_seq, limit.unwrap_or(100)).await,
        }
    }

    /// 방 입장 처리
    async fn on_join(&mut self, room_id: i64) -> bool {
        // 연결 하나가 너무 많은 방에 들어가지 못하게 막는다.
        if !self.joined.contains(&room_id) && self.joined.len() >= MAX_ROOMS_PER_CONN {
            return self.error("INVALID", "too many rooms on one connection", None).await;
        }
        // 채팅 서버에 "이 사람이 이 방에 들어가도 되는지" 묻는다.
        let mut chat = self.state.chat.clone();
        let response = chat
            .check_join(CheckJoinRequest {
                user_id: self.claims.sub,
                room_id,
            })
            .await;
        let info = match response {
            Ok(response) => response.into_inner(),
            // 권한 없음, 방 없음 등: 오류를 앱에 전달한다.
            Err(status) => return self.grpc_error(status, None).await,
        };

        // 처음 들어가는 방이면 방 목록에 등록한다 (필요하면 NATS 구독도 여기서 시작된다).
        if self.joined.insert(room_id) {
            let live = info.room_type == "live";
            let joined = self
                .state
                .hub
                .join(room_id, live, self.conn_id, self.conn.clone())
                .await;
            // 구독에 실패하면 입장을 취소하고 오류를 알린다.
            if let Err(err) = joined {
                tracing::error!(room_id, "room subscription failed: {err:#}");
                self.joined.remove(&room_id);
                return self.error("UNAVAILABLE", "could not join room", None).await;
            }
        }
        // 입장 완료를 알린다.
        // latest_seq 는 구독하기 전에 읽은 값이다. 그 사이에 발행된 메시지는
        // 앱이 이 응답을 받은 직후 보내는 sync 로 채워진다.
        self.reply(json!({
            "t": "joined",
            "room_id": room_id,
            "room_type": info.room_type,
            "latest_seq": info.latest_seq,
        }))
        .await
    }

    /// 메시지 전송 처리
    async fn on_send(&mut self, room_id: i64, client_msg_id: String, body: String) -> bool {
        // 입장하지 않은 방에는 보낼 수 없다.
        if !self.joined.contains(&room_id) {
            return self
                .error("NOT_JOINED", "join the room before sending", Some(&client_msg_id))
                .await;
        }
        // 채팅 서버에 넘긴다. 검사, 번호 붙이기, 발행은 전부 채팅 서버가 한다.
        // 응답을 기다린 뒤에야 이 앱의 다음 프레임을 읽으므로, 한 사람이 보낸 메시지끼리는 순서가 유지된다.
        let mut chat = self.state.chat.clone();
        let response = chat
            .send_message(SendMessageRequest {
                user_id: self.claims.sub,
                user_name: self.claims.name.clone(),
                room_id,
                client_msg_id: client_msg_id.clone(),
                body,
            })
            .await;
        match response {
            // 성공: 전송 완료(ack)를 알린다. 고유 번호는 JavaScript 정수 한계 때문에 문자열로 보낸다.
            Ok(response) => {
                let sent = response.into_inner();
                self.reply(json!({
                    "t": "ack",
                    "room_id": room_id,
                    "client_msg_id": client_msg_id,
                    "message_id": sent.message_id.to_string(),
                    "seq": sent.seq,
                    "duplicate": sent.duplicate,
                }))
                .await
            }
            // 실패: 어떤 메시지가 실패했는지 알 수 있게 임시 번호와 함께 오류를 보낸다.
            Err(status) => self.grpc_error(status, Some(&client_msg_id)).await,
        }
    }

    /// 지난 메시지 요청 처리
    async fn on_sync(&mut self, room_id: i64, after_seq: i64, limit: i32) -> bool {
        // 채팅 서버에서 after_seq 이후의 메시지를 가져온다. (권한 검사도 거기서 한다)
        let mut chat = self.state.chat.clone();
        let response = chat
            .fetch_messages(FetchMessagesRequest {
                user_id: self.claims.sub,
                room_id,
                after_seq,
                limit,
            })
            .await;
        match response {
            Ok(response) => {
                let page = response.into_inner();
                // 메시지들은 이미 JSON 문자열이다. 다시 변환하지 않고 쉼표로 이어 붙여 끼워 넣는다.
                let frame = format!(
                    r#"{{"t":"sync","room_id":{},"latest_seq":{},"has_more":{},"messages":[{}]}}"#,
                    room_id,
                    page.latest_seq,
                    page.has_more,
                    page.messages.join(","),
                );
                self.reply_raw(frame).await
            }
            Err(status) => self.grpc_error(status, None).await,
        }
    }
}
