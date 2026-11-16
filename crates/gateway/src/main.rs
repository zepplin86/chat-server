//! 게이트웨이: 연결 계층.
//!
//! WebSocket 연결을 붙잡고, 로그인 확인을 하고, 각 연결이 어느 방에 있는지 기억하고,
//! 프레임을 실어 나른다. 채팅 로직은 전부 채팅 서버에 있고 gRPC 로 부른다.

mod conn;
mod hub;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use common::pb::chat_client::ChatClient;
use common::{env_or, env_parse, retry, shutdown_signal};
use redis::aio::ConnectionManager;
use serde_json::{json, Value};
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};

use crate::hub::Hub;

/// 종료 신호를 받은 뒤 연결들이 다른 서버로 옮겨 갈 때까지 기다려 주는 최대 시간
const DRAIN_DEADLINE: Duration = Duration::from_secs(8);

/// 모든 연결이 함께 쓰는 상태
pub struct AppState {
    /// 이 게이트웨이의 이름 (예: gateway-1)
    pub gateway_id: String,
    /// 방별 연결 목록과 팬아웃 담당
    pub hub: Arc<Hub>,
    /// 채팅 서버를 부르는 gRPC 클라이언트
    pub chat: ChatClient<Channel>,
    /// 접속 중 표시를 기록하는 Redis
    pub redis: ConnectionManager,
    /// 토큰 검증용 비밀 키
    pub jwt_secret: String,
    /// 연결 하나에 쌓아 둘 수 있는 프레임 수. 넘으면 느린 사용자로 본다.
    pub send_queue: usize,
    /// 다음 연결에 줄 번호
    pub next_conn_id: AtomicU64,
    /// 게이트웨이가 종료를 시작하면 true 로 바뀐다.
    pub drain: watch::Receiver<bool>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 로그 출력을 켠다.
    common::init_tracing();

    // 설정을 환경 변수에서 읽는다.
    let gateway_id = env_or("GATEWAY_ID", "gateway");
    let redis_url = env_or("REDIS_URL", "redis://redis:6379");
    let nats_url = env_or("NATS_URL", "nats://nats:4222");
    let chat_addrs = env_or("CHAT_SERVICE_ADDRS", "http://chat-service:50051");

    // Redis 와 NATS 에 연결한다. 아직 안 떠 있으면 뜰 때까지 재시도한다.
    let redis_client = redis::Client::open(redis_url)?;
    let redis = retry("redis", || ConnectionManager::new(redis_client.clone())).await;
    let nats = retry("nats", || async_nats::connect(nats_url.as_str())).await;

    // 채팅 서버 주소 목록(쉼표로 구분)을 읽어, 요청을 여러 대에 골고루 나눠 보내는 채널을 만든다.
    let endpoints = chat_addrs
        .split(',')
        .map(|addr| {
            Ok(Endpoint::from_shared(addr.trim().to_string())?
                // 연결 시도는 3초, 요청 하나는 10초까지 기다린다.
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10)))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let chat = ChatClient::new(Channel::balance_list(endpoints.into_iter()));

    // 종료 신호를 모든 연결에 알리기 위한 채널. 처음 값은 false.
    let (drain_tx, drain_rx) = watch::channel(false);
    let state = Arc::new(AppState {
        gateway_id: gateway_id.clone(),
        hub: Hub::new(nats),
        chat,
        redis,
        jwt_secret: env_or("JWT_SECRET", "dev-secret-change-me"),
        send_queue: env_parse("SEND_QUEUE", 256),
        next_conn_id: AtomicU64::new(1),
        drain: drain_rx.clone(),
    });

    // 주소와 처리 함수를 연결한다.
    let hub = state.hub.clone();
    let app = Router::new()
        // WebSocket 접속
        .route("/ws", get(conn::ws_handler))
        // 살아 있는지 확인용
        .route("/healthz", get(|| async { "ok" }))
        // 현재 상태 숫자 조회
        .route("/stats", get(stats))
        .with_state(state);

    // 부드러운 종료: 종료 신호(SIGTERM)가 오면 새 접속을 받지 않고,
    // 기존 연결들에게 다른 곳으로 다시 접속하라고 알린다.
    // 끝까지 안 나가는 연결이 있어도 정해진 시간이 지나면 무조건 종료한다.
    tokio::spawn(async move {
        // 종료 신호를 기다린다.
        shutdown_signal().await;
        tracing::info!("shutdown requested, draining connections");
        // 모든 연결에 "종료 시작"을 알린다.
        let _ = drain_tx.send(true);
        // 최대 대기 시간이 지나면 강제로 끝낸다.
        tokio::time::sleep(DRAIN_DEADLINE).await;
        tracing::warn!("drain deadline reached, exiting with connections still open");
        std::process::exit(0);
    });

    // 포트를 열고 접속을 받기 시작한다.
    let addr = env_or("LISTEN_ADDR", "0.0.0.0:8080");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("{gateway_id} listening on {addr}");
    let mut drained = drain_rx;
    axum::serve(listener, app)
        // 종료가 시작되면 새 접속 받기를 멈춘다.
        .with_graceful_shutdown(async move {
            let _ = drained.changed().await;
        })
        .await?;

    // 여기 왔다는 것은 포트가 닫혔다는 뜻이다.
    // 이미 WebSocket 으로 전환된 연결은 HTTP 서버가 세어 주지 않으므로,
    // 각 연결이 재접속 요청을 받고 떠날 때까지 직접 기다린다.
    while hub.connections.load(Ordering::Relaxed) > 0 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tracing::info!("all connections drained");
    std::process::exit(0);
}

/// 현재 상태를 숫자로 보여준다: 연결 수, 방 수, 버린 프레임 수, 끊은 연결 수.
async fn stats(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({
        "gateway": state.gateway_id,
        "connections": state.hub.connections.load(Ordering::Relaxed),
        "rooms": state.hub.room_count(),
        "dropped_frames": state.hub.dropped.load(Ordering::Relaxed),
        "kicked_slow_consumers": state.hub.kicked.load(Ordering::Relaxed),
    }))
}
