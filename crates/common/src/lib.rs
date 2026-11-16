//! 모든 서비스(게이트웨이, 채팅 서버, 저장 담당, 푸시 담당)가 함께 쓰는 타입과 도우미 함수.

// 토큰 발급/검증
pub mod auth;
// 메시지, 방 종류 등 데이터 모양
pub mod model;
// 메시지 고유 번호 생성기
pub mod snowflake;

// proto/chat.proto 로부터 빌드 시점에 자동 생성된 gRPC 코드.
// (생성은 build.rs 가 담당한다)
pub mod pb {
    tonic::include_proto!("chat.v1");
}

use std::future::Future;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 기록 통로(Kafka)의 토픽 이름.
/// 파티션 키로 방 번호를 쓰기 때문에, 한 방의 메시지는 항상 같은 파티션에 순서대로 쌓인다.
pub const KAFKA_TOPIC: &str = "chat.messages";

/// ScyllaDB 파티션 하나에 담을 메시지 수.
/// 방 하나의 메시지가 한 덩어리로 무한히 커지는 것을 막기 위해 10만 개 단위로 나눈다.
pub const BUCKET_SIZE: i64 = 100_000;

/// 순번(seq)이 속한 구간 번호를 구한다. 예: seq 250000 → 구간 2
pub fn bucket_of(seq: i64) -> i32 {
    (seq / BUCKET_SIZE) as i32
}

/// 실시간 전달 통로(NATS)에서 이 방이 쓰는 주제 이름. 예: "room.42"
pub fn room_subject(room_id: i64) -> String {
    format!("room.{room_id}")
}

/// Redis 키 이름을 한곳에서 만든다. 서비스마다 키 이름이 어긋나지 않게 하기 위함.
pub mod keys {
    /// 사용자가 접속 중인지 표시하는 키 (짧은 만료 시간이 붙는다)
    pub fn presence(user_id: i64) -> String {
        format!("presence:{user_id}")
    }
    /// 방의 현재 순번 카운터
    pub fn room_seq(room_id: i64) -> String {
        format!("room:{room_id}:seq")
    }
    /// 방의 최근 메시지 묶음 (순번으로 정렬된 집합)
    pub fn room_recent(room_id: i64) -> String {
        format!("room:{room_id}:recent")
    }
    /// 도배 방지용 카운터. 사용자별, 1초 단위로 하나씩 만들어진다.
    pub fn rate_limit(user_id: i64, second: i64) -> String {
        format!("rl:{user_id}:{second}")
    }
    /// 중복 전송 방지용 키. 같은 임시 번호(client_msg_id)가 다시 오면 이 키로 알아챈다.
    pub fn dedup(room_id: i64, user_id: i64, client_msg_id: &str) -> String {
        format!("dedup:{room_id}:{user_id}:{client_msg_id}")
    }
}

/// 환경 변수를 읽는다. 없으면 기본값을 쓴다.
pub fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// 환경 변수를 읽어 숫자 등 원하는 타입으로 바꾼다. 없거나 형식이 틀리면 기본값을 쓴다.
pub fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// 현재 시각을 1970년 1월 1일부터 흐른 밀리초로 돌려준다.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 로그 출력을 켠다. RUST_LOG 환경 변수가 없으면 info 수준 이상만 찍는다.
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// 성공할 때까지 `connect` 를 반복 호출한다.
/// 서비스가 시작할 때 DB 등이 아직 뜨는 중이어도 죽지 않고 기다리게 하기 위함.
pub async fn retry<T, E, F, Fut>(what: &str, mut connect: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    // 처음에는 1초 기다리고, 실패할 때마다 두 배로 늘린다 (최대 10초).
    let mut delay = 1;
    loop {
        match connect().await {
            // 연결 성공: 결과를 돌려주고 끝낸다.
            Ok(value) => {
                tracing::info!("connected to {what}");
                return value;
            }
            // 연결 실패: 경고를 남기고 잠시 쉰 뒤 다시 시도한다.
            Err(err) => {
                tracing::warn!("{what} not ready ({err}), retrying in {delay}s");
                tokio::time::sleep(Duration::from_secs(delay)).await;
                delay = (delay * 2).min(10);
            }
        }
    }
}

/// 종료 신호(SIGTERM 또는 Ctrl-C)가 올 때까지 기다린다.
/// `docker compose stop` 은 SIGTERM 을 보낸다.
pub async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    // SIGTERM 수신기를 등록한다.
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    // 둘 중 먼저 오는 신호에 반응한다.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
