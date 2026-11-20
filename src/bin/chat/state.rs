//! 모든 요청이 함께 쓰는 것들.
use async_nats::Client;
use sqlx::PgPool;

/// 모든 요청이 함께 쓰는 것들.
/// pub 을 붙여야 다른 파일(main.rs, grpc.rs 등)에서 쓸 수 있다. 안 붙이면 이 파일 안에서만 보인다.
///
/// 게이트웨이의 묶음과 견주어 보면 두 프로그램의 일이 갈린 것이 보인다.
/// 여기에는 DB 가 있고 방송 채널이 없다. 게이트웨이에는 방송 채널이 있고 DB 가 없다.
pub struct AppState {
    /// PostgreSQL 로 가는 연결 묶음(pool). DB 를 아는 것은 이제 채팅 서버뿐이다.
    /// DB 연결은 맺는 데 시간이 걸려서, 몇 개를 미리 맺어 두고 돌려 쓴다.
    pub db: PgPool,
    /// NATS 로 가는 연결. 저장한 메시지를 여기로 내보내면 모든 게이트웨이가 받는다.
    pub nats: Client,
}
