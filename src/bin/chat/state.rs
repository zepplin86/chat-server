//! 모든 요청이 함께 쓰는 것들.
use async_nats::Client;
use rdkafka::producer::FutureProducer;
use sqlx::PgPool;

use chat_server::store::Store;

use crate::seq::SeqMaker;

/// 모든 요청이 함께 쓰는 것들.
/// pub 을 붙여야 다른 파일(main.rs, grpc.rs 등)에서 쓸 수 있다. 안 붙이면 이 파일 안에서만 보인다.
pub struct AppState {
    /// PostgreSQL 로 가는 연결 묶음(pool). 계정, 입장권, 방을 읽고 쓴다.
    pub db: PgPool,
    /// ScyllaDB 로 가는 연결. 지난 대화를 읽는다. 넣는 일은 저장 담당이 한다.
    pub store: Store,
    /// NATS 로 가는 연결. 메시지를 여기로 내보내면 모든 게이트웨이가 받는다. (지금 전하는 일)
    pub nats: Client,
    /// Kafka 에 적는 손잡이. 메시지를 여기에 적어 두면 저장 담당이 읽어 DB 에 넣는다. (남기는 일)
    pub kafka: FutureProducer,
    /// 순번을 매기는 것. DB 를 기다리지 않으므로 채팅 서버가 직접 매긴다.
    pub seq: SeqMaker,
}
