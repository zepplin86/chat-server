//! 채팅 서버: 아무것도 기억하지 않는 로직 계층.
//!
//! - gRPC (:50051) 게이트웨이용: 전송, 입장 확인, 지난 메시지 조회
//! - HTTP (:8081) 앱용: 로그인, 방 관리

mod error;
mod grpc;
mod http;
mod service;

use std::sync::Arc;

use common::pb::chat_server::ChatServer;
use common::snowflake::Snowflake;
use common::{env_or, env_parse, retry, shutdown_signal};
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use redis::aio::ConnectionManager;
use scylla::client::session_builder::SessionBuilder;
use sqlx::postgres::PgPoolOptions;

use crate::service::Service;

/// 지난 메시지 조회 쿼리.
/// 한 방의 한 구간(bucket) 안에서 순번 범위에 드는 메시지를 오래된 것부터 가져온다.
const FETCH_CQL: &str = "SELECT seq, message_id, sender_id, sender_name, kind, body, created_at \
     FROM chat.messages \
     WHERE room_id = ? AND bucket = ? AND seq > ? AND seq < ? \
     ORDER BY seq ASC LIMIT ?";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 로그 출력을 켠다.
    common::init_tracing();

    // 접속 주소들을 환경 변수에서 읽는다. 없으면 compose 안의 기본 주소를 쓴다.
    let database_url = env_or("DATABASE_URL", "postgres://chat:chat@postgres:5432/chat");
    let redis_url = env_or("REDIS_URL", "redis://redis:6379");
    let nats_url = env_or("NATS_URL", "nats://nats:4222");
    let kafka_brokers = env_or("KAFKA_BROKERS", "kafka:9092");
    let scylla_nodes = env_or("SCYLLA_NODES", "scylla:9042");
    // 이 서버의 번호. 메시지 고유 번호에 들어가므로 서버마다 달라야 한다.
    let node_id: i64 = env_parse("NODE_ID", 0);

    // PostgreSQL 에 연결한다 (연결을 여러 개 만들어 두고 돌려 쓴다).
    let pg = retry("postgres", || {
        PgPoolOptions::new()
            .max_connections(env_parse("PG_MAX_CONNECTIONS", 10))
            .connect(&database_url)
    })
    .await;

    // Redis 에 연결한다. ConnectionManager 는 끊기면 알아서 다시 붙는다.
    let redis_client = redis::Client::open(redis_url)?;
    let redis = retry("redis", || ConnectionManager::new(redis_client.clone())).await;

    // NATS(실시간 전달 통로)에 연결한다.
    let nats = retry("nats", || async_nats::connect(nats_url.as_str())).await;

    // ScyllaDB 에 연결한다.
    let scylla = retry("scylla", || async {
        SessionBuilder::new()
            .known_nodes(scylla_nodes.split(','))
            .build()
            .await
    })
    .await;
    // 조회 쿼리를 미리 준비해 둔다. 테이블이 아직 없으면 생길 때까지 기다린다.
    let fetch_stmt = retry("scylla schema", || scylla.prepare(FETCH_CQL)).await;

    // Kafka(기록 통로)로 보내는 생산자를 만든다.
    let kafka: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        // 모든 복제본이 받았다고 확인해야 성공으로 본다.
        .set("acks", "all")
        // 재시도 중에 같은 메시지가 두 번 기록되지 않게 한다.
        .set("enable.idempotence", "true")
        // 5초 안에 기록하지 못하면 실패로 처리한다.
        .set("message.timeout.ms", "5000")
        // 5밀리초 동안 모아서 한꺼번에 보낸다 (처리량을 높이기 위함).
        .set("linger.ms", "5")
        .create()?;

    // 연결과 설정을 한데 모아 모든 요청이 공유하게 한다.
    let service = Arc::new(Service {
        pg,
        redis,
        nats,
        kafka,
        scylla,
        fetch_stmt,
        ids: Snowflake::new(node_id),
        jwt_secret: env_or("JWT_SECRET", "dev-secret-change-me"),
        rate_limit_per_sec: env_parse("RATE_LIMIT_PER_SEC", 5),
        recent_cap: env_parse("RECENT_CACHE_SIZE", 500),
    });

    // gRPC 서버를 준비한다. 종료 신호가 오면 처리 중인 요청을 마치고 멈춘다.
    let grpc_addr = env_or("GRPC_ADDR", "0.0.0.0:50051").parse()?;
    let grpc = tonic::transport::Server::builder()
        .add_service(ChatServer::new(grpc::ChatGrpc(service.clone())))
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    // HTTP 서버를 준비한다. 마찬가지로 종료 신호에 맞춰 멈춘다.
    let http_addr = env_or("HTTP_ADDR", "0.0.0.0:8081");
    let listener = tokio::net::TcpListener::bind(&http_addr).await?;
    let http =
        axum::serve(listener, http::router(service)).with_graceful_shutdown(shutdown_signal());

    tracing::info!(node_id, "chat-service listening: grpc {grpc_addr}, http {http_addr}");
    // 두 서버를 동시에 돌리고, 둘 다 끝날 때까지 기다린다.
    let (grpc_result, http_result) = tokio::join!(grpc, http);
    grpc_result?;
    http_result?;
    Ok(())
}
