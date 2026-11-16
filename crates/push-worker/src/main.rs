//! 푸시 담당: 기록 통로(Kafka)를 읽어, 접속해 있지 않은 방 멤버에게 알림을 보낸다.
//!
//! 실제 APNs/FCM 발송은 아직 없고 로그만 남긴다. 실제 연동은 `deliver` 함수에 넣으면 된다.
//! 라이브 방은 알림을 보내지 않는다.

use std::time::Duration;

use common::model::{ChatMessage, RoomType};
use common::{env_or, keys, retry, shutdown_signal, KAFKA_TOPIC};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use redis::aio::ConnectionManager;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 로그 출력을 켠다.
    common::init_tracing();

    // 접속 주소를 환경 변수에서 읽는다.
    let database_url = env_or("DATABASE_URL", "postgres://chat:chat@postgres:5432/chat");
    let redis_url = env_or("REDIS_URL", "redis://redis:6379");
    let kafka_brokers = env_or("KAFKA_BROKERS", "kafka:9092");

    // PostgreSQL(방 멤버 조회용)과 Redis(접속 여부 확인용)에 연결한다.
    let pg = retry("postgres", || {
        PgPoolOptions::new().max_connections(5).connect(&database_url)
    })
    .await;
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis = retry("redis", || ConnectionManager::new(redis_client.clone())).await;

    // Kafka 소비자를 만든다.
    // 알림은 "되면 좋은" 수준이라, 읽은 위치를 자동으로 기록하고
    // 처음 시작할 때는 지난 메시지를 건너뛰고 새 메시지부터 본다.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        // 저장 담당과 다른 그룹 이름이라서, 같은 메시지를 저장 담당과 별개로 각자 받는다.
        .set("group.id", "push-worker")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .create()?;
    consumer.subscribe(&[KAFKA_TOPIC])?;
    tracing::info!("push-worker consuming {KAFKA_TOPIC}");

    // 종료 신호를 받을 준비
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        // 종료 신호가 오면 끝내고, 아니면 다음 기록을 하나 받는다.
        let record = tokio::select! {
            _ = &mut shutdown => break,
            record = consumer.recv() => record,
        };
        let record = match record {
            Ok(record) => record,
            // 받기 실패: 1초 쉬고 다시 시도한다.
            Err(err) => {
                tracing::warn!("kafka receive failed: {err}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        // 내용을 메시지로 풀어 본다. 해석이 안 되면 건너뛴다.
        let Some(Ok(message)) = record.payload().map(serde_json::from_slice::<ChatMessage>)
        else {
            continue;
        };
        // 그룹 방이 아니면(라이브 방이면) 알림을 보내지 않는다.
        if message.room_type != RoomType::Group {
            continue;
        }
        // 접속해 있지 않은 멤버를 찾아 알림을 보낸다. 실패해도 경고만 남기고 계속한다.
        if let Err(err) = notify_offline_members(&pg, &mut redis, &message).await {
            tracing::warn!(room_id = message.room_id, "push failed: {err:#}");
        }
    }

    tracing::info!("push-worker stopped");
    Ok(())
}

/// 방 멤버 중 지금 접속해 있지 않은 사람에게 알림을 보낸다.
async fn notify_offline_members(
    pg: &PgPool,
    redis: &mut ConnectionManager,
    message: &ChatMessage,
) -> anyhow::Result<()> {
    // 이 방의 멤버를 가져온다. 보낸 사람 본인은 뺀다.
    let members: Vec<(i64,)> =
        sqlx::query_as("SELECT user_id FROM room_members WHERE room_id = $1 AND user_id <> $2")
            .bind(message.room_id)
            .bind(message.sender_id)
            .fetch_all(pg)
            .await?;
    // 다른 멤버가 없으면 할 일이 없다.
    if members.is_empty() {
        return Ok(());
    }

    // 멤버 각각의 "접속 중" 키가 있는지를 Redis 에 한꺼번에 물어본다.
    // (한 명씩 묻는 대신 묶어서 한 번만 왕복한다)
    let mut pipe = redis::pipe();
    for (user_id,) in &members {
        pipe.exists(keys::presence(*user_id));
    }
    let online: Vec<bool> = pipe.query_async(redis).await?;

    // 접속 중이 아닌 멤버에게만 알림을 보낸다.
    for ((user_id,), online) in members.iter().zip(online) {
        if !online {
            deliver(*user_id, message);
        }
    }
    Ok(())
}

/// APNs/FCM 자리. 지금은 로그만 남긴다.
fn deliver(user_id: i64, message: &ChatMessage) {
    // 알림에 보여줄 미리보기: 내용의 앞 40글자
    let preview: String = message.body.chars().take(40).collect();
    tracing::info!(
        to_user = user_id,
        room_id = message.room_id,
        seq = message.seq,
        "PUSH {}: {}",
        message.sender_name,
        preview
    );
}
