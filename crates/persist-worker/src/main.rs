//! 저장 담당: 기록 통로(Kafka)를 읽어 메시지를 ScyllaDB 에 저장한다.
//!
//! "어디까지 읽었는지"(오프셋)는 DB 저장이 성공한 뒤에만 기록한다.
//! 그래서 중간에 죽으면 메시지를 잃는 대신 같은 메시지를 다시 읽게 된다.
//! 다시 읽어도 문제없다: 기본 키가 같아서 같은 행을 덮어쓸 뿐이다.

use std::time::Duration;

use common::model::{ChatMessage, RoomType};
use common::{bucket_of, env_or, retry, shutdown_signal, KAFKA_TOPIC};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use scylla::client::session_builder::SessionBuilder;
use scylla::value::CqlTimestamp;

/// 메시지 한 건을 저장하는 쿼리
const INSERT_CQL: &str = "INSERT INTO chat.messages \
     (room_id, bucket, seq, message_id, sender_id, sender_name, kind, body, created_at) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 로그 출력을 켠다.
    common::init_tracing();

    // 접속 주소를 환경 변수에서 읽는다.
    let kafka_brokers = env_or("KAFKA_BROKERS", "kafka:9092");
    let scylla_nodes = env_or("SCYLLA_NODES", "scylla:9042");

    // ScyllaDB 에 연결한다.
    let scylla = retry("scylla", || async {
        SessionBuilder::new()
            .known_nodes(scylla_nodes.split(','))
            .build()
            .await
    })
    .await;
    // 저장 쿼리를 미리 준비해 둔다. 테이블이 아직 없으면 생길 때까지 기다린다.
    let insert = retry("scylla schema", || scylla.prepare(INSERT_CQL)).await;

    // Kafka 소비자를 만든다.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        // 소비자 그룹 이름. 같은 이름의 worker 를 여러 개 띄우면 일을 나눠 갖는다.
        .set("group.id", "persist-worker")
        // "어디까지 읽었는지"를 자동으로 기록하지 않는다. 저장에 성공한 뒤 직접 기록한다.
        .set("enable.auto.commit", "false")
        // 처음 시작할 때는 가장 오래된 메시지부터 읽는다 (하나도 빠뜨리지 않기 위함).
        .set("auto.offset.reset", "earliest")
        .create()?;
    // 토픽을 구독한다.
    consumer.subscribe(&[KAFKA_TOPIC])?;
    tracing::info!("persist-worker consuming {KAFKA_TOPIC}");

    // 종료 신호를 받을 준비
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    // 지금까지 저장한 건수
    let mut stored: u64 = 0;

    loop {
        // 종료 신호가 오면 루프를 끝내고, 아니면 다음 기록을 하나 받는다.
        let record = tokio::select! {
            _ = &mut shutdown => break,
            record = consumer.recv() => record,
        };
        let record = match record {
            Ok(record) => record,
            // Kafka 에서 받기 실패: 1초 쉬고 다시 시도한다.
            Err(err) => {
                tracing::warn!("kafka receive failed: {err}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // 기록의 내용(JSON)을 메시지로 풀어 본다.
        match record.payload().map(serde_json::from_slice::<ChatMessage>) {
            // 그룹 방 메시지: DB 에 저장한다.
            Some(Ok(message)) if message.room_type == RoomType::Group => {
                // 성공할 때까지 계속 시도한다.
                // 저장하지 못한 채 다음으로 넘어가면 그 메시지는 영영 사라지기 때문이다.
                loop {
                    let values = (
                        message.room_id,
                        // 순번으로 구간 번호를 계산한다 (10만 개 단위).
                        bucket_of(message.seq),
                        message.seq,
                        message.message_id,
                        message.sender_id,
                        message.sender_name.as_str(),
                        message.kind.to_i8(),
                        message.body.as_str(),
                        CqlTimestamp(message.created_at),
                    );
                    match scylla.execute_unpaged(&insert, values).await {
                        Ok(_) => break,
                        // DB 쓰기 실패: 1초 쉬고 같은 메시지를 다시 쓴다.
                        Err(err) => {
                            tracing::warn!("scylla write failed, retrying: {err}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                stored += 1;
                // 1000건마다 진행 상황을 로그로 남긴다.
                if stored % 1000 == 0 {
                    tracing::info!(stored, "messages stored");
                }
            }
            // 라이브 방 메시지: DB 에 저장하지 않는다. Redis 의 최근 메시지 보관함에만 남는다.
            Some(Ok(_)) => {}
            // 해석할 수 없는 기록: 로그를 남기고 건너뛴다. (여기서 멈추면 뒤의 메시지가 전부 막힌다)
            Some(Err(err)) => tracing::error!(
                partition = record.partition(),
                offset = record.offset(),
                "skipping undecodable record: {err}"
            ),
            // 내용이 없는 기록: 무시한다.
            None => {}
        }

        // 이 기록까지 처리했다고 Kafka 에 알린다. 다음에 시작하면 이 다음부터 읽는다.
        if let Err(err) = consumer.commit_message(&record, CommitMode::Async) {
            tracing::warn!("offset commit failed: {err}");
        }
    }

    tracing::info!(stored, "persist-worker stopped");
    Ok(())
}
