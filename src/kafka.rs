//! Kafka 와 주고받는 일. 채팅 서버는 적고(append), 저장 담당은 읽는다(reader).
//!
//! Kafka 는 받은 것을 순서대로 디스크에 적어 두는 프로그램이다. 끝에 덧붙이기만 하는 긴 공책이라고 보면 된다.
//! NATS 와 견주면 이렇다.
//! - NATS: 그 순간 듣고 있는 쪽에 전하고 잊는다. 늦게 온 쪽은 못 받는다.
//! - Kafka: 적어 두고 남긴다. 읽는 쪽이 꺼져 있었어도, 다시 켜지면 못 읽은 곳부터 이어서 읽는다.
//!
//! 용어 몇 개.
//! - 토픽(topic): 공책 한 권의 이름. 우리는 "chat.messages" 한 권만 쓴다.
//! - 기록(record): 공책에 적힌 한 줄. 여기서는 메시지 하나다.
//! - 오프셋(offset): 그 줄이 몇 번째 줄인지. 읽는 쪽은 "나는 몇 번째 줄까지 읽었다"를 Kafka 에 적어 둔다.
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};

/// Kafka 주소. docker-compose.yml 에 적은 포트와 같아야 한다.
/// KAFKA_BROKERS 환경 변수가 있으면 그 값을 대신 쓴다.
const DEFAULT_KAFKA_BROKERS: &str = "localhost:9092";

/// 메시지를 적어 두는 토픽(공책)의 이름.
pub const TOPIC: &str = "chat.messages";

/// Kafka 에 적는 메시지 하나. JSON 으로 바꿔 적는다.
/// 예: {"seq":1763611200000000,"room_id":1,"name":"철수","body":"안녕"}
///
/// Serialize 는 이 구조를 JSON 으로 바꾸는 능력, Deserialize 는 JSON 을 이 구조로 되돌리는 능력이다.
/// 채팅 서버가 바꿔서 적고, 저장 담당이 읽어서 되돌린다.
#[derive(Serialize, Deserialize)]
pub struct Record {
    /// 채팅 서버가 매긴 순번.
    pub seq: i64,
    pub room_id: i64,
    /// 말한 사람.
    pub name: String,
    pub body: String,
}

/// Kafka 주소를 담은 설정을 만든다. 적는 쪽, 읽는 쪽이 모두 여기서 시작한다.
fn config() -> ClientConfig {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| DEFAULT_KAFKA_BROKERS.to_string());
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    // localhost 는 주소가 두 가지(IPv4 의 127.0.0.1, IPv6 의 ::1)인데 Kafka 컨테이너는 127.0.0.1 로만 열려 있다.
    // IPv4 만 쓰게 한다. 이 줄이 없으면 ::1 로 먼저 가 보고 실패했다는 로그가 계속 찍힌다.
    config.set("broker.address.family", "v4");
    // Kafka 라이브러리가 스스로 찍는 로그를 줄인다. 필요한 것은 우리 코드가 찍는다.
    config.set("log_level", "2");
    config
}

/// 토픽이 없으면 만든다. 이미 있으면 아무 일도 하지 않는다.
/// Kafka 가 꺼져 있으면 여기서 실패한다. 시작할 때 한 번 불러 Kafka 가 켜져 있는지도 함께 확인한다.
pub async fn ensure_topic() -> Result<(), KafkaError> {
    let admin: AdminClient<DefaultClientContext> = config().create()?;
    // 뒤의 두 숫자: 공책을 몇 갈래로 나눌지(1), 복사본을 몇 개 둘지(1). Kafka 가 한 대뿐이라 둘 다 1 이다.
    let topic = NewTopic::new(TOPIC, 1, TopicReplication::Fixed(1));
    // 5초 안에 답이 없으면 꺼져 있는 것으로 본다.
    let options = AdminOptions::new().request_timeout(Some(Duration::from_secs(5)));
    for result in admin.create_topics(&[topic], &options).await? {
        match result {
            Ok(_) => println!("[Kafka] 토픽 {TOPIC} 을(를) 만듦"),
            // 이미 있다. 다른 프로그램이 먼저 만들었거나 전에 만들어 둔 것이다.
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((_, code)) => return Err(KafkaError::AdminOp(code)),
        }
    }
    Ok(())
}

/// 적는 쪽 손잡이를 만든다. 채팅 서버가 쓴다.
pub fn writer() -> Result<FutureProducer, KafkaError> {
    config()
        // 5초 안에 적지 못하면 포기하고 실패로 알린다. (기본값은 5분이다)
        .set("message.timeout.ms", "5000")
        .create()
}

/// 메시지 하나를 공책 끝에 적는다. Kafka 가 "디스크에 적었다"고 답할 때까지 기다린다.
/// 성공했다면 이 메시지는 사라지지 않는다. 저장 담당이 지금 꺼져 있어도 나중에 읽어 간다.
pub async fn append(writer: &FutureProducer, record: &Record) -> Result<(), KafkaError> {
    let payload = serde_json::to_string(record).expect("Record 는 언제나 JSON 으로 바뀐다");
    // 열쇠(key)로 방 번호를 붙인다. 지금은 쓰이지 않지만, 공책을 여러 갈래로 나누게 되면
    // 같은 열쇠의 기록은 같은 갈래로 가서 한 방의 순서가 지켜진다.
    let key = record.room_id.to_string();
    writer
        .send(FutureRecord::to(TOPIC).key(&key).payload(&payload), Timeout::Never)
        .await
        // 실패하면 (이유, 못 적은 기록) 이 돌아온다. 이유만 남긴다.
        .map_err(|(error, _)| error)?;
    Ok(())
}

/// 읽는 쪽 손잡이를 만들고 토픽을 읽기 시작한다. 저장 담당이 쓴다.
///
/// group 은 읽는 쪽의 이름이다. Kafka 는 이 이름마다 "어디까지 읽었는지"를 따로 기억한다.
/// 그래서 저장 담당을 껐다 켜도 같은 이름으로 오면 못 읽은 곳부터 이어서 읽는다.
pub fn reader(group: &str) -> Result<StreamConsumer, KafkaError> {
    let reader: StreamConsumer = config()
        .set("group.id", group)
        // "어디까지 읽었는지"를 알아서 적지 않게 한다. DB 에 넣는 데 성공한 뒤에 직접 적는다.
        // 알아서 적게 두면, 읽기만 하고 DB 에 넣기 전에 죽었을 때 그 메시지를 건너뛰게 된다.
        .set("enable.auto.commit", "false")
        // 읽는 쪽이 6초 동안 소식이 없으면 Kafka 는 죽은 것으로 본다. (기본값은 45초다)
        // 죽은 것으로 봐야 같은 이름으로 다시 온 쪽에게 읽을 차례를 넘겨준다.
        // 인사하고 나가면(Ctrl+C) 바로 넘겨주고, 갑자기 죽으면 이만큼 기다린 뒤에 넘겨준다.
        .set("session.timeout.ms", "6000")
        // 이 이름으로 처음 왔다면 공책의 맨 앞부터 읽는다.
        .set("auto.offset.reset", "earliest")
        .create()?;
    reader.subscribe(&[TOPIC])?;
    Ok(reader)
}
