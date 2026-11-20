//! 저장 담당: Kafka 에 적힌 메시지를 읽어 ScyllaDB 에 넣는다. 하는 일은 이것 하나다.
//!
//! 채팅 서버는 Kafka 에 적어 두기만 하고 바로 다음 일을 한다. 여기서 그것을 읽어 저장소에 넣는다.
//! 저장소가 느려지면 이 프로그램만 밀린다. 채팅은 밀리지 않는다.
//!
//! 7단계에서 넣는 곳이 PostgreSQL 에서 ScyllaDB 로 바뀌었다. 이 프로그램은 이제 PostgreSQL 을 모른다.
//!
//! 브라우저도, 게이트웨이도, 채팅 서버도 이 프로그램을 부르지 않는다. 문을 열지 않는다. 읽고 넣기만 한다.
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;

// chat_server 는 이 프로젝트의 src/lib.rs 다. 세 프로그램이 함께 쓰는 것들이 들어 있다.
use chat_server::kafka::{self, Record};
use chat_server::store::Store;

#[tokio::main]
async fn main() {
    // ScyllaDB 에 접속한다. 꺼져 있으면 시작하지 않는다.
    let store = Store::connect()
        .await
        .expect("ScyllaDB 에 접속할 수 없습니다. docker compose up -d 로 먼저 켜세요 (켜지는 데 1분쯤 걸립니다)");

    // Kafka 에 토픽이 있는지 확인하고(없으면 만들고), 읽기 시작한다.
    kafka::ensure_topic()
        .await
        .expect("Kafka 에 접속할 수 없습니다. docker compose up -d 로 Kafka 를 먼저 켜세요");
    // "persist" 는 읽는 쪽의 이름이다. Kafka 가 이 이름으로 "어디까지 읽었는지"를 기억해 준다.
    let reader = kafka::reader("persist").expect("Kafka 읽기를 시작할 수 없습니다");
    println!("저장 담당: Kafka 의 {} 을(를) 읽어 ScyllaDB 에 넣는 중", kafka::TOPIC);

    loop {
        // 두 가지를 동시에 기다린다: 끄라는 신호(Ctrl+C), 다음 기록 하나.
        // 새로 적힌 것이 없으면 여기서 기다린다.
        let record = tokio::select! {
            // Ctrl+C 가 눌렸다. 반복을 빠져나가 아래에서 인사하고 끝낸다.
            _ = tokio::signal::ctrl_c() => break,
            record = reader.recv() => record,
        };
        let record = match record {
            Ok(record) => record,
            Err(error) => {
                println!("[Kafka] 읽지 못함, 1초 뒤 다시: {error}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // 기록의 내용(JSON)을 메시지로 되돌린다.
        match record.payload().map(serde_json::from_slice::<Record>) {
            Some(Ok(message)) => {
                // ScyllaDB 에 넣는다. 될 때까지 다시 한다.
                // 넣지 못한 채 다음으로 넘어가면 그 메시지는 영영 저장소에 없게 된다.
                // 여기서 기다리는 동안 채팅은 멈추지 않는다. Kafka 에 읽을 것이 쌓일 뿐이다.
                //
                // 같은 메시지를 두 번 넣어도 된다. 같은 방, 같은 순번이면 같은 줄을 덮어쓸 뿐이다. (store.rs)
                // 그래서 6단계에 있던 "이미 있으면 넘어간다"는 확인이 없어졌다.
                loop {
                    match store.save_message(message.room_id, message.seq, &message.name, &message.body).await {
                        Ok(()) => {
                            println!("[저장] 순번 {} ({}: {:?})", message.seq, message.name, message.body);
                            break;
                        }
                        Err(error) => {
                            println!("[저장] 순번 {} 을(를) 넣지 못함, 1초 뒤 다시: {error}", message.seq);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
            // 읽을 수 없는 기록이다. 건너뛴다. 여기서 멈추면 뒤의 메시지가 전부 막힌다.
            Some(Err(error)) => println!("[Kafka] {}번째 기록을 읽을 수 없어 건너뜀: {error}", record.offset()),
            // 내용이 없는 기록이다.
            None => {}
        }

        // 저장소에 넣었으니 "여기까지 읽었다"를 Kafka 에 적는다.
        // 넣기 전에 적으면 안 된다. 그 사이에 꺼지면 넣지 못한 메시지를 읽은 것으로 치게 된다.
        // 넣은 뒤에 적으면, 그 사이에 꺼졌을 때 같은 메시지를 한 번 더 읽는다. 그래도 같은 줄을 덮어쓸 뿐이라 괜찮다.
        if let Err(error) = reader.commit_message(&record, CommitMode::Async) {
            println!("[Kafka] 어디까지 읽었는지 적지 못함: {error}");
        }
    }

    // 읽는 손잡이를 내려놓으면 Kafka 에 "나 나간다"고 알린다.
    // 그래야 다시 켰을 때 Kafka 가 기다리지 않고 바로 읽을 차례를 준다.
    drop(reader);
    println!("저장 담당: 끝냄");
}
