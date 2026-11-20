//! 옮기기: PostgreSQL 에 있던 옛 메시지를 ScyllaDB 로 복사한다. 7단계로 넘어올 때 한 번만 돌린다.
//!
//! 6단계까지 메시지는 PostgreSQL 의 messages 표에 쌓였다. 7단계부터 채팅 서버는 ScyllaDB 에서 읽는다.
//! 이것을 돌리지 않으면 예전 대화가 화면에 안 보인다. (PostgreSQL 에는 그대로 남아 있다)
//!
//! 여러 번 돌려도 된다. 같은 방, 같은 순번은 덮어쓰기라 줄이 늘지 않는다.
//! PostgreSQL 쪽은 읽기만 한다. 지우지 않는다.
use chat_server::db;
use chat_server::store::Store;

#[tokio::main]
async fn main() {
    let db = db::connect()
        .await
        .expect("DB 에 접속할 수 없습니다. docker compose up -d 로 DB 를 먼저 켜세요");
    let store = Store::connect()
        .await
        .expect("ScyllaDB 에 접속할 수 없습니다. docker compose up -d 로 먼저 켜세요 (켜지는 데 1분쯤 걸립니다)");

    let messages = db::load_all_messages(&db)
        .await
        .expect("PostgreSQL 에서 옛 메시지를 읽지 못했습니다");
    println!("옮길 메시지 {}개", messages.len());

    for (done, (seq, room_id, name, body)) in messages.iter().enumerate() {
        store
            .save_message(*room_id, *seq, name, body)
            .await
            .unwrap_or_else(|error| panic!("순번 {seq} 을(를) 옮기지 못했습니다: {error}"));
        // 만 개마다 어디까지 했는지 알린다.
        if (done + 1) % 10_000 == 0 {
            println!("  {}개 옮김", done + 1);
        }
    }
    println!("다 옮겼습니다. PostgreSQL 의 messages 표는 그대로 두었습니다");
}
