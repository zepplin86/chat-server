//! 프로그램들이 함께 쓰는 것들.
//!
//! 이 프로젝트는 프로그램 세 개를 만든다. (src/bin/ 아래 폴더 하나가 프로그램 하나다)
//! - src/bin/gateway: 게이트웨이. 브라우저 연결(WebSocket)을 붙들고 있는다.
//! - src/bin/chat: 채팅 서버. 로그인, 방, 순번 같은 로직을 맡는다.
//! - src/bin/persist: 저장 담당. Kafka 에 적힌 메시지를 읽어 DB 에 넣는다.
//!
//! 이 파일(lib.rs)과 여기서 포함한 것은 세 프로그램이 모두 가져다 쓸 수 있다.

/// NATS 와 주고받는 일. 채팅 서버는 내보내고, 게이트웨이는 받는다.
pub mod bus;

/// PostgreSQL 과 주고받는 일. 채팅 서버는 읽고, 저장 담당은 메시지를 넣는다.
pub mod db;

/// Kafka 와 주고받는 일. 채팅 서버는 적고, 저장 담당은 읽는다.
pub mod kafka;

/// 게이트웨이와 채팅 서버 사이의 약속. proto/chat.proto 에서 만들어진 코드다.
/// 직접 쓴 코드가 아니라 빌드할 때 생긴다. (build.rs)
pub mod proto {
    // "chat" 은 chat.proto 안의 package 이름이다.
    tonic::include_proto!("chat");
}

/// 메시지 하나를 화면이 받는 모양인 JSON 한 줄로 만든다.
/// 예: {"body":"안녕","name":"철수","seq":7}
/// seq 는 채팅 서버가 매긴 순번이다. 화면은 이것으로 순서를 맞추고 중복을 가려낸다.
///
/// 채팅 서버는 새 메시지를 NATS 에 내보낼 때, 게이트웨이는 지난 대화를 화면에 보낼 때 쓴다.
pub fn to_line(seq: i64, name: &str, body: &str) -> String {
    serde_json::json!({ "seq": seq, "name": name, "body": body }).to_string()
}
