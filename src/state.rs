//! 모든 연결이 함께 쓰는 것들.
use std::sync::Mutex;

use tokio::sync::broadcast;

/// 모든 연결이 함께 쓰는 것들. 서버의 메모리에만 있으므로 서버를 끄면 사라진다.
/// pub 을 붙여야 다른 파일(main.rs, ws.rs)에서 쓸 수 있다. 안 붙이면 이 파일 안에서만 보인다.
pub struct AppState {
    /// 지금까지의 대화. 늦게 들어온 사람에게 보여 주기 위해 모아 둔다.
    /// 여러 연결이 동시에 건드리므로 한 번에 하나만 들어갈 수 있게 잠금(Mutex)으로 감싼다.
    pub history: Mutex<Vec<String>>,
    /// 방송 채널의 보내는 쪽 손잡이. 여기에 메시지를 넣으면 구독 중인 모든 연결이 받는다.
    pub tx: broadcast::Sender<String>,
}

impl AppState {
    /// 대화 기록이 비어 있는 새 묶음을 만든다.
    pub fn new() -> Self {
        // 방송 채널을 만든다. 256 은 느린 연결을 위해 쌓아 둘 수 있는 메시지 수.
        // tx 는 보내는 쪽 손잡이다. 받는 쪽은 연결마다 따로 만들 것이라 여기서는 버린다.
        let (tx, _) = broadcast::channel::<String>(256);
        Self {
            history: Mutex::new(Vec::new()),
            tx,
        }
    }
}
