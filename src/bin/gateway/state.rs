//! 모든 연결이 함께 쓰는 것들.
use tokio::sync::broadcast;
use tonic::transport::Channel;

use chat_server::proto::chat_client::ChatClient;

/// 모든 연결이 함께 쓰는 것들.
/// pub 을 붙여야 다른 파일(main.rs, ws.rs)에서 쓸 수 있다. 안 붙이면 이 파일 안에서만 보인다.
///
/// DB 연결이 없다. 게이트웨이는 DB 를 모른다. DB 가 필요한 일은 전부 채팅 서버에 부탁한다.
pub struct AppState {
    /// 방송 채널의 보내는 쪽 손잡이. 여기에 메시지를 넣으면 구독 중인 모든 연결이 받는다.
    /// 메모리에만 있다. 지금 접속해 있는 사람에게 전달하는 용도라 저장할 필요가 없다.
    ///
    /// 채널은 게이트웨이 전체에 하나뿐이고, 모든 방의 메시지가 여기로 지나간다.
    /// 그래서 (방 번호, 메시지) 를 한 쌍으로 넣는다. 받는 쪽은 자기 방 것만 골라 전달한다.
    pub tx: broadcast::Sender<(i64, String)>,
    /// 채팅 서버로 가는 gRPC 연결. 입장(Join)과 말하기(Say)를 부탁할 때 쓴다.
    /// 복제(clone)해서 쓴다. 복제되는 것은 손잡이뿐이고 연결은 하나를 함께 쓴다.
    pub chat: ChatClient<Channel>,
    /// 이 게이트웨이의 이름표. 포트 번호를 쓴다. 예: "8081"
    /// 여러 대 중 지금 붙은 것이 어느 것인지 화면에 알려 주는 데만 쓴다.
    pub server: String,
}

impl AppState {
    /// 채팅 서버로 가는 연결과 이름표를 받아 새 묶음을 만든다.
    pub fn new(chat: ChatClient<Channel>, server: String) -> Self {
        // 방송 채널을 만든다. 256 은 느린 연결을 위해 쌓아 둘 수 있는 메시지 수.
        // tx 는 보내는 쪽 손잡이다. 받는 쪽은 연결마다 따로 만들 것이라 여기서는 버린다.
        let (tx, _) = broadcast::channel::<(i64, String)>(256);
        Self { tx, chat, server }
    }
}
