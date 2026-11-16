//! 이 게이트웨이에 붙은 연결 중 누가 어느 방에 있는지 관리하고,
//! 실시간 전달 통로(NATS)에서 받은 메시지를 그 연결들에게 뿌린다.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::room_subject;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// 이미 JSON 으로 만들어진 프레임 하나.
/// Arc 로 감싸서, 같은 메시지를 받는 모든 연결이 복사 없이 하나를 함께 쓴다.
pub type Frame = Arc<str>;

/// 연결 하나를 가리키는 손잡이
#[derive(Clone)]
pub struct Conn {
    /// 이 연결로 내보낼 프레임을 넣는 대기열 (크기 제한 있음).
    /// 연결의 쓰기 담당 작업이 여기서 꺼내 소켓에 쓴다.
    pub tx: mpsc::Sender<Frame>,
    /// 이것을 취소하면 연결이 강제로 닫힌다.
    pub kill: CancellationToken,
}

/// 방 하나의 상태 (이 게이트웨이 기준)
struct Room {
    /// 라이브 방인지 여부. 느린 사용자를 어떻게 처리할지가 달라진다.
    live: bool,
    /// 이 방에 들어와 있는 연결들 (연결 번호 → 손잡이)
    conns: HashMap<u64, Conn>,
    /// 이 방의 NATS 주제를 읽는 작업. 방에 아무도 없게 되면 중단한다.
    subscription: JoinHandle<()>,
}

pub struct Hub {
    nats: async_nats::Client,
    /// 방 번호 → 방 상태. 이 게이트웨이에 사람이 있는 방만 들어 있다.
    rooms: Mutex<HashMap<i64, Room>>,
    /// 현재 연결 수
    pub connections: AtomicUsize,
    /// 라이브 방에서 느린 사용자 때문에 버린 프레임 수
    pub dropped: AtomicU64,
    /// 그룹 방에서 느린 사용자라서 끊은 연결 수
    pub kicked: AtomicU64,
}

impl Hub {
    pub fn new(nats: async_nats::Client) -> Arc<Self> {
        Arc::new(Self {
            nats,
            rooms: Mutex::new(HashMap::new()),
            connections: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            kicked: AtomicU64::new(0),
        })
    }

    /// 이 게이트웨이에 사람이 있는 방의 수
    pub fn room_count(&self) -> usize {
        self.rooms.lock().unwrap().len()
    }

    /// 연결을 방에 넣는다.
    /// 이 게이트웨이에서 그 방의 첫 사람이면 NATS 구독을 시작한다.
    /// 구독이 완료된 뒤에 함수가 끝나므로, 그 뒤에 발행된 메시지는 놓치지 않는다.
    pub async fn join(
        self: &Arc<Self>,
        room_id: i64,
        live: bool,
        conn_id: u64,
        conn: Conn,
    ) -> anyhow::Result<()> {
        // 빠른 길: 이미 누군가 있는 방이면 목록에 추가만 하고 끝낸다.
        {
            let mut rooms = self.rooms.lock().unwrap();
            if let Some(room) = rooms.get_mut(&room_id) {
                room.conns.insert(conn_id, conn);
                return Ok(());
            }
        }

        // 이 게이트웨이에서 첫 사람이다. 잠금을 쥐지 않은 상태로 NATS 구독을 건다.
        // (네트워크 작업 중에 잠금을 쥐고 있으면 다른 연결이 전부 멈추기 때문)
        let mut subscriber = self.nats.subscribe(room_subject(room_id)).await?;
        // 구독 요청이 NATS 서버에 실제로 도착했는지 확인한다.
        self.nats.flush().await?;

        let mut rooms = self.rooms.lock().unwrap();
        // 구독하는 사이에 다른 연결이 먼저 방을 만들었을 수 있다.
        if let Some(room) = rooms.get_mut(&room_id) {
            // 그 방에 추가만 한다. 방금 만든 구독은 여기서 버려지며 자동으로 해제된다.
            room.conns.insert(conn_id, conn);
            return Ok(());
        }
        // NATS 에서 이 방의 메시지를 계속 읽어 연결들에게 뿌리는 작업을 띄운다.
        let hub = self.clone();
        let subscription = tokio::spawn(async move {
            while let Some(message) = subscriber.next().await {
                // 받은 바이트를 문자열로 확인한 뒤 그대로 뿌린다 (다시 JSON 으로 만들지 않는다).
                match std::str::from_utf8(&message.payload) {
                    Ok(text) => hub.fan_out(room_id, Arc::from(text)),
                    Err(_) => tracing::warn!(room_id, "dropping non-utf8 payload"),
                }
            }
        });
        // 방을 새로 등록한다.
        rooms.insert(
            room_id,
            Room {
                live,
                conns: HashMap::from([(conn_id, conn)]),
                subscription,
            },
        );
        Ok(())
    }

    /// 연결을 방에서 뺀다. 방에 아무도 남지 않으면 NATS 구독도 끝낸다.
    pub fn leave(&self, room_id: i64, conn_id: u64) {
        let mut rooms = self.rooms.lock().unwrap();
        if let Some(room) = rooms.get_mut(&room_id) {
            room.conns.remove(&conn_id);
            if room.conns.is_empty() {
                // 마지막 사람이 나갔다: 방을 지우고 읽기 작업을 중단한다.
                if let Some(room) = rooms.remove(&room_id) {
                    room.subscription.abort();
                }
            }
        }
    }

    /// 프레임 하나를 방 안의 모든 연결에게 전달한다. 절대 기다리지 않는다.
    ///
    /// 대기열이 꽉 찼다는 것은 그 사용자가 메시지를 못 받아 가고 있다는 뜻이다.
    /// - 라이브 방: 그 사용자에게 가는 이 프레임만 버린다.
    /// - 그룹 방: 조용히 빠뜨리면 안 되므로 연결을 끊는다. 앱이 다시 접속해서
    ///   순번 동기화로 빠진 메시지를 받아 간다.
    fn fan_out(&self, room_id: i64, frame: Frame) {
        let rooms = self.rooms.lock().unwrap();
        let Some(room) = rooms.get(&room_id) else {
            return;
        };
        for conn in room.conns.values() {
            // try_send 는 대기열에 자리가 없으면 기다리지 않고 바로 실패한다.
            // frame.clone() 은 내용 복사가 아니라 참조 횟수만 늘린다.
            match conn.tx.try_send(frame.clone()) {
                // 성공했거나, 이미 닫힌 연결이면 할 일이 없다.
                Ok(()) | Err(TrySendError::Closed(_)) => {}
                // 라이브 방에서 꽉 참: 버리고 개수만 센다.
                Err(TrySendError::Full(_)) if room.live => {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
                // 그룹 방에서 꽉 참: 연결을 끊는다 (이미 끊는 중이면 중복으로 세지 않는다).
                Err(TrySendError::Full(_)) => {
                    if !conn.kill.is_cancelled() {
                        self.kicked.fetch_add(1, Ordering::Relaxed);
                        conn.kill.cancel();
                    }
                }
            }
        }
    }
}
