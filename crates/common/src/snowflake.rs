//! 메시지 고유 번호 생성기 (Snowflake 방식).

use std::sync::Mutex;

/// 번호에 넣는 시각의 기준점: 2026-01-01T00:00:00Z
const EPOCH_MS: i64 = 1_767_225_600_000;
/// 서버 번호에 쓰는 비트 수 (서버 1024대까지 구분)
const NODE_BITS: u32 = 10;
/// 같은 밀리초 안에서의 일련번호에 쓰는 비트 수 (1밀리초에 4096개까지)
const SEQ_BITS: u32 = 12;
/// 일련번호가 12비트를 넘지 않게 자르는 마스크
const SEQ_MASK: i64 = (1 << SEQ_BITS) - 1;

/// 64비트 번호 = 시각(밀리초) 41비트 + 서버 번호 10비트 + 일련번호 12비트.
/// 실행 중인 채팅 서버마다 서버 번호(NODE_ID)가 다르면 절대 겹치지 않는다.
/// 시각이 앞쪽에 있어서 번호를 크기순으로 정렬하면 대략 시간순이 된다.
pub struct Snowflake {
    /// 이 서버의 번호
    node: i64,
    /// (마지막으로 번호를 만든 밀리초, 그 밀리초에서 쓴 일련번호)
    state: Mutex<(i64, i64)>,
}

impl Snowflake {
    pub fn new(node: i64) -> Self {
        Self {
            // 서버 번호가 10비트를 넘지 않게 자른다.
            node: node & ((1 << NODE_BITS) - 1),
            state: Mutex::new((0, 0)),
        }
    }

    /// 새 번호를 하나 만든다.
    pub fn next(&self) -> i64 {
        // 여러 요청이 동시에 불러도 안전하도록 잠근다.
        let mut state = self.state.lock().unwrap();
        let (last_ms, counter) = *state;
        // 시계가 뒤로 가더라도 번호가 작아지지 않게, 마지막 시각보다 과거면 마지막 시각을 쓴다.
        let mut now = crate::now_ms().max(last_ms);
        let mut next_counter = 0;
        // 직전 번호와 같은 밀리초라면 일련번호를 1 올린다.
        if now == last_ms {
            next_counter = (counter + 1) & SEQ_MASK;
            if next_counter == 0 {
                // 이 밀리초의 일련번호 4096개를 다 썼다: 다음 밀리초를 미리 당겨 쓴다.
                now += 1;
            }
        }
        // 이번에 쓴 시각과 일련번호를 기억해 둔다.
        *state = (now, next_counter);
        // 세 조각을 비트 단위로 이어 붙여 하나의 숫자로 만든다.
        ((now - EPOCH_MS) << (NODE_BITS + SEQ_BITS)) | (self.node << SEQ_BITS) | next_counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2만 개를 연속으로 만들어도 항상 직전보다 큰지(겹치지 않고 늘어나는지) 확인한다.
    #[test]
    fn ids_are_unique_and_increasing() {
        let gen = Snowflake::new(1);
        let mut prev = 0;
        for _ in 0..20_000 {
            let id = gen.next();
            assert!(id > prev);
            prev = id;
        }
    }
}
