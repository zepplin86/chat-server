//! 서비스들이 주고받는 데이터의 모양.

use serde::{Deserialize, Serialize};

/// 방 종류. JSON 에서는 소문자("group", "live")로 표현된다.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RoomType {
    /// 그룹 방: 멤버가 정해져 있고, 모든 메시지를 빠짐없이 전달하고 저장한다.
    Group,
    /// 라이브 방: 접속한 사람 누구나 들어올 수 있고, 최선을 다해 전달하며, 최근 메시지만 보관한다.
    Live,
}

impl RoomType {
    /// DB 나 응답에 넣을 문자열로 바꾼다.
    pub fn as_str(self) -> &'static str {
        match self {
            RoomType::Group => "group",
            RoomType::Live => "live",
        }
    }

    /// 문자열을 방 종류로 바꾼다. 모르는 값이면 None.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "group" => Some(RoomType::Group),
            "live" => Some(RoomType::Live),
            _ => None,
        }
    }
}

/// 메시지 종류. JSON 에서는 소문자("text", "system")로 표현된다.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageKind {
    /// 사용자가 쓴 일반 글
    Text,
    /// 시스템 알림
    System,
}

impl MessageKind {
    /// ScyllaDB 의 tinyint 컬럼에 넣을 숫자로 바꾼다.
    pub fn to_i8(self) -> i8 {
        match self {
            MessageKind::Text => 0,
            MessageKind::System => 1,
        }
    }

    /// ScyllaDB 에서 읽은 숫자를 메시지 종류로 바꾼다. 모르는 값은 일반 글로 본다.
    pub fn from_i8(v: i8) -> Self {
        match v {
            1 => MessageKind::System,
            _ => MessageKind::Text,
        }
    }
}

/// 채팅 메시지 하나.
///
/// 이 구조체를 JSON 으로 바꾼 결과가 Kafka, NATS, Redis 최근 메시지 보관함, WebSocket 을
/// 그대로 흘러간다. 즉 채팅 서버가 딱 한 번만 JSON 으로 바꾸고, 그 뒤로는 아무도 다시 바꾸지 않는다.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// 프레임 종류 표시. 항상 "message".
    pub t: String,
    /// 어느 방의 메시지인지
    pub room_id: i64,
    /// 방 종류. 저장 담당/푸시 담당이 DB 를 다시 조회하지 않아도 되도록 같이 실어 보낸다.
    pub room_type: RoomType,
    /// 방 안에서 1씩 늘어나는 순번. 순서를 정하고, 앱이 빠진 메시지를 알아채는 기준이다.
    pub seq: i64,
    /// 전체 서비스에서 겹치지 않는 고유 번호.
    /// JavaScript 가 안전하게 다룰 수 있는 정수 범위를 넘기 때문에 문자열로 보낸다.
    #[serde(with = "id_string")]
    pub message_id: i64,
    /// 보낸 사람 번호
    pub sender_id: i64,
    /// 보낸 사람 이름
    pub sender_name: String,
    /// 메시지 종류. JSON 필드 이름은 "type".
    #[serde(rename = "type")]
    pub kind: MessageKind,
    /// 메시지 내용
    pub body: String,
    /// 서버가 메시지를 받은 시각 (1970년부터 흐른 밀리초). 사용자 기기의 시계는 쓰지 않는다.
    pub created_at: i64,
}

impl ChatMessage {
    /// `t` 필드에 넣는 값
    pub const TAG: &'static str = "message";
}

/// i64 를 JSON 문자열로 쓰고, JSON 문자열에서 i64 로 읽기 위한 변환기.
mod id_string {
    use serde::{Deserialize, Deserializer, Serializer};

    /// 숫자 → "숫자" 문자열
    pub fn serialize<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }

    /// "숫자" 문자열 → 숫자. 숫자가 아니면 오류.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}
