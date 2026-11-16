//! 로그인 토큰(JWT) 발급과 검증.
//! 채팅 서버가 발급하고, 채팅 서버와 게이트웨이가 같은 비밀 키로 검증한다.

use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// 토큰 유효 시간: 24시간
const TOKEN_TTL_SECS: u64 = 24 * 60 * 60;

/// 토큰 안에 담기는 내용
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// 사용자 번호
    pub sub: i64,
    /// 사용자 이름
    pub name: String,
    /// 만료 시각 (1970년부터 흐른 초)
    pub exp: u64,
}

/// 사용자 번호와 이름으로 토큰을 만든다.
pub fn issue(secret: &str, user_id: i64, name: &str) -> anyhow::Result<String> {
    // 지금 시각에 24시간을 더해 만료 시각을 정한다.
    let claims = Claims {
        sub: user_id,
        name: name.to_string(),
        exp: (crate::now_ms() / 1000) as u64 + TOKEN_TTL_SECS,
    };
    // 비밀 키로 서명해서 문자열 토큰으로 만든다 (HS256 방식).
    Ok(encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )?)
}

/// 토큰이 진짜인지(서명이 맞는지), 만료되지 않았는지 확인하고 안의 내용을 꺼낸다.
pub fn verify(secret: &str, token: &str) -> anyhow::Result<Claims> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        // 기본 검증 규칙: 서명 방식 HS256, 만료 시각 확인
        &Validation::default(),
    )?;
    Ok(data.claims)
}
