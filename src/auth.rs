//! 로그인: 이름과 비밀번호를 확인하고 입장권(token)을 준다.
use std::sync::Arc;

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db;
use crate::state::AppState;

/// 화면이 보내오는 로그인 요청. 예: {"name": "철수", "password": "1234"}
#[derive(Deserialize)]
pub struct LoginRequest {
    name: String,
    password: String,
}

/// 로그인 결과. 성공하면 Ok, 실패하면 Err 이고 둘 다 (응답 번호, JSON) 이다.
type LoginResult = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// 실패 응답을 만든다. 예: 401 {"error": "비밀번호가 틀렸습니다"}
fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

/// DB 나 해시 계산이 실패했을 때. 자세한 내용은 로그에만 남기고 화면에는 알리지 않는다.
fn server_error(error: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    println!("[로그인] 서버 오류: {error}");
    fail(StatusCode::INTERNAL_SERVER_ERROR, "서버 오류")
}

/// "/login" 으로 요청이 오면 불린다.
/// 처음 보는 이름이면 그 비밀번호로 계정을 만들고, 있는 이름이면 비밀번호를 확인한다.
/// 어느 쪽이든 성공하면 입장권을 준다.
pub async fn login(
    State(state): State<Arc<AppState>>,
    // 요청 본문의 JSON 을 LoginRequest 로 바꿔 받는다.
    Json(request): Json<LoginRequest>,
) -> LoginResult {
    let name = request.name.trim();
    // chars().count() 는 글자 수다. len() 은 바이트 수라서 한글은 한 글자가 3으로 세어진다.
    if name.is_empty() || name.chars().count() > 32 {
        return Err(fail(StatusCode::BAD_REQUEST, "이름은 1~32자여야 합니다"));
    }
    if request.password.chars().count() < 4 {
        return Err(fail(StatusCode::BAD_REQUEST, "비밀번호는 4자 이상이어야 합니다"));
    }

    let user_id = match db::find_user(&state.db, name).await.map_err(server_error)? {
        // 있는 이름이다: 비밀번호가 맞는지 확인한다.
        Some((user_id, password_hash)) => {
            if !verify_password(&request.password, &password_hash) {
                println!("[로그인] {name}: 비밀번호 틀림");
                return Err(fail(StatusCode::UNAUTHORIZED, "비밀번호가 틀렸습니다"));
            }
            println!("[로그인] {name}: 성공");
            user_id
        }
        // 처음 보는 이름이다: 계정을 만든다.
        None => {
            let password_hash = hash_password(&request.password).map_err(server_error)?;
            match db::create_user(&state.db, name, &password_hash)
                .await
                .map_err(server_error)?
            {
                Some(user_id) => {
                    println!("[로그인] {name}: 새 계정을 만듦");
                    user_id
                }
                // 찾아봤을 땐 없었는데, 만드는 사이에 누가 같은 이름으로 먼저 만들었다.
                None => {
                    return Err(fail(
                        StatusCode::CONFLICT,
                        "방금 다른 사람이 그 이름을 썼습니다. 다시 시도해 주세요",
                    ))
                }
            }
        }
    };

    // 입장권을 만들어 돌려준다. 화면은 이것을 들고 WebSocket 으로 접속한다.
    let token = db::create_session(&state.db, user_id)
        .await
        .map_err(server_error)?;
    Ok(Json(json!({ "token": token, "name": name })))
}

/// 비밀번호를 해시로 바꾼다.
///
/// 해시는 한쪽으로만 가는 변환이다. 비밀번호에서 해시는 만들 수 있지만,
/// 해시에서 비밀번호를 되찾을 수는 없다. DB 가 통째로 새어 나가도 비밀번호는 드러나지 않는다.
///
/// 소금(salt)은 계정마다 다르게 섞는 무작위 값이다.
/// 두 사람이 같은 비밀번호를 써도 해시가 달라지게 한다. 소금은 해시 글자 안에 함께 들어간다.
fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default().hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

/// 입력한 비밀번호가 저장된 해시와 맞는지 확인한다.
/// 입력한 비밀번호를 같은 소금으로 다시 해시해서 결과가 같은지 본다.
fn verify_password(password: &str, password_hash: &str) -> bool {
    match PasswordHash::new(password_hash) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        // 저장된 해시가 깨져 있다. 맞다고 할 수 없다.
        Err(_) => false,
    }
}
