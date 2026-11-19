//! 방: 목록을 보여 주고, 새로 만든다.
//! 방에 들어가 대화하는 일은 ws.rs 가 한다. 여기는 방 자체만 다룬다.
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{fail, require_user, server_error, ApiResult};
use crate::db;
use crate::state::AppState;

/// 화면이 보내오는 방 만들기 요청. 예: {"name": "점심 메뉴"}
#[derive(Deserialize)]
pub struct CreateRoomRequest {
    name: String,
}

/// "/rooms" 로 get 요청이 오면 불린다. 방 목록을 돌려준다.
/// 예: [{"id": 1, "name": "모두의 방"}, {"id": 2, "name": "점심 메뉴"}]
pub async fn list(State(state): State<Arc<AppState>>, headers: HeaderMap) -> ApiResult {
    // 로그인한 사람만 볼 수 있다.
    require_user(&state, &headers).await?;

    let rooms = db::list_rooms(&state.db).await.map_err(server_error)?;
    let rooms: Vec<Value> = rooms
        .into_iter()
        .map(|(id, name)| json!({ "id": id, "name": name }))
        .collect();
    Ok(Json(json!(rooms)))
}

/// "/rooms" 로 post 요청이 오면 불린다. 방을 새로 만든다.
pub async fn create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    // 본문을 읽는 Json 은 인자 중 맨 마지막에 와야 한다. (axum 의 규칙)
    Json(request): Json<CreateRoomRequest>,
) -> ApiResult {
    let user = require_user(&state, &headers).await?;

    let name = request.name.trim();
    if name.is_empty() || name.chars().count() > 32 {
        return Err(fail(StatusCode::BAD_REQUEST, "방 이름은 1~32자여야 합니다"));
    }

    match db::create_room(&state.db, name).await.map_err(server_error)? {
        Some(id) => {
            println!("[방] {user} 이(가) {name:?} 방을 만듦 (방 번호 {id})");
            Ok(Json(json!({ "id": id, "name": name })))
        }
        // 409 는 "이미 있는 것과 부딪힌다"는 뜻의 HTTP 응답 번호다.
        None => Err(fail(StatusCode::CONFLICT, "같은 이름의 방이 이미 있습니다")),
    }
}
