//! 방: 목록을 보여 주고, 새로 만들고, 지난 대화를 한 묶음씩 내준다.
//! 방에 들어가 대화하는 일은 게이트웨이가 한다. 여기는 방 자체만 다룬다.
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::auth::{fail, require_user, server_error, ApiResult};
use chat_server::db;
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

/// 지난 대화 요청의 주소 뒤에 붙는 값: /rooms/1/messages?before=151
#[derive(Deserialize)]
pub struct OlderQuery {
    /// 화면이 가진 가장 오래된 메시지의 순번. 이것보다 앞의 것을 달라는 뜻이다.
    before: i64,
}

/// "/rooms/방번호/messages?before=순번" 으로 get 요청이 오면 불린다.
/// 그 방에서 순번이 before 보다 작은 메시지 한 묶음을 순번이 작은 것부터 돌려준다.
/// 더 없으면 빈 목록([])을 돌려준다.
///
/// 지난 대화는 WebSocket 이 아니라 보통의 요청으로 받는다.
/// "달라고 하면 준다"는 한 번 묻고 답하는 일이라, 열어 두는 통로가 필요 없기 때문이다.
pub async fn older_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    // 주소 중간의 방 번호를 꺼낸다. 예: /rooms/1/messages 의 1
    Path(room_id): Path<i64>,
    Query(query): Query<OlderQuery>,
) -> ApiResult {
    require_user(&state, &headers).await?;

    // 메시지는 ScyllaDB 에 있다.
    let rows = state
        .store
        .load_messages_before(room_id, query.before)
        .await
        .map_err(server_error)?;
    // WebSocket 으로 보내는 메시지와 같은 모양으로 만든다. (src/lib.rs 의 to_line)
    let messages: Vec<Value> = rows
        .into_iter()
        .map(|(seq, name, body)| json!({ "seq": seq, "name": name, "body": body }))
        .collect();
    Ok(Json(json!(messages)))
}
