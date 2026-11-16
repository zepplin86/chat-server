//! 앱이 직접 부르는 REST API: 로그인과 방 관리.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::header::AUTHORIZATION;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use common::auth::{self, Claims};
use common::model::RoomType;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::AppError;
use crate::service::Service;

type Shared = Arc<Service>;

/// 주소와 처리 함수를 연결한다.
pub fn router(service: Shared) -> Router {
    Router::new()
        // 살아 있는지 확인용
        .route("/healthz", get(|| async { "ok" }))
        // 로그인
        .route("/api/login", post(login))
        // 방 목록 조회(GET) / 방 만들기(POST)
        .route("/api/rooms", get(list_rooms).post(create_room))
        // 그룹 방 멤버로 가입
        .route("/api/rooms/:room_id/join", post(join_room))
        .with_state(service)
}

/// 요청 헤더의 `Authorization: Bearer <토큰>` 을 확인해 누구의 요청인지 알아낸다.
fn authenticate(service: &Service, headers: &HeaderMap) -> Result<Claims, AppError> {
    let token = headers
        // Authorization 헤더를 꺼내고
        .get(AUTHORIZATION)
        // 문자열로 바꾼 뒤
        .and_then(|v| v.to_str().ok())
        // 앞의 "Bearer " 를 떼어낸다.
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(AppError::Unauthorized)?;
    // 토큰의 서명과 만료를 검증한다.
    auth::verify(&service.jwt_secret, token).map_err(|_| AppError::Unauthorized)
}

#[derive(Deserialize)]
struct LoginBody {
    name: String,
}

/// 개발용 로그인: 이름만 있으면 된다.
/// 외부에 공개하기 전에 반드시 실제 인증으로 바꿔야 한다.
async fn login(
    State(service): State<Shared>,
    Json(body): Json<LoginBody>,
) -> Result<Json<Value>, AppError> {
    // 이름 검사: 공백을 떼고 1~32자인지 확인한다.
    let name = body.name.trim();
    if name.is_empty() || name.chars().count() > 32 {
        return Err(AppError::Invalid("name must be 1-32 characters"));
    }
    // 처음 보는 이름이면 사용자를 새로 만들고, 이미 있으면 그 사용자의 번호를 가져온다.
    let (user_id,): (i64,) = sqlx::query_as(
        "INSERT INTO users (name) VALUES ($1) \
         ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
    )
    .bind(name)
    .fetch_one(&service.pg)
    .await?;
    // 토큰을 발급해 돌려준다.
    let token = auth::issue(&service.jwt_secret, user_id, name)?;
    Ok(Json(
        json!({ "token": token, "user_id": user_id, "name": name }),
    ))
}

/// 방 목록. 각 방에 대해 "내가 멤버인지"도 함께 알려준다.
async fn list_rooms(
    State(service): State<Shared>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    // 누구의 요청인지 확인.
    let claims = authenticate(&service, &headers)?;
    // 최근에 만든 방부터 200개까지 가져온다.
    let rows: Vec<(i64, String, String, bool)> = sqlx::query_as(
        "SELECT r.id, r.name, r.kind, \
                EXISTS(SELECT 1 FROM room_members m WHERE m.room_id = r.id AND m.user_id = $1) \
         FROM rooms r ORDER BY r.id DESC LIMIT 200",
    )
    .bind(claims.sub)
    .fetch_all(&service.pg)
    .await?;
    // DB 의 행을 JSON 으로 바꾼다.
    let rooms: Vec<Value> = rows
        .into_iter()
        .map(|(id, name, kind, member)| {
            json!({ "id": id, "name": name, "type": kind, "member": member })
        })
        .collect();
    Ok(Json(json!({ "rooms": rooms })))
}

#[derive(Deserialize)]
struct CreateRoomBody {
    name: String,
    #[serde(rename = "type")]
    kind: String,
}

/// 방 만들기. 만든 사람은 자동으로 그 방의 멤버(방장)가 된다.
async fn create_room(
    State(service): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<CreateRoomBody>,
) -> Result<Json<Value>, AppError> {
    let claims = authenticate(&service, &headers)?;
    // 방 이름 검사: 1~64자.
    let name = body.name.trim();
    if name.is_empty() || name.chars().count() > 64 {
        return Err(AppError::Invalid("name must be 1-64 characters"));
    }
    // 방 종류 검사: group 또는 live 만 허용.
    let room_type =
        RoomType::parse(&body.kind).ok_or(AppError::Invalid("type must be group or live"))?;

    // 방 생성과 방장 등록을 하나의 트랜잭션으로 묶는다.
    // 둘 중 하나만 성공하는 일이 없게 하기 위함.
    let mut tx = service.pg.begin().await?;
    // 방을 만들고 새 방 번호를 받는다.
    let (room_id,): (i64,) =
        sqlx::query_as("INSERT INTO rooms (name, kind, owner_id) VALUES ($1, $2, $3) RETURNING id")
            .bind(name)
            .bind(room_type.as_str())
            .bind(claims.sub)
            .fetch_one(&mut *tx)
            .await?;
    // 만든 사람을 방장으로 등록한다.
    sqlx::query("INSERT INTO room_members (room_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(room_id)
        .bind(claims.sub)
        .execute(&mut *tx)
        .await?;
    // 두 작업을 함께 확정한다.
    tx.commit().await?;

    Ok(Json(json!({
        "id": room_id, "name": name, "type": room_type.as_str(), "member": true
    })))
}

/// 그룹 방 멤버로 가입한다. 이미 멤버면 아무 일도 하지 않는다.
async fn join_room(
    State(service): State<Shared>,
    headers: HeaderMap,
    Path(room_id): Path<i64>,
) -> Result<Json<Value>, AppError> {
    let claims = authenticate(&service, &headers)?;
    // 방이 실제로 있는지 확인한다.
    let exists: Option<(i64,)> = sqlx::query_as("SELECT id FROM rooms WHERE id = $1")
        .bind(room_id)
        .fetch_optional(&service.pg)
        .await?;
    if exists.is_none() {
        return Err(AppError::NotFound("room not found"));
    }
    // 멤버로 등록한다. 이미 등록돼 있으면 그냥 넘어간다.
    sqlx::query(
        "INSERT INTO room_members (room_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(room_id)
    .bind(claims.sub)
    .execute(&service.pg)
    .await?;
    Ok(Json(json!({ "ok": true })))
}
