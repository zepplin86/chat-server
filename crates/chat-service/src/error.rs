//! 채팅 서버의 오류 종류와, 그것을 gRPC 응답 / HTTP 응답으로 바꾸는 규칙.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug)]
pub enum AppError {
    /// 로그인 토큰이 없거나 틀림
    Unauthorized,
    /// 방 등 대상이 존재하지 않음
    NotFound(&'static str),
    /// 권한 없음 (예: 멤버가 아닌 그룹 방)
    Forbidden(&'static str),
    /// 요청 내용이 잘못됨 (빈 메시지, 너무 긴 메시지 등)
    Invalid(&'static str),
    /// 너무 빨리 많이 보냄 (도배 제한)
    RateLimited,
    /// 같은 임시 번호(client_msg_id)의 메시지가 지금 처리되는 중. 앱이 잠시 뒤 다시 보내면 된다.
    InProgress,
    /// DB, Redis, Kafka 등 내부 장애
    Internal(anyhow::Error),
}

/// DB 오류 등 어떤 오류든 `?` 로 넘기면 자동으로 Internal 로 바뀌게 한다.
impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(err: E) -> Self {
        AppError::Internal(err.into())
    }
}

impl AppError {
    /// 내부 장애일 때만 서버 로그에 자세한 원인을 남긴다.
    /// (사용자에게는 자세한 원인을 보여주지 않는다)
    fn log_internal(&self) {
        if let AppError::Internal(err) = self {
            tracing::error!("internal error: {err:#}");
        }
    }
}

/// gRPC 응답용 변환. 게이트웨이가 이 상태 코드를 보고 앱에 보낼 오류 코드를 정한다.
impl From<AppError> for tonic::Status {
    fn from(err: AppError) -> Self {
        err.log_internal();
        match err {
            AppError::Unauthorized => tonic::Status::unauthenticated("unauthorized"),
            AppError::NotFound(m) => tonic::Status::not_found(m),
            AppError::Forbidden(m) => tonic::Status::permission_denied(m),
            AppError::Invalid(m) => tonic::Status::invalid_argument(m),
            AppError::RateLimited => tonic::Status::resource_exhausted("too many messages"),
            AppError::InProgress => tonic::Status::aborted("message is being processed"),
            AppError::Internal(_) => tonic::Status::unavailable("temporarily unavailable"),
        }
    }
}

/// HTTP(REST) 응답용 변환. 상태 코드와 함께 {"code": ..., "message": ...} JSON 을 돌려준다.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        self.log_internal();
        let (status, code, message) = match self {
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "unauthorized"),
            AppError::NotFound(m) => (StatusCode::NOT_FOUND, "NOT_FOUND", m),
            AppError::Forbidden(m) => (StatusCode::FORBIDDEN, "FORBIDDEN", m),
            AppError::Invalid(m) => (StatusCode::BAD_REQUEST, "INVALID", m),
            AppError::RateLimited => (
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                "too many messages",
            ),
            AppError::InProgress => (
                StatusCode::CONFLICT,
                "IN_PROGRESS",
                "message is being processed",
            ),
            AppError::Internal(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "UNAVAILABLE",
                "temporarily unavailable",
            ),
        };
        (status, Json(json!({ "code": code, "message": message }))).into_response()
    }
}
