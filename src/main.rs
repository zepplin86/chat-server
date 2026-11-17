//! 1단계: 서버 한 대.
//!
//! 지금 하는 일은 하나뿐이다.
//! 1. 브라우저가 접속하면 화면 한 장을 보여 준다.

use axum::response::Html;
use axum::routing::get;
use axum::Router;

#[tokio::main]
async fn main() {
    // 주소와 처리 함수를 연결한다.
    let app = Router::new()
        // "/" 로 오면 화면을 보여 준다. (화면 파일은 빌드할 때 실행 파일 안에 넣어 둔다)
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }));

    // 8080 포트를 열고 접속을 받기 시작한다.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("8080 포트를 열 수 없습니다");
    println!("http://localhost:8080 에서 접속을 기다리는 중");
    axum::serve(listener, app).await.expect("서버 실행 실패");
}
