//! 2단계: 서버 한 대, 방 여러 개, 대화는 PostgreSQL 에 저장.
//!
//! 지금 하는 일은 다섯 가지다.
//! 1. 브라우저가 접속하면 화면 한 장을 보여 준다.
//! 2. 이름과 비밀번호로 로그인하면 입장권(token)을 준다.
//! 3. 방 목록을 보여 주고, 새 방을 만든다.
//! 4. 화면이 입장권과 방 번호를 들고 WebSocket 으로 연결해 오면, 그 방의 최근 대화를 DB 에서 읽어 먼저 보내 준다.
//! 5. 누가 보낸 말이든 DB 에 저장하고, 보낸 사람 이름을 붙여 같은 방에 있는 모든 사람에게 뿌린다.
//!
//! 이 파일은 조립만 한다. 실제 일은 아래 다섯 파일에 있다.
//! - auth.rs: 로그인 (비밀번호 확인, 입장권 발급)
//! - db.rs: PostgreSQL 과 주고받는 일 (접속, 저장, 읽기)
//! - rooms.rs: 방 목록, 방 만들기
//! - state.rs: 모든 연결이 함께 쓰는 것들 (DB 연결, 방송 채널)
//! - ws.rs: WebSocket 연결 하나를 맺고 끝까지 담당하는 코드

// mod 는 "같은 폴더의 auth.rs, db.rs, rooms.rs, state.rs, ws.rs 를 이 프로그램에 포함한다"는 선언이다.
// 이 줄이 없으면 파일이 있어도 컴파일되지 않는다.
mod auth;
mod db;
mod rooms;
mod state;
mod ws;

// use 는 이미 있는 것을 짧은 이름으로 부르기 위한 것이다.
// 표준 라이브러리, 외부 라이브러리, 이 프로젝트 순으로 적는다.
use std::sync::Arc;

use axum::response::Html;
use axum::routing::{get, post};
use axum::Router;

use state::AppState;

#[tokio::main]
async fn main() {
    // PostgreSQL 에 접속한다. DB 가 꺼져 있으면 서버를 시작하지 않는다.
    let db = db::connect()
        .await
        .expect("DB 에 접속할 수 없습니다. docker compose up -d 로 DB 를 먼저 켜세요");

    // DB 연결과 방송 채널의 묶음을 만든다.
    // Arc 는 값 하나를 여러 곳에서 함께 가리키게 해 주는 포장지다.
    // 연결마다 이 묶음을 복제해 받지만, 복제되는 것은 가리키는 손잡이뿐이고 내용은 하나다.
    let state = Arc::new(AppState::new(db));

    // 주소와 처리 함수를 연결한다.
    let app = Router::new()
        // "/" 로 오면 화면을 보여 준다. (화면 파일은 빌드할 때 실행 파일 안에 넣어 둔다)
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }))
        // "/login" 으로 이름과 비밀번호를 보내오면 확인하고 입장권을 준다.
        // get 은 "달라"는 요청, post 는 "이것을 받아 처리해 달라"는 요청이다.
        .route("/login", post(auth::login))
        // "/rooms" 는 같은 주소에 두 가지 일을 건다. get 이면 방 목록, post 면 방 만들기.
        .route("/rooms", get(rooms::list).post(rooms::create))
        // "/ws" 로 오면 WebSocket 연결을 맺는다.
        .route("/ws", get(ws::ws_handler))
        // 모든 처리 함수가 같은 묶음(DB 연결 + 방송 채널)을 꺼내 쓸 수 있게 맡겨 둔다.
        .with_state(state);

    // 8080 포트를 열고 접속을 받기 시작한다.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("8080 포트를 열 수 없습니다");
    println!("http://localhost:8080 에서 접속을 기다리는 중");
    axum::serve(listener, app).await.expect("서버 실행 실패");
}
