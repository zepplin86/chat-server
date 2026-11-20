//! 채팅 서버: 로직 담당. 로그인, 방, 저장을 맡는다. 브라우저 연결(WebSocket)은 붙들지 않는다.
//!
//! 5단계에서 서버 한 대가 하던 일을 둘로 나눴다. 이쪽은 "무엇이 옳은지 정하는 일"이다.
//! 누가 누구인지, 그런 방이 있는지, 메시지에 몇 번을 매길지. 그래서 DB 를 아는 것은 이 프로그램뿐이다.
//!
//! 문을 두 개 연다.
//! - HTTP (8090): 브라우저가 로드밸런서를 거쳐 직접 부른다. 로그인, 방 목록, 방 만들기, 이전 대화 더 보기.
//! - gRPC (50051): 게이트웨이가 부른다. 입장, 말하기.
//!
//! 이 파일은 조립만 한다. 실제 일은 아래 파일에 있다.
//! - auth.rs: 로그인 (비밀번호 확인, 입장권 발급)
//! - db.rs: PostgreSQL 과 주고받는 일 (접속, 저장, 읽기)
//! - grpc.rs: 게이트웨이가 부르는 일 (입장, 말하기)
//! - rooms.rs: 방 목록, 방 만들기, 이전 대화 더 보기
//! - state.rs: 모든 요청이 함께 쓰는 것들 (DB 연결, NATS 연결)

// mod 는 "같은 폴더의 auth.rs, db.rs, ... 를 이 프로그램에 포함한다"는 선언이다.
mod auth;
mod db;
mod grpc;
mod rooms;
mod state;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

// chat_server 는 이 프로젝트의 src/lib.rs 다. 게이트웨이와 함께 쓰는 것들이 들어 있다.
use chat_server::bus;
use chat_server::proto::chat_server::ChatServer;

use state::AppState;

#[tokio::main]
async fn main() {
    // 문 두 개의 포트. 밖에서 정해 주지 않으면 HTTP 는 8090, gRPC 는 50051 을 쓴다.
    let http_port = std::env::var("PORT").unwrap_or_else(|_| "8090".to_string());
    let grpc_port = std::env::var("GRPC_PORT").unwrap_or_else(|_| "50051".to_string());

    // PostgreSQL 에 접속한다. DB 가 꺼져 있으면 시작하지 않는다.
    let db = db::connect()
        .await
        .expect("DB 에 접속할 수 없습니다. docker compose up -d 로 DB 를 먼저 켜세요");

    // NATS 에 접속한다. 저장한 메시지를 게이트웨이들에게 알리는 길이다. 이것도 꺼져 있으면 시작하지 않는다.
    let nats = bus::connect()
        .await
        .expect("NATS 에 접속할 수 없습니다. docker compose up -d 로 NATS 를 먼저 켜세요");

    // DB 연결과 NATS 연결의 묶음. HTTP 쪽과 gRPC 쪽이 같은 묶음을 함께 쓴다.
    let state = Arc::new(AppState { db, nats });

    // HTTP 문: 주소와 처리 함수를 연결한다.
    let app = Router::new()
        // "/login" 으로 이름과 비밀번호를 보내오면 확인하고 입장권을 준다.
        .route("/login", post(auth::login))
        // "/rooms" 는 같은 주소에 두 가지 일을 건다. get 이면 방 목록, post 면 방 만들기.
        .route("/rooms", get(rooms::list).post(rooms::create))
        // 방 하나의 지난 대화. :id 자리에는 방 번호가 온다. 예: /rooms/1/messages
        .route("/rooms/:id/messages", get(rooms::older_messages))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{http_port}"))
        .await
        .unwrap_or_else(|error| panic!("{http_port} 포트를 열 수 없습니다: {error}"));
    // HTTP 문은 뒤에서 따로 돌려 둔다. spawn 은 "이 일을 따로 돌려 두고 나는 다음 줄로 간다"는 뜻이다.
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("HTTP 실행 실패");
    });

    // gRPC 문: 게이트웨이가 부르는 입장(Join)과 말하기(Say)를 받는다.
    let grpc_address = format!("0.0.0.0:{grpc_port}")
        .parse()
        .unwrap_or_else(|error| panic!("gRPC 포트 {grpc_port} 이(가) 올바르지 않습니다: {error}"));
    println!("채팅 서버: HTTP {http_port}, gRPC {grpc_port} 포트에서 기다리는 중");
    tonic::transport::Server::builder()
        .add_service(ChatServer::new(grpc::ChatService { state }))
        .serve(grpc_address)
        .await
        .unwrap_or_else(|error| panic!("gRPC 포트 {grpc_port} 을(를) 열 수 없습니다: {error}"));
}
