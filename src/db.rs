//! PostgreSQL 과 주고받는 일. SQL 은 전부 이 파일에만 있다.
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// DB 주소. 모양은 postgres://계정:비밀번호@주소:포트/DB이름 이다.
/// docker-compose.yml 에 적은 값과 같아야 한다.
/// DATABASE_URL 환경 변수가 있으면 그 값을 대신 쓴다.
const DEFAULT_DATABASE_URL: &str = "postgres://chat:chat@localhost:5432/chat";

/// DB 에 접속하고, 필요한 표가 없으면 만든다.
pub async fn connect() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_string());

    // 연결을 최대 5개까지 맺어 두고 돌려 쓴다.
    let db = PgPoolOptions::new().max_connections(5).connect(&url).await?;

    // 표(table)는 엑셀 시트 같은 것이다. 메시지 하나가 한 줄이 된다.
    // id 는 DB 가 1, 2, 3... 으로 알아서 매기는 번호다. 들어온 순서를 알 수 있다.
    // IF NOT EXISTS 덕분에 이미 있으면 아무 일도 하지 않는다.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS messages (
            id         BIGSERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            body       TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(&db)
    .await?;

    // 계정. 이름은 겹칠 수 없다(UNIQUE).
    // 비밀번호는 그대로 두지 않고 해시(되돌릴 수 없게 바꾼 글자)로 저장한다.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
            id            BIGSERIAL PRIMARY KEY,
            name          TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(&db)
    .await?;

    // 입장권. 로그인에 성공하면 한 장 만들어 준다.
    // token 은 아무도 맞힐 수 없는 긴 무작위 글자이고, user_id 는 그 입장권의 주인이다.
    // REFERENCES 는 "users 표에 실제로 있는 id 만 넣을 수 있다"는 뜻이다.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS sessions (
            token      TEXT PRIMARY KEY,
            user_id    BIGINT NOT NULL REFERENCES users (id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(&db)
    .await?;

    // 방. 이름은 겹칠 수 없다.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rooms (
            id         BIGSERIAL PRIMARY KEY,
            name       TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(&db)
    .await?;

    // 방이 하나도 없으면 첫 방을 만들어 둔다. 처음 만든 방이라 번호는 1 이다.
    sqlx::query("INSERT INTO rooms (name) SELECT '모두의 방' WHERE NOT EXISTS (SELECT 1 FROM rooms)")
        .execute(&db)
        .await?;

    // 메시지가 어느 방의 것인지 적을 칸(room_id)을 messages 표에 덧붙인다.
    // messages 표는 방이 없던 때에 이미 만들어졌다. CREATE TABLE IF NOT EXISTS 는
    // 표가 있으면 아무 일도 하지 않으므로, 있는 표를 고치려면 ALTER TABLE 을 써야 한다.
    // DEFAULT 1 덕분에 방이 없던 때의 메시지는 모두 1번 방('모두의 방')의 것이 된다.
    sqlx::query(
        "ALTER TABLE messages
         ADD COLUMN IF NOT EXISTS room_id BIGINT NOT NULL DEFAULT 1 REFERENCES rooms (id)",
    )
    .execute(&db)
    .await?;

    Ok(db)
}

/// 방 목록을 만든 순서대로 (방 번호, 이름) 으로 돌려준다.
pub async fn list_rooms(db: &PgPool) -> Result<Vec<(i64, String)>, sqlx::Error> {
    sqlx::query_as("SELECT id, name FROM rooms ORDER BY id")
        .fetch_all(db)
        .await
}

/// 방을 새로 만들고 방 번호를 돌려준다. 같은 이름의 방이 이미 있으면 None.
pub async fn create_room(db: &PgPool, name: &str) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO rooms (name) VALUES ($1)
         ON CONFLICT (name) DO NOTHING
         RETURNING id",
    )
    .bind(name)
    .fetch_optional(db)
    .await
}

/// 방 번호로 방 이름을 찾는다. 그런 방이 없으면 None.
pub async fn find_room_name(db: &PgPool, room_id: i64) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT name FROM rooms WHERE id = $1")
        .bind(room_id)
        .fetch_optional(db)
        .await
}

/// 이름으로 계정을 찾아 (계정 번호, 비밀번호 해시) 를 돌려준다. 없으면 None.
pub async fn find_user(db: &PgPool, name: &str) -> Result<Option<(i64, String)>, sqlx::Error> {
    sqlx::query_as("SELECT id, password_hash FROM users WHERE name = $1")
        .bind(name)
        .fetch_optional(db)
        .await
}

/// 계정을 새로 만들고 계정 번호를 돌려준다.
/// 그 사이에 누가 같은 이름으로 먼저 만들었으면 아무것도 넣지 않고 None 을 돌려준다.
pub async fn create_user(
    db: &PgPool,
    name: &str,
    password_hash: &str,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO users (name, password_hash) VALUES ($1, $2)
         ON CONFLICT (name) DO NOTHING
         RETURNING id",
    )
    .bind(name)
    .bind(password_hash)
    .fetch_optional(db)
    .await
}

/// 입장권을 한 장 만들어 돌려준다.
pub async fn create_session(db: &PgPool, user_id: i64) -> Result<String, sqlx::Error> {
    // gen_random_uuid() 는 DB 가 만들어 주는 무작위 값이다.
    // 예: 3f2b8c1e-9a4d-4e7b-b1c2-5d6e7f8a9b0c
    sqlx::query_scalar(
        "INSERT INTO sessions (token, user_id) VALUES (gen_random_uuid()::text, $1)
         RETURNING token",
    )
    .bind(user_id)
    .fetch_one(db)
    .await
}

/// 입장권의 주인 이름을 돌려준다. 그런 입장권이 없으면 None.
pub async fn find_session_name(db: &PgPool, token: &str) -> Result<Option<String>, sqlx::Error> {
    // JOIN 은 두 표를 이어 붙여 읽는 것이다. 입장권의 user_id 와 같은 id 를 가진 계정을 찾는다.
    sqlx::query_scalar(
        "SELECT users.name FROM sessions
         JOIN users ON users.id = sessions.user_id
         WHERE sessions.token = $1",
    )
    .bind(token)
    .fetch_optional(db)
    .await
}

/// 메시지 하나를 저장하고, DB 가 매긴 순번(id)을 돌려준다.
pub async fn save_message(
    db: &PgPool,
    room_id: i64,
    name: &str,
    body: &str,
) -> Result<i64, sqlx::Error> {
    // $1, $2, $3 자리에 뒤의 bind 값이 순서대로 들어간다.
    // 글자를 직접 이어 붙이지 않는 이유: 사용자가 입력한 글자가 SQL 명령으로 실행되는 것을 막는다.
    // RETURNING id 는 "방금 넣은 줄의 id 를 알려 달라"는 뜻이다.
    sqlx::query_scalar("INSERT INTO messages (room_id, name, body) VALUES ($1, $2, $3) RETURNING id")
        .bind(room_id)
        .bind(name)
        .bind(body)
        .fetch_one(db)
        .await
}

/// 그 방의 지금까지의 메시지를 순번이 작은 것부터 (순번, 이름, 내용) 으로 돌려준다.
pub async fn load_messages(
    db: &PgPool,
    room_id: i64,
) -> Result<Vec<(i64, String, String)>, sqlx::Error> {
    sqlx::query_as("SELECT id, name, body FROM messages WHERE room_id = $1 ORDER BY id")
        .bind(room_id)
        .fetch_all(db)
        .await
}
