//! 채팅의 핵심 로직: 권한 검사, 번호 붙이기, 발행, 지난 메시지 조회.

use std::time::Duration;

use anyhow::anyhow;
use common::model::{ChatMessage, MessageKind, RoomType};
use common::snowflake::Snowflake;
use common::{bucket_of, keys, now_ms, room_subject, KAFKA_TOPIC};
use rdkafka::producer::{FutureProducer, FutureRecord};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;
use sqlx::PgPool;

use crate::error::AppError;

/// 메시지 한 개의 최대 글자 수
const MAX_BODY_CHARS: usize = 2000;
/// 한 번에 조회할 수 있는 최대 메시지 수
const MAX_FETCH: i32 = 200;
/// 중복 전송 방지 기록을 보관하는 시간(초). 이 시간 안의 재전송은 중복으로 걸러진다.
const DEDUP_TTL_SECS: u64 = 300;
/// "지금 처리 중"임을 표시하는 임시 값
const DEDUP_PENDING: &str = "pending";

/// 채팅 서버가 쓰는 모든 연결과 설정을 한데 모은 것. 요청마다 공유된다.
pub struct Service {
    /// PostgreSQL: 사용자, 방, 멤버
    pub pg: PgPool,
    /// Redis: 순번, 최근 메시지, 도배 카운터, 중복 방지
    pub redis: ConnectionManager,
    /// NATS: 실시간 전달 통로
    pub nats: async_nats::Client,
    /// Kafka: 기록 통로
    pub kafka: FutureProducer,
    /// ScyllaDB: 메시지 저장소
    pub scylla: Session,
    /// 지난 메시지 조회용으로 미리 준비해 둔 쿼리
    pub fetch_stmt: PreparedStatement,
    /// 메시지 고유 번호 생성기
    pub ids: Snowflake,
    /// 토큰 서명/검증용 비밀 키
    pub jwt_secret: String,
    /// 사용자 한 명이 1초에 보낼 수 있는 메시지 수
    pub rate_limit_per_sec: i64,
    /// 방마다 Redis 에 보관하는 최근 메시지 수
    pub recent_cap: isize,
}

/// 메시지 전송 결과
pub struct SendResult {
    pub message_id: i64,
    pub seq: i64,
    /// 이미 처리된 메시지의 재전송이었으면 true
    pub duplicate: bool,
}

/// 지난 메시지 조회 결과
pub struct FetchResult {
    /// 이미 JSON 으로 바뀌어 있는 메시지들 (오래된 것부터)
    pub messages: Vec<String>,
    /// 이 방의 가장 최신 순번
    pub latest_seq: i64,
    /// 아직 더 받아갈 메시지가 남았는지
    pub has_more: bool,
}

impl Service {
    /// 사용자가 이 방을 읽고 쓸 수 있는지 확인하고 방 종류를 돌려준다.
    /// 그룹 방은 멤버여야 하고, 라이브 방은 로그인한 사람 누구나 가능하다.
    pub async fn room_access(&self, user_id: i64, room_id: i64) -> Result<RoomType, AppError> {
        // 방 종류와 "이 사용자가 멤버인지"를 쿼리 한 번으로 가져온다.
        let row: Option<(String, bool)> = sqlx::query_as(
            "SELECT r.kind, \
                    EXISTS(SELECT 1 FROM room_members m WHERE m.room_id = r.id AND m.user_id = $2) \
             FROM rooms r WHERE r.id = $1",
        )
        .bind(room_id)
        .bind(user_id)
        .fetch_optional(&self.pg)
        .await?;

        // 결과가 없으면 그런 방이 없는 것.
        let (kind, is_member) = row.ok_or(AppError::NotFound("room not found"))?;
        // DB 의 문자열을 방 종류로 바꾼다.
        let room_type =
            RoomType::parse(&kind).ok_or_else(|| anyhow!("unknown room kind {kind:?}"))?;
        // 그룹 방인데 멤버가 아니면 거부한다.
        if room_type == RoomType::Group && !is_member {
            return Err(AppError::Forbidden("not a member of this room"));
        }
        Ok(room_type)
    }

    /// 방의 가장 최신 순번을 Redis 에서 읽는다. 아직 메시지가 없으면 0.
    pub async fn latest_seq(&self, room_id: i64) -> Result<i64, AppError> {
        let mut redis = self.redis.clone();
        let seq: Option<i64> = redis.get(keys::room_seq(room_id)).await?;
        Ok(seq.unwrap_or(0))
    }

    /// 메시지 하나를 받아 검사하고, 번호를 붙여 내보낸다.
    pub async fn send(
        &self,
        user_id: i64,
        user_name: &str,
        room_id: i64,
        client_msg_id: &str,
        body: &str,
    ) -> Result<SendResult, AppError> {
        // 1) 내용 검사: 앞뒤 공백을 떼고, 비어 있거나 너무 길면 거부한다.
        let body = body.trim();
        if body.is_empty() {
            return Err(AppError::Invalid("message is empty"));
        }
        if body.chars().count() > MAX_BODY_CHARS {
            return Err(AppError::Invalid("message is too long"));
        }
        // 앱이 붙인 임시 번호도 형식을 확인한다.
        if client_msg_id.is_empty() || client_msg_id.len() > 64 {
            return Err(AppError::Invalid("client_msg_id must be 1-64 bytes"));
        }

        // 2) 권한 검사: 이 방에 보낼 수 있는 사람인지 확인한다.
        let room_type = self.room_access(user_id, room_id).await?;
        let mut redis = self.redis.clone();

        // 3) 도배 제한: 사용자별로 "지금 이 1초" 카운터를 1 올리고, 한도를 넘으면 거부한다.
        //    카운터 키는 2초 뒤 자동으로 지워진다.
        let rl_key = keys::rate_limit(user_id, now_ms() / 1000);
        let (count,): (i64,) = redis::pipe()
            .atomic()
            .incr(&rl_key, 1)
            .expire(&rl_key, 2)
            .ignore()
            .query_async(&mut redis)
            .await?;
        if count > self.rate_limit_per_sec {
            return Err(AppError::RateLimited);
        }

        // 4) 중복 전송 방지: 순번을 발급하기 전에 임시 번호를 먼저 "찜"한다.
        //    SET ... NX 는 키가 없을 때만 성공하므로, 같은 임시 번호의 두 번째 요청은 실패한다.
        //    이렇게 해야 앱이 같은 메시지를 다시 보내도 메시지가 두 개 생기지 않는다.
        let dedup_key = keys::dedup(room_id, user_id, client_msg_id);
        let claimed: Option<String> = redis::cmd("SET")
            .arg(&dedup_key)
            .arg(DEDUP_PENDING)
            .arg("NX")
            .arg("EX")
            .arg(DEDUP_TTL_SECS)
            .query_async(&mut redis)
            .await?;
        if claimed.is_none() {
            // 찜에 실패했다 = 이미 같은 임시 번호가 들어온 적이 있다.
            let earlier: Option<String> = redis.get(&dedup_key).await?;
            return match earlier.as_deref().and_then(parse_dedup) {
                // 이전 처리가 끝나 결과가 기록돼 있으면 그 결과를 그대로 돌려준다.
                Some((message_id, seq)) => Ok(SendResult {
                    message_id,
                    seq,
                    duplicate: true,
                }),
                // 아직 "처리 중" 표시뿐이면 잠시 뒤 다시 보내라고 알린다.
                None => Err(AppError::InProgress),
            };
        }

        // 5) 번호를 붙이고 내보낸다.
        match self
            .publish(user_id, user_name, room_id, room_type, body)
            .await
        {
            Ok((message_id, seq)) => {
                // 성공: "처리 중" 표시를 실제 결과(고유번호:순번)로 바꿔 둔다.
                // 이후 같은 임시 번호가 다시 오면 이 값을 돌려준다.
                let _: Result<(), _> = redis
                    .set_ex(&dedup_key, format!("{message_id}:{seq}"), DEDUP_TTL_SECS)
                    .await;
                Ok(SendResult {
                    message_id,
                    seq,
                    duplicate: false,
                })
            }
            Err(err) => {
                // 실패: 찜을 풀어서 앱이 다시 보냈을 때 정상 처리되게 한다.
                let _: Result<(), _> = redis.del(&dedup_key).await;
                Err(err)
            }
        }
    }

    /// 순번과 고유 번호를 붙여 기록 통로와 실시간 전달 통로로 내보낸다.
    /// 돌려주는 값은 (고유 번호, 순번).
    async fn publish(
        &self,
        user_id: i64,
        user_name: &str,
        room_id: i64,
        room_type: RoomType,
        body: &str,
    ) -> Result<(i64, i64), AppError> {
        let mut redis = self.redis.clone();
        // 방의 순번 카운터를 1 올리고 그 값을 이 메시지의 순번으로 쓴다.
        // Redis 의 INCR 는 동시에 여러 요청이 와도 같은 번호를 두 번 주지 않는다.
        let seq: i64 = redis.incr(keys::room_seq(room_id), 1).await?;
        // 메시지를 완성한다. 시각은 서버 시계 기준.
        let message = ChatMessage {
            t: ChatMessage::TAG.to_string(),
            room_id,
            room_type,
            seq,
            message_id: self.ids.next(),
            sender_id: user_id,
            sender_name: user_name.to_string(),
            kind: MessageKind::Text,
            body: body.to_string(),
            created_at: now_ms(),
        };
        // JSON 으로 바꾼다. 이후 모든 경로가 이 문자열을 그대로 쓴다.
        let payload = serde_json::to_string(&message)?;

        // 기록 통로(Kafka)에 적는다. 여기가 "되돌릴 수 없는 지점"이다.
        // 이 기록이 확인된 뒤에야 보낸 사람에게 "전송 완료"를 알린다.
        // 실패하면 이 순번은 그냥 빈 번호로 남는다.
        // 키를 방 번호로 주기 때문에 같은 방의 메시지는 같은 파티션에 쌓인다.
        let key = room_id.to_string();
        self.kafka
            .send(
                FutureRecord::to(KAFKA_TOPIC).key(&key).payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| anyhow!("kafka produce failed: {err}"))?;

        // 여기부터는 실패해도 전송 자체는 성공으로 본다.
        // 놓친 것은 앱이 순번 동기화(sync)로 다시 받아 간다.

        // Redis 최근 메시지 보관함에 넣고(순번을 점수로 정렬), 오래된 것은 잘라낸다.
        let recent_key = keys::room_recent(room_id);
        let cached: Result<(), _> = redis::pipe()
            .zadd(&recent_key, &payload, seq)
            .ignore()
            .zremrangebyrank(&recent_key, 0, -(self.recent_cap + 1))
            .ignore()
            .query_async(&mut redis)
            .await;
        if let Err(err) = cached {
            tracing::warn!(room_id, seq, "recent cache update failed: {err}");
        }
        // 실시간 전달 통로(NATS)로 내보낸다. 이 방을 구독 중인 게이트웨이들이 받아 사용자에게 전달한다.
        if let Err(err) = self.nats.publish(room_subject(room_id), payload.into()).await {
            tracing::warn!(room_id, seq, "realtime publish failed: {err}");
        }

        Ok((message.message_id, seq))
    }

    /// `after_seq` 보다 뒤의 메시지를 오래된 것부터 돌려준다.
    ///
    /// 최근 메시지는 Redis 에서, 그보다 오래된 것은 ScyllaDB 에서 가져온다.
    /// (ScyllaDB 는 그룹 방만 쓴다. 라이브 방은 Redis 에 있는 최근분이 전부다.)
    /// 두 범위는 겹치지 않는다: Redis 에 있는 가장 오래된 순번보다 앞은 ScyllaDB 가,
    /// 그 이후는 Redis 가 맡는다. 그래서 저장 담당이 아직 DB 에 못 넣은 메시지도 빠지지 않는다.
    pub async fn fetch(
        &self,
        user_id: i64,
        room_id: i64,
        after_seq: i64,
        limit: i32,
    ) -> Result<FetchResult, AppError> {
        // 권한 검사.
        let room_type = self.room_access(user_id, room_id).await?;
        // 요청 값을 허용 범위로 맞춘다.
        let limit = limit.clamp(1, MAX_FETCH) as usize;
        let after_seq = after_seq.max(0);
        let latest_seq = self.latest_seq(room_id).await?;

        let mut messages: Vec<String> = Vec::new();
        // 지금까지 담은 메시지 중 마지막 순번
        let mut last_seq = after_seq;
        // 이미 최신까지 다 받은 상태면 빈 결과를 돌려준다.
        if after_seq >= latest_seq {
            return Ok(FetchResult {
                messages,
                latest_seq,
                has_more: false,
            });
        }

        // Redis 보관함에서 가장 오래된 메시지의 순번을 알아낸다.
        let mut redis = self.redis.clone();
        let recent_key = keys::room_recent(room_id);
        let oldest: Vec<String> = redis::cmd("ZRANGE")
            .arg(&recent_key)
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut redis)
            .await?;
        let oldest_cached: Option<i64> = oldest.get(1).and_then(|s| parse_score(s));
        // ScyllaDB 가 맡을 범위의 끝(이 값 미만까지). 보관함이 비어 있으면 최신 순번까지 전부.
        let db_upper = oldest_cached.unwrap_or(latest_seq + 1);

        // 요청 범위가 Redis 보관함보다 오래된 구간을 포함하면 ScyllaDB 에서 읽는다.
        if room_type == RoomType::Group && after_seq + 1 < db_upper {
            // 메시지는 10만 개 단위 구간(bucket)으로 나뉘어 있으므로 해당 구간을 차례로 읽는다.
            for bucket in bucket_of(after_seq + 1)..=bucket_of(db_upper - 1) {
                let remaining = limit - messages.len();
                if remaining == 0 {
                    break;
                }
                // 이 구간에서 last_seq 초과, db_upper 미만인 메시지를 순번 오름차순으로 가져온다.
                let result = self
                    .scylla
                    .execute_unpaged(
                        &self.fetch_stmt,
                        (room_id, bucket, last_seq, db_upper, remaining as i32),
                    )
                    .await?
                    .into_rows_result()?;
                for row in
                    result.rows::<(i64, i64, i64, String, i8, String, CqlTimestamp)>()?
                {
                    let (seq, message_id, sender_id, sender_name, kind, body, created_at) = row?;
                    // DB 의 행을 메시지로 되살린다.
                    let message = ChatMessage {
                        t: ChatMessage::TAG.to_string(),
                        room_id,
                        room_type,
                        seq,
                        message_id,
                        sender_id,
                        sender_name,
                        kind: MessageKind::from_i8(kind),
                        body,
                        created_at: created_at.0,
                    };
                    // 실시간으로 받는 메시지와 똑같은 JSON 모양으로 만든다.
                    messages.push(serde_json::to_string(&message)?);
                    last_seq = seq;
                }
            }
        }

        // 아직 자리가 남았으면 나머지를 Redis 보관함에서 채운다.
        if messages.len() < limit {
            // DB 에서 읽은 부분과 겹치지 않도록 시작 지점을 정한다.
            let from = last_seq.max(db_upper - 1);
            // "(from" 은 from 초과를 뜻한다. 결과는 [메시지, 순번, 메시지, 순번, ...] 순서로 온다.
            let cached: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(&recent_key)
                .arg(format!("({from}"))
                .arg("+inf")
                .arg("WITHSCORES")
                .arg("LIMIT")
                .arg(0)
                .arg(limit - messages.len())
                .query_async(&mut redis)
                .await?;
            // 두 개씩 묶어서 (메시지, 순번)으로 읽는다.
            for pair in cached.chunks_exact(2) {
                if let Some(seq) = parse_score(&pair[1]) {
                    last_seq = seq;
                }
                // 보관함의 메시지는 이미 JSON 이므로 그대로 담는다.
                messages.push(pair[0].clone());
            }
        }

        Ok(FetchResult {
            // 뭔가 담았는데 아직 최신 순번에 못 미쳤으면 더 받아갈 것이 있다는 뜻.
            has_more: !messages.is_empty() && last_seq < latest_seq,
            messages,
            latest_seq,
        })
    }
}

/// 중복 방지 키에 저장된 "고유번호:순번" 문자열을 풀어낸다. "pending" 이면 None.
fn parse_dedup(value: &str) -> Option<(i64, i64)> {
    let (message_id, seq) = value.split_once(':')?;
    Some((message_id.parse().ok()?, seq.parse().ok()?))
}

/// Redis 가 돌려준 점수(실수 문자열)를 순번(정수)으로 바꾼다.
fn parse_score(score: &str) -> Option<i64> {
    score.parse::<f64>().ok().map(|v| v as i64)
}
