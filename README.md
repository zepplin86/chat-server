# 채팅 서버

[ARCHITECTURE.md](ARCHITECTURE.md)의 설계를 Docker Compose로 띄울 수 있게 구현한 것.

## 실행

```bash
docker compose up -d --build     # 처음에는 이미지 받고 빌드하느라 몇 분 걸린다
open http://localhost:8080       # 테스트 화면
```

브라우저 창을 두 개 열어 서로 다른 이름으로 들어간 뒤, 한쪽에서 방을 만들고 다른 쪽에서 방 목록을 새로고침해 들어가면 된다.

```bash
node scripts/smoke.mjs           # 전체 흐름 자동 점검 (Node 22 이상)
docker compose logs -f gateway-1 gateway-2 chat-service-1 chat-service-2
docker compose down              # 중지
docker compose down -v           # 중지 + 데이터 삭제
```

호스트 8080 포트가 이미 쓰이고 있으면 `HTTP_PORT=9000 docker compose up -d`처럼 바꾼다.
호스트로 열리는 포트는 이것 하나뿐이다. DB, Kafka 등은 compose 네트워크 안에서만 접근된다.

게이트웨이나 채팅 서버 이미지를 다시 빌드해 컨테이너가 새로 만들어졌으면 `docker compose restart lb`를 한 번 해 준다. nginx가 시작할 때 알아낸 주소를 계속 쓰기 때문이다.

## 구성

| 컨테이너 | 역할 | 설계 문서의 이름 |
|---|---|---|
| `lb` | nginx. 테스트 화면 제공, `/ws`는 게이트웨이로, `/api`는 채팅 서버로 분배 | 로드밸런서 |
| `gateway-1`, `gateway-2` | WebSocket 연결 유지, 방 구독, 느린 사용자 처리 | 게이트웨이 서버 |
| `chat-service-1`, `chat-service-2` | 권한 검사, 도배 제한, 번호 붙이기, 발행. 로그인/방 관리 REST | 채팅 서버 |
| `nats` | 게이트웨이로 메시지 실시간 전달 | 실시간 전달 통로 |
| `kafka` | 모든 메시지를 순서대로 기록 | 기록 통로 |
| `persist-worker` | Kafka → ScyllaDB 저장 | 저장 담당 |
| `push-worker` | Kafka → 접속 안 한 멤버에게 알림 (지금은 로그만 출력) | 푸시 담당 |
| `scylla` | 메시지 저장 | 메시지 DB |
| `redis` | 접속 상태, 방별 순번, 최근 메시지, 도배 카운터, 중복 전송 방지 | 보조 저장소 |
| `postgres` | 사용자, 방, 멤버 | 보조 저장소 |
| `kafka-init`, `scylla-init` | 토픽/테이블을 한 번 만들고 종료 | — |

게이트웨이와 채팅 서버를 일부러 두 대씩 띄웠다. 서로 다른 게이트웨이에 붙은 사용자끼리 NATS를 거쳐 대화가 되는지 확인하기 위해서다. 테스트 화면 오른쪽 위에 지금 붙은 게이트웨이 이름이 나온다.

## 코드 위치

```
proto/chat.proto            게이트웨이 ↔ 채팅 서버 gRPC 규격
crates/common               공용: 메시지 형식, 토큰, ID 생성기, Redis 키 이름
crates/gateway              연결 계층
  src/conn.rs                 연결 하나의 읽기/쓰기 루프, 클라이언트 프레임 처리
  src/hub.rs                  방별 연결 목록, NATS 구독, 팬아웃
crates/chat-service         로직 계층
  src/service.rs              전송, 권한, 이력 조회의 핵심 로직
  src/grpc.rs  src/http.rs    gRPC / REST 입구
crates/persist-worker       저장 담당
crates/push-worker          푸시 담당
deploy/                     nginx 설정, Postgres/ScyllaDB 초기 스키마
web/index.html              테스트 화면
scripts/smoke.mjs           자동 점검 스크립트
```

## 통신 규격

### REST (`/api`)

| 요청 | 설명 |
|---|---|
| `POST /api/login` `{name}` | 개발용 로그인. 이름만으로 토큰 발급 |
| `GET /api/rooms` | 방 목록 |
| `POST /api/rooms` `{name, type}` | 방 생성. `type`은 `group` 또는 `live` |
| `POST /api/rooms/{id}/join` | 그룹 방 멤버로 가입 |

로그인 외에는 `Authorization: Bearer <token>` 헤더가 필요하다.

### WebSocket (`/ws?token=<token>`)

모든 프레임은 JSON이고 `t` 필드로 종류를 구분한다.

| 방향 | 프레임 | 설명 |
|---|---|---|
| 서버→앱 | `ready` | 연결 완료. 붙은 게이트웨이 이름 포함 |
| 앱→서버 | `join {room_id}` | 방 입장 |
| 서버→앱 | `joined {room_id, room_type, latest_seq}` | 입장 완료 |
| 앱→서버 | `send {room_id, client_msg_id, body}` | 메시지 전송 |
| 서버→앱 | `ack {client_msg_id, message_id, seq, duplicate}` | 전송 완료 |
| 서버→앱 | `message {room_id, seq, message_id, sender_id, sender_name, type, body, created_at}` | 메시지 수신 |
| 앱→서버 | `sync {room_id, after_seq, limit}` | `after_seq` 이후 메시지 요청 |
| 서버→앱 | `sync {room_id, messages, latest_seq, has_more}` | 요청한 메시지 묶음 |
| 앱→서버 | `leave {room_id}` | 방 나가기 |
| 앱→서버 / 서버→앱 | `ping` / `pong` | 연결 확인. 앱이 25초마다 보낸다 |
| 서버→앱 | `reconnect` | 서버 교체 중. 다시 접속하라는 요청 |
| 서버→앱 | `error {code, message, client_msg_id}` | `FORBIDDEN`, `NOT_FOUND`, `RATE_LIMITED`, `INVALID`, `NOT_JOINED`, `IN_PROGRESS`, `UNAVAILABLE` |

앱이 지켜야 할 규칙:

- 방마다 마지막으로 받은 `seq`를 기억한다. 입장 직후와 재접속 직후에 `sync`를 보낸다.
- 받은 메시지의 `seq`가 기억한 값보다 2 이상 크면 `sync`로 빠진 구간을 요청한다.
- `sync` 응답이 비어 있으면 기억한 `seq`를 `latest_seq`로 올린다. 전송이 실패한 메시지의 순번은 영영 비어 있을 수 있다.
- `ack`를 받지 못한 메시지는 같은 `client_msg_id`로 다시 보낸다. 서버가 중복을 걸러 준다.
- 재접속은 점점 길게, 각자 조금씩 다른 시간만큼 기다렸다가 한다.

## 직접 확인해 볼 것

```bash
# 게이트웨이별 접속자 수, 버려진 메시지 수
docker compose exec gateway-1 bash -c 'exec 3<>/dev/tcp/localhost/8080; printf "GET /stats HTTP/1.0\r\n\r\n" >&3; cat <&3' 2>/dev/null | tail -1

# 저장된 메시지
docker compose exec scylla cqlsh -e "SELECT room_id, seq, sender_name, body FROM chat.messages LIMIT 20"

# 게이트웨이 한 대 교체: 붙어 있던 화면이 자동으로 다른 게이트웨이로 옮겨 간다
docker compose restart gateway-1

# 접속 안 한 멤버에게 가는 알림 (로그로만 출력)
docker compose logs -f push-worker
```

## 설정

`docker-compose.yml`의 환경 변수로 조정한다.

| 변수 | 기본값 | 설명 |
|---|---|---|
| `JWT_SECRET` | `dev-secret-change-me` | 토큰 서명 키. **개발용 기본값이므로 공유 환경에서는 반드시 바꾼다** |
| `RATE_LIMIT_PER_SEC` | 5 | 사용자 한 명이 1초에 보낼 수 있는 메시지 수 (채팅 서버) |
| `RECENT_CACHE_SIZE` | 500 | 방마다 Redis에 두는 최근 메시지 수 (채팅 서버) |
| `SEND_QUEUE` | 256 | 연결 하나에 쌓아 둘 수 있는 메시지 수. 넘으면 느린 사용자로 처리 (게이트웨이) |
| `NODE_ID` | — | 채팅 서버마다 달라야 한다. 메시지 고유 번호에 들어간다 |

## 아직 없는 것

설계 문서에는 있지만 이 구현에는 빠져 있다.

- **실제 인증**: 지금 로그인은 이름만 받는다. 누구나 아무 이름으로 들어올 수 있다.
- **실제 푸시 발송**: APNs/FCM 연동 없이 로그만 남긴다.
- **파일 저장소**: 사진/파일 전송 없음.
- **읽음 표시, 입력 중 표시, 수정/삭제, 멘션, 답글**: `last_read_seq` 컬럼만 만들어 두었다.
- **큰 방 메시지 샘플링**: 느린 사용자에게 가는 메시지는 버리지만, 방 전체의 전송 속도 상한은 없다.
- **1:1 전용 토픽(`user.{id}`)과 여러 기기 동기화**
- **Redis가 날아갔을 때 순번 복구**: Redis 데이터를 지우면 방 순번이 1부터 다시 시작해 기존 메시지와 겹친다.
- **TLS, 다중 리전, 지표 수집(Prometheus 등), 부하 테스트**
- 인프라는 전부 한 대짜리 개발용 설정이다. 운영에 그대로 쓰면 안 된다.
