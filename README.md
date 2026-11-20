# 채팅 서버

대규모 채팅 서버를 가장 단순한 형태에서 시작해 한 단계씩 키워 가는 프로젝트.

- 진행 원칙과 단계 계획, git 작업 방식: [CLAUDE.md](CLAUDE.md)
- 완성형 참고 구현: `result` 브랜치 (`git switch result`)
- 단계별 사고 과정 (왜 그렇게 했고, 무엇을 일부러 하지 않았는지): [docs/](docs/)

## 지금 상태

6단계(`v0.6.0`)까지 끝났다. 메시지를 DB 에 넣는 일을 Kafka 뒤로 뺐다.
채팅 서버는 Kafka 에 적어 두고 바로 전달한다. 저장 담당이 Kafka 를 읽어 PostgreSQL 에 넣는다.
DB 가 느려지거나 멈춰도 대화는 멈추지 않는다. (사고 과정: [docs/06-kafka.md](docs/06-kafka.md))

```
                              ┌──▶ 게이트웨이 1 ──┐
              화면, /ws       │        ▲          │ gRPC
브라우저들 ──▶ 로드밸런서 ────┤       NATS ◀──────┼────── 채팅 서버 ──읽기──▶ PostgreSQL
              (8080)          │        ▼          │           │                  ▲
                   │          └──▶ 게이트웨이 2 ──┘           │ 적기             │ 넣기
                   │                                          ▼                  │
                   │                                        Kafka ──읽기──▶ 저장 담당
                   └────────── /login, /rooms ──▶ 채팅 서버
```

## 실행

protoc 와 cmake 가 필요하다. 없으면 `brew install protobuf cmake` 로 깐다.
(protoc 는 gRPC 약속 파일을 코드로 바꾸고, cmake 는 Kafka 라이브러리를 빌드한다. 처음 빌드는 몇 분 걸린다)

```bash
docker compose up -d                   # PostgreSQL, 로드밸런서, NATS, Kafka 켜기 (처음 한 번)
cargo run --bin chat                   # 터미널 1: 채팅 서버 (HTTP 8090, gRPC 50051)
cargo run --bin persist                # 터미널 2: 저장 담당
cargo run --bin gateway                # 터미널 3: 게이트웨이 (8081)
PORT=8082 cargo run --bin gateway      # 터미널 4: 게이트웨이 한 대 더 (8082)
```

브라우저로 http://localhost:8080 을 열고 이름과 비밀번호를 넣고 방을 고르면 대화할 수 있다.
처음 쓰는 이름이면 그 비밀번호로 계정이 만들어진다.

게이트웨이는 한 대만 띄워도 된다. 로드밸런서가 켜져 있는 쪽으로 보낸다.
저장 담당을 꺼 둬도 대화는 된다. 다만 그동안 한 말은 DB 에 안 들어가고, 다시 켜면 밀린 것이 들어간다.
