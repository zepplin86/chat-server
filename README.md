# 채팅 서버

대규모 채팅 서버를 가장 단순한 형태에서 시작해 한 단계씩 키워 가는 프로젝트.

- 진행 원칙과 단계 계획, git 작업 방식: [CLAUDE.md](CLAUDE.md)
- 완성형 참고 구현: `result` 브랜치 (`git switch result`)
- 단계별 사고 과정 (왜 그렇게 했고, 무엇을 일부러 하지 않았는지): [docs/](docs/)

## 지금 상태

7단계(`feature/07-scylla`) 진행 중. 메시지를 넣고 읽는 곳을 PostgreSQL 에서 ScyllaDB 로 옮겼다.
계정·입장권·방은 PostgreSQL 에 그대로 있다.
옮기는 일은 끝났고 잘 돈다. 다만 재 보니 저장은 1.6배 빨라졌을 뿐 여전히 전달을 따라가지 못한다.
(사고 과정과 잰 값: [docs/07-scylla.md](docs/07-scylla.md))

```
                              ┌──▶ 게이트웨이 1 ──┐
              화면, /ws       │        ▲          │ gRPC              ┌─▶ PostgreSQL (계정, 입장권, 방)
브라우저들 ──▶ 로드밸런서 ────┤       NATS ◀──────┼────── 채팅 서버 ──┤
              (8080)          │        ▼          │           │       └─▶ ScyllaDB (지난 대화 읽기)
                   │          └──▶ 게이트웨이 2 ──┘           │ 적기             ▲
                   │                                          ▼                  │ 넣기
                   │                                        Kafka ──읽기──▶ 저장 담당
                   └────────── /login, /rooms ──▶ 채팅 서버
```

## 실행

protoc 와 cmake 가 필요하다. 없으면 `brew install protobuf cmake` 로 깐다.
(protoc 는 gRPC 약속 파일을 코드로 바꾸고, cmake 는 Kafka 라이브러리를 빌드한다. 처음 빌드는 몇 분 걸린다)

```bash
docker compose up -d                   # PostgreSQL, ScyllaDB, 로드밸런서, NATS, Kafka 켜기
                                       # ScyllaDB 는 처음 켤 때 1분쯤 걸린다
cargo run --bin migrate                # 6단계까지 쌓인 메시지를 ScyllaDB 로 옮긴다 (한 번만)
cargo run --bin chat                   # 터미널 1: 채팅 서버 (HTTP 8090, gRPC 50051)
cargo run --bin persist                # 터미널 2: 저장 담당
cargo run --bin gateway                # 터미널 3: 게이트웨이 (8081)
PORT=8082 cargo run --bin gateway      # 터미널 4: 게이트웨이 한 대 더 (8082)
```

브라우저로 http://localhost:8080 을 열고 이름과 비밀번호를 넣고 방을 고르면 대화할 수 있다.
처음 쓰는 이름이면 그 비밀번호로 계정이 만들어진다.

게이트웨이는 한 대만 띄워도 된다. 로드밸런서가 켜져 있는 쪽으로 보낸다.
저장 담당을 꺼 둬도 대화는 된다. 다만 그동안 한 말은 저장소에 안 들어가고, 다시 켜면 밀린 것이 들어간다.
