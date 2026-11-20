# 채팅 서버

대규모 채팅 서버를 가장 단순한 형태에서 시작해 한 단계씩 키워 가는 프로젝트.

- 진행 원칙과 단계 계획, git 작업 방식: [CLAUDE.md](CLAUDE.md)
- 완성형 참고 구현: `result` 브랜치 (`git switch result`)
- 단계별 사고 과정 (왜 그렇게 했고, 무엇을 일부러 하지 않았는지): [docs/](docs/)

## 지금 상태

5단계(`feature/05-split-gateway`) 진행 중. 서버 한 대가 하던 일을 두 프로그램으로 나눴다.
게이트웨이는 브라우저 연결을 붙들고, 채팅 서버는 로그인·방·저장을 맡는다. 둘 사이는 gRPC 로 부른다.
채팅 서버를 껐다 켜도 붙어 있던 사람은 끊기지 않는다. (사고 과정: [docs/05-split-gateway.md](docs/05-split-gateway.md))

```
                              ┌──▶ 게이트웨이 1 (8081) ──┐
              화면, /ws       │           ▲              │ gRPC
브라우저들 ──▶ 로드밸런서 ────┤          NATS ◀──────────┼──────── 채팅 서버 ──▶ PostgreSQL
              (8080)          │           ▼              │        (8090, 50051)
                   │          └──▶ 게이트웨이 2 (8082) ──┘              ▲
                   └──────────────── /login, /rooms ────────────────────┘
```

## 실행

protoc 가 필요하다. 없으면 `brew install protobuf` 로 깐다. (gRPC 약속 파일을 코드로 바꾸는 프로그램)

```bash
docker compose up -d                   # PostgreSQL, 로드밸런서, NATS 켜기 (처음 한 번)
cargo run --bin chat                   # 터미널 1: 채팅 서버 (HTTP 8090, gRPC 50051)
cargo run --bin gateway                # 터미널 2: 게이트웨이 (8081)
PORT=8082 cargo run --bin gateway      # 터미널 3: 게이트웨이 한 대 더 (8082)
```

로드밸런서 설정이 바뀌었다. 전 단계에서 켜 둔 것이 있으면 `docker compose restart lb` 로 다시 읽힌다.

브라우저로 http://localhost:8080 을 열고 이름과 비밀번호를 넣고 방을 고르면 대화할 수 있다.
처음 쓰는 이름이면 그 비밀번호로 계정이 만들어진다.

게이트웨이는 한 대만 띄워도 된다. 로드밸런서가 켜져 있는 쪽으로 보낸다.
채팅 서버를 껐다(Ctrl+C) 다시 켜도 지난 대화가 남아 있고, 붙어 있던 사람도 끊기지 않는다.
