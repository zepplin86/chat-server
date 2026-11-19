# 채팅 서버

대규모 채팅 서버를 가장 단순한 형태에서 시작해 한 단계씩 키워 가는 프로젝트.

- 진행 원칙과 단계 계획, git 작업 방식: [CLAUDE.md](CLAUDE.md)
- 완성형 참고 구현: `result` 브랜치 (`git switch result`)
- 단계별 사고 과정 (왜 그렇게 했고, 무엇을 일부러 하지 않았는지): [docs/](docs/)

## 지금 상태

3단계(`feature/03-two-servers`) 진행 중. 같은 서버를 여러 대 띄울 수 있다.
다만 다른 서버에 붙은 사람끼리는 아직 실시간으로 대화가 안 된다. (이유: [docs/03-two-servers.md](docs/03-two-servers.md))

```
브라우저 A ──▶ 서버 1 ─┐
                       ├──▶ PostgreSQL
브라우저 B ──▶ 서버 2 ─┘
```

## 실행

```bash
docker compose up -d   # PostgreSQL 켜기 (처음 한 번)
cargo run              # 채팅 서버 켜기
```

브라우저 두 개로 http://localhost:8080 을 열고 이름과 비밀번호를 넣고 방을 고르면 대화할 수 있다.
처음 쓰는 이름이면 그 비밀번호로 계정이 만들어진다.

서버를 껐다(Ctrl+C) 다시 켜도 지난 대화가 남아 있다.

서버를 두 대 띄우려면 터미널 두 개에서 포트를 다르게 준다.

```bash
PORT=8080 cargo run   # 터미널 1 → http://localhost:8080
PORT=8081 cargo run   # 터미널 2 → http://localhost:8081
```
