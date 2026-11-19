# 채팅 서버

대규모 채팅 서버를 가장 단순한 형태에서 시작해 한 단계씩 키워 가는 프로젝트.

- 진행 원칙과 단계 계획, git 작업 방식: [CLAUDE.md](CLAUDE.md)
- 완성형 참고 구현: `result` 브랜치 (`git switch result`)
- 단계별 사고 과정 (왜 그렇게 했고, 무엇을 일부러 하지 않았는지): [docs/](docs/)

## 지금 상태

2단계(`feature/02-postgres`) 진행 중. 서버 한 대, 방 하나, 대화는 PostgreSQL 에 저장한다.

```
브라우저 ──WebSocket──▶ 채팅 서버 ──SQL──▶ PostgreSQL
```

## 실행

```bash
docker compose up -d   # PostgreSQL 켜기 (처음 한 번)
cargo run              # 채팅 서버 켜기
```

브라우저 두 개로 http://localhost:8080 을 열고 이름을 넣으면 대화할 수 있다.

서버를 껐다(Ctrl+C) 다시 켜도 지난 대화가 남아 있다.
