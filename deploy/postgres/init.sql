-- Metadata: users, rooms, memberships. Runs once, when the data volume is first created.

CREATE TABLE users (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE rooms (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT        NOT NULL,
    -- group: fixed members, everything stored. live: open to all, recent messages only.
    kind       TEXT        NOT NULL CHECK (kind IN ('group', 'live')),
    owner_id   BIGINT      NOT NULL REFERENCES users (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE room_members (
    room_id       BIGINT      NOT NULL REFERENCES rooms (id) ON DELETE CASCADE,
    user_id       BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role          TEXT        NOT NULL DEFAULT 'member',
    -- Read state is one number per member, not a flag per message.
    last_read_seq BIGINT      NOT NULL DEFAULT 0,
    joined_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (room_id, user_id)
);

CREATE INDEX room_members_user_idx ON room_members (user_id);
