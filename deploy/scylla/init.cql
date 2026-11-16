-- Message store. Single-node replication for local development.

CREATE KEYSPACE IF NOT EXISTS chat
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS chat.messages (
    room_id     bigint,
    bucket      int,        -- seq / 100000, keeps one room from growing a single huge partition
    seq         bigint,
    message_id  bigint,
    sender_id   bigint,
    sender_name text,
    kind        tinyint,    -- 0 text, 1 system
    body        text,
    created_at  timestamp,
    PRIMARY KEY ((room_id, bucket), seq)
) WITH CLUSTERING ORDER BY (seq DESC);
