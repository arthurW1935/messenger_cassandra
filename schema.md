
# Messenger App - Cassandra Schema

This document describes the Cassandra database schema used for the Messenger application.

## Keyspace

```
CREATE KEYSPACE IF NOT EXISTS messenger WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

## Tables

### 1. last_message_cache

Stores the last message sent in each conversation.

```
CREATE TABLE IF NOT EXISTS last_message_cache (
    sender_id INT,
    receiver_id INT,
    conversation_id INT,
    last_timestamp TIMESTAMP,
    last_message TEXT,
    PRIMARY KEY (conversation_id)
);
```

### 2. messages

Stores all messages in a conversation. Messages are clustered by timestamp (most recent first) and message ID for uniqueness.

```
CREATE TABLE IF NOT EXISTS messages (
    conversation_id INT,
    timestamp TIMESTAMP,
    message_id INT,
    content TEXT,
    sender_id INT,
    receiver_id INT,
    PRIMARY KEY (conversation_id, timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC);
```

### 3. conversations

Tracks user conversations and allows quick retrieval of the most recent ones.

```
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id INT,
    user1_id INT,
    user2_id INT,
    last_timestamp TIMESTAMP,
    PRIMARY KEY (conversation_id, user1_id, user2_id)
);
```

## Notes

- All table operations are performed after ensuring the keyspace is set.
- `SimpleStrategy` with replication factor 1 is used, suitable for development or single-node setups.
