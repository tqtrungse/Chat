/*
    Always perform an upsert for the message subsequently, even if the idempotence record already exists:

    Generate a new `msg_id` and execute `INSERT ... IF NOT EXISTS` into `offline_message_idempotence`.
    If `applied` is `false`, the result returns the existing row; use that row's `msg_id`.
    Always upsert into `offline_messages` using this `msg_id`.

    Step 3 is naturally idempotent because the key is `((device_id, day_bucket), msg_id)`: writing with the same key
    simply overwrites the value with the same data. In `gocqlx`, `GetCAS(&existing)` returns the `applied` status and
    populates `existing` with the existing row.

    Constraint: The `day_bucket` must remain consistent across retries. If calculated based on the current time, a retry
    crossing midnight would write to a different partition and create a duplicate. Therefore, derive the `day_bucket`
    from the timestamp within the `msg_id` (using Sonyflake).

    TODO: LWT Costs
    The `IF NOT EXISTS` operation runs Paxos (or Raft, depending on the Scylla version and configuration), incurring
    multiple round trips and lower throughput compared to standard writes. If this operation sits on the write path for
    every message, it becomes a bottleneck. However, you cannot simply replace it with a `SELECT` followed by an
    `INSERT`, as two concurrent retries might both see the record as "non-existent." Under heavy load,
    consider the following:
        (1) Use LWT only when the client marks a message as a retry, while performing a standard write for the initial
        attempt.

        (2) Alternatively, accept duplicates at the storage layer and deduplicate at the read/delivery layer
        (clients already possess the `device_channel_msg_seq` to handle this).
*/

CREATE TABLE offline_message_idempotence (
    device_id  BIGINT,
    channel_id BIGINT,
    msg_id     BIGINT,

    -- Random a number, The collision probability only needs to be calculated within a single partition defined by
    -- `(device_id, channel_id)`. For *n* messages over 14 days, the probability is approximately *n*²/2⁶⁴.
    -- With *n* = 100,000, this is around 5 × 10⁻¹⁰, which is negligible.
    dedup_key  BIGINT,

    PRIMARY KEY ((device_id, channel_id), dedup_key)
) WITH
    -- (1) Auto-retry window: Use exponential backoff with jitter (capped at a 60-second interval between attempts);
    -- stop after approximately 24 hours or N attempts, then mark the status as "send failed" to let the user decide
    -- on the next step.
    --
    -- (2) Client-side deduplication key lifespan: The key remains in the outbox and is valid for manual retries.
    -- I propose 3 days, with a maximum of 7.
    -- Once expired, the message is treated as new, and a new key is generated.
    --
    -- (3) Server-side idempotence table TTL: Must exceed the retry window (2) plus a safety margin.
    -- I propose 7 days.
    default_time_to_live = 604800; -- 7 days


CREATE TABLE offline_messages (
    device_id         BIGINT,
    sender_device_id  BIGINT,
    day_bucket        INT,
    msg_id            BIGINT,
    channel_id        BIGINT,

    -- E2E
    -- According to ScyllaDB documentation: while a blob can theoretically reach up to 2 GB, the recommendation is to
    -- keep it under 1 MB; furthermore, row size impacts latency—latency is good at the hundreds-of-KB range but poor at
    -- the MB range. Additionally, a mutation is limited by default to 16 MB (half the size of a commitlog segment).
    -- For chat messages, you should avoid approaching these limits.
    --
    -- Recommendation: a hard limit of 64 KiB for the entire blob (encrypted key + content ciphertext, including nonce,
    -- tag, and framing), with a typical target of under 4 KiB.
    payload           BLOB,

    PRIMARY KEY ((device_id, day_bucket), msg_id)
) WITH
   default_time_to_live = 1209600 -- 14 days
   AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'HOURS',
        'compaction_window_size': 24
   };