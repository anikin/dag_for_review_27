CREATE TABLE raw.local_transactions ON CLUSTER cluster_4x2 (
    transaction_id      String,
    user_id             String,
    amount              Float64,
    created_at          DateTime
) ENGINE = ReplicatedMergeTree()
PARTITION BY amount
ORDER BY (transaction_id);


CREATE TABLE raw.transactions ON CLUSTER cluster_4x2 (
    transaction_id      String,
    user_id             String,
    amount              Float64,
    created_at          DateTime
) ENGINE = Distributed('cluster_4x2', 'raw', 'local_transactions', cityHash64(user_id));
