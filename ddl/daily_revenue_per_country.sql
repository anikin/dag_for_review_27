CREATE TABLE datamarts.daily_revenue_per_country ON CLUSTER cluster_4x2 (
    event_date Date,
    country String,
    total_revenue Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date);
