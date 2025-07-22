CREATE TABLE core.localUserMetadata ON CLUSTER cluster_4x2 (
    UserId            String,
    Country           String,
    LastLoginDate     DateTime
) ENGINE = ReplicatedReplacingMergeTree()
ORDER BY (UserId);


CREATE TABLE core.userMetadata ON CLUSTER cluster_4x2 (
    UserId            String,
    Country           String,
    LastLoginDate     DateTime
) ENGINE = Distributed('cluster_4x2', 'core', 'localUserMetadata', cityHash64(LastLoginDate));
