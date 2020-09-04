create function str_len as 'com.yiwei.functions.udf.StringLength';

create TABLE user_log (
    user_id VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    ts TIMESTAMP(3)
) with (
    'connector.type' = 'kafka',-- 使用 kafka connector
    'connector.version' = 'universal',-- kafka 版本，universal 支持 0.11 以上的版本
    'connector.topic' = 'test',-- kafka topic
--    'connector.startup-mode' = 'earliest-offset',-- 从起始 offset 开始读取
    'connector.startup-mode' = 'latest-offset',-- 从最新 offset 开始读取
    'connector.properties.0.key' = 'bootstrap.servers',-- 连接信息
    'connector.properties.0.value' = 'localhost:9092',
    'connector.properties.1.key' = 'group.id',
    'connector.properties.1.value' = 'testGroup-remote',
    'format.type' = 'json',-- 数据源格式为 json
    'update-mode' = 'append',
    'format.derive-schema' = 'true'-- 从 DDL schema 确定 json 解析规则
);

create TABLE user_behavior_sink (
    user_id VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    len1 BIGINT,
    len2 BIGINT
) with (
     'connector.type' = 'console'   -- print console
);

insert into user_behavior_sink
select
  user_id,
  item_id,
  category_id,
  behavior,
  str_len(category_id),
  str_len(category_id,item_id)
from user_log;