create TABLE user_log (
   msg varchar
) with (
    'connector.type' = 'kafka',-- 使用 kafka connector
    'connector.version' = 'universal',-- kafka 版本，universal 支持 0.11 以上的版本
    'connector.topic' = 'test',-- kafka topic
--    'connector.startup-mode' = 'earliest-offset',-- 从起始 offset 开始读取
    'connector.startup-mode' = 'latest-offset',-- 从最新 offset 开始读取
    'connector.properties.0.key' = 'bootstrap.servers',-- 连接信息
    'connector.properties.0.value' = 'hd1-tech-vpc-back-flink-hangzhou-prod-002:9092',
    'connector.properties.1.key' = 'group.id',
    'connector.properties.1.value' = 'testGroup-remote',
    'format.type' = 'string',-- 数据源格式为 json
    'update-mode' = 'append',
    'format.derive-schema' = 'true'-- 从 DDL schema 确定 json 解析规则
);

create TABLE user_behavior_sink (
    msg varchar
) with (
     'connector.type' = 'console'   -- print console
);

insert into user_behavior_sink
select
  msg
from user_log;