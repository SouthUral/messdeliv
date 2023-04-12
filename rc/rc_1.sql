create schema if not exists device;

create table if not exists device.messages 
(
    id          bigserial,
    offset_msg	bigint,
    created_at  timestamp default CURRENT_TIMESTAMP,
    created_id  bytea,
    device_id   bigint,
    object_id   integer,
    mes_id      bigint,
    mes_time    timestamp,
    mes_code    integer,
    mes_status  jsonb,
    mes_data    jsonb,
    event_value varchar,
    event_data  jsonb
);