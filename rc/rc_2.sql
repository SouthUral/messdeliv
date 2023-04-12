CREATE OR REPLACE PROCEDURE device.set_messages(IN p_message jsonb, IN p_offset bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $procedure$
    begin
	    INSERT INTO device.messages (
	    	offset_msg,
            created_id,
            device_id,
            object_id,
            mes_id,
            mes_time,
            mes_code,
            mes_status,
            mes_data,
            event_value,
            event_data
        )
        VALUES (
        	p_offset,
            convert_to((p_message -> 'data' ->> '_id'), 'utf8'),
            (p_message ->> 'device_id') :: bigint,
            (p_message ->> 'object_id') :: integer,
            (p_message ->> 'mes_id') :: bigint,
            (p_message ->> 'mes_time') :: timestamp,
            (p_message -> 'event_info' ->> 'code') :: integer,
            (p_message -> 'data' -> 'status_info'),
            (p_message -> 'data'),
            (p_message -> 'event'),
            (p_message -> 'event_data')
        );
    end;
$procedure$
;