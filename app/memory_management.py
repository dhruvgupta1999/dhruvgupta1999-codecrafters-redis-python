"""
Logic to manage the memory.
"""
from collections import defaultdict

from app.key_value_utils import NO_EXPIRY, ValueObj, NULL_VALUE_OBJ, ValueTypes
from app.streams_dsa import RedisStream, NUM_DIGITS_TS, NUM_DIGITS_SEQ, StreamTimestampId

redis_memstore: [bytes, ValueObj] = {}


def get_from_memstore(key, request_recv_time_ms):
    value_obj = redis_memstore.get(key, NULL_VALUE_OBJ)
    if (value_obj.unix_expiry_ms != NO_EXPIRY) and (request_recv_time_ms > value_obj.unix_expiry_ms):
        print(f"{key=} expired")
        print(f"request time = {request_recv_time_ms}")
        print(f"expiry time = {value_obj.unix_expiry_ms}")
        del redis_memstore[key]
        value_obj = NULL_VALUE_OBJ
    return value_obj

def set_to_memstore(request_recv_time_ms, key, val, time_to_live_ms=None):
    if time_to_live_ms is not None:
        expiry_time_ms = request_recv_time_ms + time_to_live_ms
    else:
        expiry_time_ms = NO_EXPIRY

    val_type = ValueTypes.get_type(val)
    redis_memstore[key] = ValueObj(val=val, val_dtype=val_type, unix_expiry_ms=expiry_time_ms)


# Streams

def append_stream_event(stream_name:bytes, event_ts_id:StreamTimestampId, val_dict):
    if stream_name not in redis_memstore:
        redis_memstore[stream_name] = ValueObj(val=RedisStream(), unix_expiry_ms=NO_EXPIRY, val_dtype=ValueTypes.STREAM)
    event_ts_id = redis_memstore[stream_name].val.append(event_ts_id, val_dict)
    print(f"Appended {stream_name=} {event_ts_id=}:\n {val_dict}")
    return event_ts_id


def pretty_print_stream(stream_name):
    if stream_name not in redis_memstore:
        raise ValueError(f"Unknown stream: {stream_name}")
    stream_obj = redis_memstore[stream_name].val
    stream_obj.pretty_print()
