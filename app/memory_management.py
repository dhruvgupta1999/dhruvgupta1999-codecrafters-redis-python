"""
Logic to manage the memory.
"""
from app.key_value_utils import NO_EXPIRY, ValueObj, NULL_VALUE_OBJ

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

