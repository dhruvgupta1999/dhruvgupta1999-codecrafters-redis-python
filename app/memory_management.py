"""
Logic to manage the memory.
"""
import asyncio
from collections import defaultdict

from app.key_value_utils import NO_EXPIRY, ValueObj, NULL_VALUE_OBJ, ValueTypes
from app.streams_dsa import RedisStream, NUM_DIGITS_TS, NUM_DIGITS_SEQ

redis_memstore: [bytes, ValueObj] = {}


def get_from_memstore(key:bytes, request_recv_time_ms):
    value_obj = redis_memstore.get(key, NULL_VALUE_OBJ)
    if (value_obj.unix_expiry_ms != NO_EXPIRY) and (request_recv_time_ms > value_obj.unix_expiry_ms):
        print(f"{key=} expired")
        print(f"request time = {request_recv_time_ms}")
        print(f"expiry time = {value_obj.unix_expiry_ms}")
        del redis_memstore[key]
        value_obj = NULL_VALUE_OBJ
    return value_obj

def set_to_memstore(key, val, request_recv_time_ms=None, time_to_live_ms=None):
    if time_to_live_ms is not None:
        expiry_time_ms = request_recv_time_ms + time_to_live_ms
    else:
        expiry_time_ms = NO_EXPIRY

    val_type = ValueTypes.get_type(val)
    redis_memstore[key] = ValueObj(val=val, val_dtype=val_type, unix_expiry_ms=expiry_time_ms)


# Increment

def incr_in_memstore(key) -> int:
    value_obj = redis_memstore.get(key, NULL_VALUE_OBJ)
    if value_obj == NULL_VALUE_OBJ:
        # If not exists, set as 1
        set_to_memstore(key, '1')
        return 1

    # right now I am storing everything as string internally!
    value_obj.val = str(int(value_obj.val) + 1)
    return int(value_obj.val)


# Streams

async def append_stream_event(stream_name:bytes, event_ts_id:str, val_dict, xadd_conditions: dict[bytes,asyncio.Condition]):
    if stream_name not in redis_memstore:
        redis_memstore[stream_name] = ValueObj(val=RedisStream(), unix_expiry_ms=NO_EXPIRY, val_dtype=ValueTypes.STREAM)
        xadd_conditions[stream_name] = asyncio.Condition()
    event_ts_id = redis_memstore[stream_name].val.append(event_ts_id, val_dict)
    async with xadd_conditions[stream_name]:
        xadd_conditions[stream_name].notify_all()
    print(f"Appended {stream_name=} {event_ts_id=}:\n {val_dict}")
    return event_ts_id


def pretty_print_stream(stream_name):
    if stream_name not in redis_memstore:
        raise ValueError(f"Unknown stream: {stream_name}")
    stream_obj = redis_memstore[stream_name].val
    stream_obj.pretty_print()

async def run_xread(starts, streams, xadd_conditions: dict[bytes,asyncio.Condition], timeout_ms):

    # 1. first check if something is already present.
    found_smth, results = xread_stream_storage(starts, streams)

    # If timeout is not set, it's non-blocking.
    if found_smth or timeout_ms is None:
        return found_smth, results


    # If not found, wait for condition.notify() from XADD.
    # Since Redis is single threaded, there is no race condition to worry about.
    print(f"xread: waiting on {streams}")
    wait_tasks = []

    for stream, start in zip(streams, starts):
        cond = xadd_conditions[stream]

        async def wait_on(cond=cond):  # Use default arg to capture `cond`
            async with cond:
                await cond.wait()
            return stream, start  # return the stream that woke us

        wait_tasks.append(asyncio.create_task(wait_on()))

    if timeout_ms == 0:
        # Then keep blocking till some input is received. (no timeout).
        done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
    else:
        done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED, timeout=timeout_ms/1000)

    if not done:
        return False, []

    for t in pending:
        t.cancel()

    # Now xread only the ones that are from done to avoid unnecessary computation on others.
    completed_stream_starts = [d.result() for d in done]
    completed_streams, completed_starts = list(zip(*completed_stream_starts))
    found_smth, results = xread_stream_storage(completed_starts, completed_streams)
    return found_smth, results


def xread_stream_storage(starts, streams):
    found_smth = False
    results = []
    for stream, start in zip(streams, starts):
        result = redis_memstore[stream].val.xread(start)
        if result:
            found_smth = True
        results.append([stream, result])
    return found_smth, results