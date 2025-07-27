import socket  # noqa: F401
import asyncio
from typing import Iterable
import time

from app.errors import InvalidStreamEventTsId
from app.memory_management import redis_memstore, get_from_memstore, set_to_memstore, append_stream_event, pretty_print_stream
from app.key_value_utils import NO_EXPIRY, ValueObj, NULL_VALUE_OBJ, ValueTypes
from app.redis_serialization_protocol import parse_redis_bytes, serialize_msg, SerializedTypes, OK_SIMPLE_STRING, typecast_as_int

MAX_MSG_LEN = 1000


def get_unix_time_ms():
    # Get the current Unix timestamp as a floating-point number
    unix_timestamp = time.time()
    unix_ts_ms = int(unix_timestamp * 1000)
    return unix_ts_ms

def create_response(msg, request_recv_time_ms):
    """
    create a response and return the redis protocol serialized version of it.

    default -> return PONG
    supports ECHO , GET <key>, SET <key> px <time_to_live_ms>.
    """
    result = b''
    if isinstance(msg, Iterable):
        tokens = list(msg)
        first_token = tokens[0].upper()
        print('first token:', first_token)
        match first_token:
            case b'ECHO':
                result = b' '.join(tokens[1:])
                result = serialize_msg(result, SerializedTypes.BULK_STRING)
            case b'GET':
                key = tokens[1]
                value_obj = get_from_memstore(key, request_recv_time_ms)
                result = value_obj.get_val_serialized()
                print("GET result:", result)
                return result
            case b'SET':
                key, val= tokens[1], tokens[2]
                time_to_live_ms = None
                if len(tokens) > 4:
                    time_to_live_ms = typecast_as_int(tokens[4])
                set_to_memstore(request_recv_time_ms, key, val, time_to_live_ms)
                return OK_SIMPLE_STRING
            case b'TYPE':
                key = tokens[1]
                value_obj = get_from_memstore(key, request_recv_time_ms)
                return serialize_msg(value_obj.val_dtype.value, SerializedTypes.SIMPLE_STRING)
            case b'XADD':
                stream_name = tokens[1]
                event_ts_id = tokens[2].decode()
                print(f"XADD {stream_name=} {event_ts_id=}")
                val_dict = {tokens[i]:tokens[i+1] for i in range(3,len(tokens),2)}
                try:
                    event_ts_id = append_stream_event(stream_name, event_ts_id, val_dict)
                except InvalidStreamEventTsId as e:
                    return serialize_msg(str(e), SerializedTypes.ERROR)
                pretty_print_stream(stream_name)
                return serialize_msg(event_ts_id, SerializedTypes.BULK_STRING)

            case _:
                result = serialize_msg('PONG', SerializedTypes.SIMPLE_STRING)
    else:
        result = serialize_msg('PONG', SerializedTypes.SIMPLE_STRING)
    print("response created: ", result)
    return result


# This function will be called separately for each client
async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Connected to {addr}")

    while True:
        data = await reader.read(MAX_MSG_LEN)
        # VERY IMP: Note down the time the request was received.
        # TO make sure the expiry time ms is calculated accurately.
        # For SET: request_recv_time + time_to_live is set as value_obj.expiry_time
        # For GET: request_recv_time < value_obj.expiry_time determines whether expired or not.
        # If use some later time rather than request_recv_time, then my expiry will be inaccurate.
        request_recv_time = get_unix_time_ms()
        if not data:
            print(f"Connection closed by {addr}")
            break
        print(f"Received from {addr}: {data}")
        err_flag, message = parse_redis_bytes(data)
        print(f"Parsed data: {message}")

        writer.write(create_response(message, request_recv_time))
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = await asyncio.start_server(handle_client, host='localhost', port=6379)
    print(len(server.sockets))
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
