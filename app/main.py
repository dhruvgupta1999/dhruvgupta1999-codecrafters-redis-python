import socket  # noqa: F401
import asyncio
from typing import Iterable
import time



from app.memory_management import redis_memstore, ValueObj, NO_EXPIRY
from app.redis_serialization_protocol import parse_redis_bytes, serialize_msg, DataTypes, NULL_BULK_STRING, \
    OK_SIMPLE_STRING, get_serialized_dtype, typecast_as_int

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
                result = serialize_msg(result, DataTypes.BULK_STRING)
            case b'GET':
                key = tokens[1]
                value_obj = redis_memstore.get(key, NULL_BULK_STRING)
                if (value_obj.unix_expiry_ms != NO_EXPIRY) and (request_recv_time_ms > value_obj.unix_expiry_ms):
                    print(f"{key=} expired")
                    print(f"request time = {request_recv_time_ms}")
                    print(f"expiry time = {value_obj.unix_expiry_ms}")
                    del redis_memstore[key]
                    return NULL_BULK_STRING

                serialized_data_type = get_serialized_dtype(value_obj.val_dtype)
                return serialize_msg(msg, serialized_data_type)
            case b'SET':
                key, val = tokens[1], tokens[2]
                expire_ms = typecast_as_int(tokens[4]) if tokens[4] else NO_EXPIRY
                expiry_time_ms = request_recv_time_ms + expire_ms
                redis_memstore[key] = ValueObj(val=val, val_dtype=type(tokens[2]), unix_expiry_ms=expiry_time_ms)
                return OK_SIMPLE_STRING
            case _:
                result = serialize_msg('PONG', DataTypes.SIMPLE_STRING)
    else:
        result = serialize_msg('PONG', DataTypes.SIMPLE_STRING)
    print("response created: ", result)
    return result


# This function will be called separately for each client
async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Connected to {addr}")

    while True:
        data = await reader.read(MAX_MSG_LEN)
        request_recv_time = get_unix_time_ms()
        if not data:
            print(f"Connection closed by {addr}")
            break
        print(f"Received from {addr}: {data}")
        err_flag, message = parse_redis_bytes(data, request_recv_time)
        print(f"Parsed data: {message}")

        response = f"+PONG\r\n"
        writer.write(create_response(message))
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
