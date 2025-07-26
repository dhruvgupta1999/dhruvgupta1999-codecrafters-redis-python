import socket  # noqa: F401
import asyncio
from typing import Iterable

from app.redis_serialization_protocol import parse_redis_bytes, serialize_msg, DataTypes, NULL_BULK_STRING, \
    OK_SIMPLE_STRING

MAX_MSG_LEN = 1000


temp_data = {}

def create_response(msg):
    """
    create a response and return the redis protocol serialized version of it.

    default -> return PONG
    supports ECHO command.
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
                msg, data_type = temp_data.get(tokens[1], NULL_BULK_STRING)
                serialized_data_type = None
                if isinstance(data_type, str) or isinstance(data_type, bytes):
                    serialized_data_type = DataTypes.BULK_STRING
                elif isinstance(data_type, Iterable):
                    serialized_data_type = DataTypes.ARRAY
                elif isinstance(data_type, int):
                    serialized_data_type = DataTypes.INTEGER
                else:
                    raise ValueError(f"ERROR: {data_type=} unknown")

                return serialize_msg(msg, serialized_data_type)
            case b'SET':
                temp_data[tokens[1]] = (tokens[2], type(tokens[2]))
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
        if not data:
            print(f"Connection closed by {addr}")
            break
        print(f"Received from {addr}: {data}")
        err_flag, message = parse_redis_bytes(data)
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
