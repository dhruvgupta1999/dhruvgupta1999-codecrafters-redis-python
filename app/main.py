import socket  # noqa: F401
import asyncio

from app.redis_serialization_protocol import parse_redis_bytes, serialize_msg, DataTypes


MAX_MSG_LEN = 1000


def create_response(msg):
    """
    create a response and return the redis protocol serialized version of it.

    default -> return PONG
    supports ECHO command.
    """
    result = b''
    if isinstance(msg, str):
        tokens = ' '.split(msg)
        if tokens[0].upper() == 'ECHO':
            print("echo mode...")
            result = ' '.join(tokens[1:])
            result = serialize_msg(result, DataTypes.BULK_STRING)
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

        message = parse_redis_bytes(data)
        print(f"Received from {addr}: {message}")

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
