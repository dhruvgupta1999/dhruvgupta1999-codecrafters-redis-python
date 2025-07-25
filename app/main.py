import socket  # noqa: F401
import asyncio

# This function will be called separately for each client
async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Connected to {addr}")

    while True:
        data = await reader.read(10)
        if not data:
            print(f"Connection closed by {addr}")
            break

        message = data.decode().strip()
        print(f"Received from {addr}: {message}")

        response = f"+PONG\r\n"
        writer.write(response.encode())
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = await asyncio.start_server(handle_client, host='127.0.0.1', port=8888)
    print(len(server.sockets))
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
