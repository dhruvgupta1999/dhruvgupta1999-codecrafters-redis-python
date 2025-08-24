import socket  # noqa: F401
import asyncio
from collections import defaultdict, namedtuple
from typing import Iterable
import time

from app.errors import InvalidStreamEventTsId, IncrOnStringValue
from app.memory_management import redis_memstore, get_from_memstore, set_to_memstore, append_stream_event, \
    pretty_print_stream, run_xread, incr_in_memstore
from app.key_value_utils import NO_EXPIRY, ValueObj, NULL_VALUE_OBJ, ValueTypes
from app.redis_serialization_protocol import parse_redis_bytes, serialize_msg, SerializedTypes, OK_SIMPLE_STRING, \
    typecast_as_int, NULL_BULK_STRING, get_resp_array_from_elems
from dataclasses import dataclass

MAX_MSG_LEN = 1000



@dataclass
class Transaction:

    # This stores which clients are currently in transaction_mode,
    # In transaction mode we queue commands until EXEC is called.
    # To enter transaction mode, use the MULTI command.
    clients_in_transaction_mode: set[str]


    # transaction commands queue
    commands_in_q: defaultdict[str,list]

    def discard_transaction(self, addr):
        self.clients_in_transaction_mode.remove(addr)
        del self.commands_in_q[addr]

    def is_in_transaction_mode(self, addr) -> bool:
        return addr in self.clients_in_transaction_mode

TRANSACTION: Transaction = Transaction(clients_in_transaction_mode=set(),
                                       commands_in_q=defaultdict(list))


# This is to wait for xadd by calls like xread.
# Each stream has an asyncio.Condition() to keep track of new xadds.
xadd_conditions: dict[bytes,asyncio.Condition] = {}

def get_unix_time_ms():
    # Get the current Unix timestamp as a floating-point number
    unix_timestamp = time.time()
    unix_ts_ms = int(unix_timestamp * 1000)
    return unix_ts_ms



def parse_xread_input(tokens):
    """
    normal read:
    redis-cli XREAD streams some_key 1526985054069-0

    some_key -> stream name
    1526985054069-0 -> event id  (timestamp and seq_no)


    blocking read:
    redis-cli XREAD block 1000 streams some_key 1526985054069-0
    """
    stream_start_idx = 2
    block_ms = None
    if tokens[1] == b'block':
        block_ms = int(tokens[2].decode())
        stream_start_idx = 4
    # Collects stream names and the start_event_ts_id for each.
    streams = []
    starts = []
    for tok in tokens[stream_start_idx:]:
        if b'-' in tok:
            break
        streams.append(tok)
    for tok in tokens[stream_start_idx + len(streams):]:
        starts.append(tok.decode())
    return block_ms, starts, streams


async def handle_command(msg, addr, request_recv_time_ms=None):
    """
    create a response and return the redis protocol serialized version of it.

    default -> return PONG
    supports ECHO , GET <key>, SET <key> px <time_to_live_ms>.
    """
    # Basic PING-PONG:
    # for commands like echo, msg is in the form of RESP array.
    # otherwise it's just a test ping-pong cmd.
    if not isinstance(msg, Iterable):
        return serialize_msg('PONG', SerializedTypes.SIMPLE_STRING)

    tokens = list(msg)
    first_token = tokens[0].upper()
    print('first token:', first_token)

    # In transaction mode, we only queue the commands.
    # They are executed when EXEC is called.
    if addr in TRANSACTION.clients_in_transaction_mode:
        if first_token == b'EXEC':
            # Execute all queued commands and reset the transaction variables.
            if addr not in TRANSACTION.commands_in_q:
                raise ValueError("Some thing is broken!")

            # further calls to handle_command won't be queued.
            TRANSACTION.clients_in_transaction_mode.remove(addr)
            result = [await handle_command(msg, addr) for msg in TRANSACTION.commands_in_q[addr]]

            # Now delete the commands_in_q[addr]
            del TRANSACTION.commands_in_q[addr]

            return get_resp_array_from_elems(result)
        if first_token == b'DISCARD':
            TRANSACTION.discard_transaction(addr)

        TRANSACTION.commands_in_q[addr].append(msg)
        return serialize_msg('QUEUED', SerializedTypes.SIMPLE_STRING)



    match first_token:
        case b'ECHO':
            result = b' '.join(tokens[1:])
            result = serialize_msg(result, SerializedTypes.BULK_STRING)

        # Redis cache

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
            set_to_memstore(key, val, request_recv_time_ms, time_to_live_ms)
            return OK_SIMPLE_STRING
        case b'TYPE':
            key = tokens[1]
            value_obj = get_from_memstore(key, request_recv_time_ms)
            return serialize_msg(value_obj.val_dtype.value, SerializedTypes.SIMPLE_STRING)
        case b'INCR':
            key = tokens[1]
            try:
                num = incr_in_memstore(key)
            except IncrOnStringValue as e:
                return serialize_msg(str(e), SerializedTypes.ERROR)
            result = serialize_msg(num, SerializedTypes.INTEGER)
            print("INCR result:", result)
            return result


        # Redis Streams

        case b'XADD':
            stream_name = tokens[1]
            event_ts_id = tokens[2].decode()
            print(f"XADD {stream_name=} {event_ts_id=}")
            val_dict = {tokens[i]:tokens[i+1] for i in range(3,len(tokens),2)}
            try:
                event_ts_id = await append_stream_event(stream_name, event_ts_id, val_dict, xadd_conditions)
            except InvalidStreamEventTsId as e:
                return serialize_msg(str(e), SerializedTypes.ERROR)
            pretty_print_stream(stream_name)
            return serialize_msg(event_ts_id, SerializedTypes.BULK_STRING)
        case b'XRANGE':
            stream_name = tokens[1]
            start, end = tokens[2].decode(), tokens[3].decode()
            result = redis_memstore[stream_name].val.xrange(start, end)
            return serialize_msg(result, SerializedTypes.ARRAY)
        case b'XREAD':
            block_ms, starts, streams = parse_xread_input(tokens)
            print(f"{block_ms=}")
            # Query the memstore
            found_smth, results = await run_xread(starts, streams, xadd_conditions, block_ms)
            if not found_smth:
                return NULL_BULK_STRING
            return serialize_msg(results, SerializedTypes.ARRAY)

        # Redis transactions

        case b'MULTI':
            # Start a transaction for the client
            if addr in TRANSACTION.clients_in_transaction_mode:
                return serialize_msg(f"{addr} is already in transaction mode!!", SerializedTypes.ERROR)
            TRANSACTION.clients_in_transaction_mode.add(addr)
            return OK_SIMPLE_STRING
        case b'EXEC':
            # This is only possible if MULTI hasn't been called yet...
            # Return error
            return serialize_msg("ERR EXEC without MULTI", SerializedTypes.ERROR)


        case _:
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
            # When no data, that means EOF was sent.
            # Client has closed connection, so break out of loop.
            print(f"Connection closed by {addr}")
            break
        print(f"Received from {addr}: {data}")
        err_flag, message = parse_redis_bytes(data)
        print(f"Parsed data: {message}")

        # Note: commands are received as redis array. eg: "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
        # After parsing, this will become a list. so message is a list, not str.
        response = await handle_command(message, addr, request_recv_time)
        writer.write(response)
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
