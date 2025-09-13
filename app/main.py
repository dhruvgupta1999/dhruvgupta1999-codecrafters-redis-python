import socket  # noqa: F401
import asyncio
from collections import defaultdict
from typing import Iterable
import time
import argparse

from app.errors import InvalidStreamEventTsId, IncrOnStringValue
from app.memory_management import redis_memstore, get_from_memstore, set_to_memstore, append_stream_event, \
    pretty_print_stream, run_xread, incr_in_memstore
from app.rdb import EMPTY_RDB_HEX
from app.redis_serialization_protocol import parse_redis_bytes, serialize_msg, SerializedTypes, OK_SIMPLE_STRING, \
    typecast_as_int, NULL_BULK_STRING, get_resp_array_from_elems, CLRS

from app.redis_streams import parse_xread_input
from app.replication import get_replication_info, _init_master, _init_replica, get_master_replid, add_replica_conn, \
    propagate_to_replica_if_write_cmd
from app.transaction import Transaction


####################################################################################################
# GLOBALS

MAX_MSG_LEN = 1000

TRANSACTION = Transaction(clients_in_transaction_mode=set(), commands_in_q=defaultdict(list))

# For use by REDIS STREAM
# This is to wait for xadd by calls like xread.
# Each stream has an asyncio.Condition() to keep track of new xadds.
xadd_conditions: dict[bytes,asyncio.Condition] = {}


####################################################################################################
# Utils

def get_unix_time_ms():
    # Get the current Unix timestamp as a floating-point number
    unix_timestamp = time.time()
    unix_ts_ms = int(unix_timestamp * 1000)
    return unix_ts_ms

####################################################################################################
# Handle command

async def handle_command_when_in_transaction(addr, first_token, msg):
    if first_token == b'EXEC':
        # further calls to handle_command won't be queued.
        TRANSACTION.clients_in_transaction_mode.remove(addr)
        result = [await handle_command(msg, addr, write_conn) for msg in TRANSACTION.commands_in_q[addr]]

        # Now delete the commands_in_q[addr]
        del TRANSACTION.commands_in_q[addr]

        return get_resp_array_from_elems(result)
    if first_token == b'DISCARD':
        TRANSACTION.discard_transaction(addr)
        return OK_SIMPLE_STRING

    TRANSACTION.commands_in_q[addr].append(msg)
    return serialize_msg('QUEUED', SerializedTypes.SIMPLE_STRING)

async def handle_command(msg, addr, write_conn, request_recv_time_ms=None):
    """
    create a response and return the redis protocol serialized version of it.

    Generally, the response is in bytes (the msg to send over network).
    However, for any reason if we have to send multiple messages in one go, then response can be a tuple of bytes.


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
        return await handle_command_when_in_transaction(addr, first_token, msg)


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

        # not adding replication support for these.
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
        case b'DISCARD':
            # This is only possibel is MULTI hasn't been called yet... (because transaction handling is done above)
            return serialize_msg("ERR DISCARD without MULTI", SerializedTypes.ERROR)


        # Redis Replication

        case b'INFO':
            # Return whether I am a master or slave and some extra details.
            info_map = get_replication_info()
            return serialize_msg(info_map,  SerializedTypes.BULK_STRING)

        case b'REPLCONF':
            # This command is used by the replica to send its config.
            # The current instance is receiving this command, and therefore is the master.

            # Add the replica write_conn to list of replicas that master handles.
            add_replica_conn(write_conn)
            return OK_SIMPLE_STRING

        case b'PSYNC':
            # This command is used by the replica to send its current status (offset till which it knows existing data).
            # The current instance is receiving this command, and therefore is the master.

            # Send fullresync, if master thinks that replica needs to re-create its cache from scratch.
            resp1 = serialize_msg(f"FULLRESYNC {get_master_replid()} 0", SerializedTypes.SIMPLE_STRING)
            # Send empty RDB file (shortcut only for this challenge).
            # Basically this means that you are assuming master has no data at the moment.
            # RDB format: $<length_of_file>\r\n<binary_contents_of_file>
            empty_rdb_bytes = bytes.fromhex(EMPTY_RDB_HEX)
            resp2 = b'$' + str(len(empty_rdb_bytes)).encode() + CLRS + empty_rdb_bytes
            return resp1, resp2


        case _:
            result = serialize_msg('PONG', SerializedTypes.SIMPLE_STRING)

    print("response created: ", result)
    return result


####################################################################################################
# Basic Server boilerplate


# This function will be called separately for each client
async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Connected to {addr}")

    while True:
        data = await reader.read(MAX_MSG_LEN)
        # VERY IMP: Note down the time the request was received (for TTL support)
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
        response = await handle_command(message, addr, writer, request_recv_time)

        await propagate_to_replica_if_write_cmd(data)

        # Generally, the response is in bytes (the msg to send over network).
        # However, for any reason if we have to send multiple messages in one go, then response can be a list of bytes.
        if isinstance(response, tuple):
            for sub_r in response:
                writer.write(sub_r)
        elif isinstance(response, bytes):
            writer.write(response)
        else:
            raise ValueError(f"Invalid value, can't send {response} as response")

        # A Replication Note.
        # When we await writer.drain() to send RDB snapshot data to our replica as a response to PSYNC,
        # we give up control to some other async task.
        # Now if that async task sets some value to the master-cache (SET FOO 10),
        # we need to propagate that cmd to our replica as well.
        # The challenge is buffering those propagated commands, until writer.drain(rdb_file) completes.
        # Can use asyncio.Condition.notify_all() just after writer.drain(rdb_file_data).
        # On the green signal, the task that is buffering the commands will flush them to the replica.
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    args = get_args()
    if not args.port:
        args.port = 6379
    print(f"Server will run on port: {args.port}")

    if args.replicaof:
        # This instance is a replica.
        master_ip, master_port = args.replicaof.split(' ')
        port = args.port
        master_addr = master_ip, int(master_port)
        _init_replica(master_addr, port)
    else:
        # This instance is the master.
        #
        await _init_master()

    server = await asyncio.start_server(handle_client, host='localhost', port=args.port)
    print(len(server.sockets))
    async with server:
        await server.serve_forever()


def get_args():
    """
    eg:
    ./your_program.sh --port 6380 --replicaof "localhost 6379"
    """
    parser = argparse.ArgumentParser(description="Example server app")
    parser.add_argument(
        "--port",
        type=int,
        required=False,
        help="Port number to run the server on"
    )
    parser.add_argument(
        "--replicaof",
        type=str,
        required=False,
        help="To make this program a replica of given master"
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    asyncio.run(main())

