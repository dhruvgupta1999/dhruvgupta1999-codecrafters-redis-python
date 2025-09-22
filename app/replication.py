import asyncio
from dataclasses import dataclass
from enum import Enum
import socket

from app.errors import IncrOnStringValue
from app.memory_management import set_to_memstore, incr_in_memstore
from app.redis_serialization_protocol import serialize_msg, SerializedTypes, parse_redis_bytes, OK_SIMPLE_STRING, \
    parse_redis_bytes_multiple_cmd, CLRS

MAX_MSG_LEN = 1000

class ReplicationRole(Enum):
    MASTER = 'master'
    SLAVE = 'slave'

@dataclass
class ReplicaMeta:
    """
    Replica fields
    """
    role: ReplicationRole

    # Socket addr : (ip, port_id)
    master_addr: tuple[str, int] = None

@dataclass
class MasterMeta:
    """
    Master fields
    """
    role: ReplicationRole
    master_replid: str
    master_repl_offset: int


########################################################################################
# Replication Meta

_replication_meta = None

# Keep note of master conn reader and writer
# master will propagate command on this connection itself.
_master_conn_reader = None
_master_conn_writer = None

# num_bytes processed by replica that were sent to it by master.
# Master occasionally pings replica to check whether the num_bytes is as expected. (using "REPLCONF ACK *"
# to which replica replies "REPLCONF ACK <num_bytes>").
num_bytes_processed = 0


def get_replication_info():
    info_map = {}
    print('info map', info_map)
    if _replication_meta.role == ReplicationRole.MASTER:
        info_map['role'] = _replication_meta.role.value
        info_map['master_repl_offset'] = _replication_meta.master_repl_offset
        info_map['master_replid'] = _replication_meta.master_replid
    else:
        info_map['role'] = _replication_meta.role.value
    return info_map


async def _init_master():
    global _replication_meta
    # 40 char alphanumeric str
    master_replid = 'a' * 40
    master_repl_offset = 0
    _replication_meta = MasterMeta(**{'role': ReplicationRole.MASTER,
                                     'master_replid': master_replid,
                                     'master_repl_offset': master_repl_offset})


async def _init_replica(master_addr, port):
    global _replication_meta
    _replication_meta = ReplicaMeta(role=ReplicationRole.SLAVE,
                                    master_addr=master_addr)
    await async_master_conn(_replication_meta)
    # send ping and check for pong
    await verify_master_conn_using_ping()
    # The replica sends REPLCONF twice to the master (replica config)
    # we send the listening port for logging, and then the capabilities of the replica.
    await send_replconf1(port)
    await send_replconf2()
    await send_psync()
    # Now master returns whether I need to do FULLRESYNC or partial sync.
    # It also sends an RDB file.
    sync_msg = await _master_conn_reader.read(MAX_MSG_LEN)
    print(str(sync_msg))
    # msg format: FULLRESYNC <master_id> <offset>\r\n<length>\r\n<rdb_snap_bytes><any_other_commands_might_also_be_here>
    # The tcp packet sent by master MAY have extra commands just after the FULLRESYNC like SET/INCR/REPLCONF.
    # So we need to parse the exact byte where FULLRESYNC part ends and next part starts.
    remainder_bytes = get_bytes_after_fullresync(sync_msg)
    if remainder_bytes:
        await handle_propagated_cmds(remainder_bytes)

    # Now listen for propagated commands like SET/INCR
    # Start background listener.
    # IMPORTANT NOTE: We can't directly do "await listen_on_master()" else we will be blocked here.
    # We need to run this in background, so that we can continue and start the server to which clients can connect.
    asyncio.create_task(listen_to_master())

def get_bytes_after_fullresync(resync_msg):
    """

    """
    SPACE = b' '
    tokens = resync_msg.split(SPACE)
    # ignore first two tokens (FULLRESYNC <master_id>)
    # third token has rdb file followed immediately by the next command (without space).
    # third token: <offset>\r\n<length>\r\n<rdb_snapshot_bytes><any_other_commands_might_also_be_here>
    third_token = tokens[2]
    third_token_split = third_token.split(CLRS)
    rdb_len = third_token_split[1]
    rdb_len = int.from_bytes(rdb_len)

    bytes_after_rdb_len = CLRS.join(third_token_split[2:])
    bytes_after_rdb = bytes_after_rdb_len[rdb_len:]
    print('bytes after rdb', bytes_after_rdb)

    # keep bytes after RDB from third_token and other tokens as is.
    result = bytes_after_rdb
    if tokens[3:]:
        result += b' ' + b' '.join(tokens[3:])
    print('bytes after FULLRESYNC', result)
    return result





def get_replication_role():
    return _replication_meta.role

def is_master():
    return _replication_meta.role == ReplicationRole.MASTER

########################################################################################
# Methods on Replica end


async def risky_recv():
    """
    recv(1024) does not guarantee that you’ll get the full message in one call.

    TCP is a stream protocol, not message-oriented — the data may arrive split into multiple chunks or coalesced into one.
    You might get only part of the message on the first recv.


    Normally, we should use some delimiter in the response.
    Or Send a length prefix.
    """
    return await _master_conn_reader.read(MAX_MSG_LEN)

async def write_to_master(data: bytes):
    _master_conn_writer.write(data)
    await _master_conn_writer.drain()

def get_master_conn(replica_meta):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(replica_meta.master_addr)
    return client_socket

async def async_master_conn(replica_meta):
    global _master_conn_reader, _master_conn_writer
    reader, writer = await asyncio.open_connection(
        host=replica_meta.master_addr[0],
        port=replica_meta.master_addr[1]
    )
    _master_conn_reader = reader
    _master_conn_writer = writer

async def verify_master_conn_using_ping():
    """
    Send PING and expect PONG
    """
    await write_to_master(serialize_msg(['PING'], SerializedTypes.ARRAY))
    response = await risky_recv()
    err_flag, response = parse_redis_bytes(response)
    print("received PING response from master:")
    print(str(response), type(response))
    assert response == b'PONG'

async def send_replconf1(listen_port):
    """
    The first time, it'll be sent like this: REPLCONF listening-port <PORT>
    This is the replica notifying the master of the port it's listening on
    (for monitoring/logging purposes, not for actual propagation).
    """
    await write_to_master(serialize_msg(['REPLCONF', 'listening-port', str(listen_port)], SerializedTypes.ARRAY))
    response = await risky_recv()
    err_flag, response = parse_redis_bytes(response)
    assert response == b'OK'

async def send_replconf2():
    """
    The second time, it'll be sent like this: REPLCONF capa psync2
    This is the replica notifying the master of its capabilities ("capa" is short for "capabilities")
    You can safely hardcode these capabilities for now, we won't need to use them in this challenge.
    """
    await write_to_master(serialize_msg(['REPLCONF', 'capa', 'psync2'], SerializedTypes.ARRAY))
    response = await risky_recv()
    err_flag, response = parse_redis_bytes(response)
    assert response == b'OK'

async def send_psync():
    """
    The PSYNC command is used to synchronize the state of the replica with the master.
    The replica sends this command to the master with two arguments:

    1. **Replication ID of the master**
       - For the first connection, the replication ID is set to `'?'` (a question mark).

    2. **Offset of the master**
       - For the first connection, the offset is set to `-1`.

    This allows the master to determine whether a full or partial resynchronization is needed.
    """
    await write_to_master(serialize_msg(['PSYNC', '?', '-1'], SerializedTypes.ARRAY))


async def listen_to_master():

    while True:
        data = await _master_conn_reader.read(MAX_MSG_LEN)
        if not data:
            # When no data, that means EOF was sent.
            # Client has closed connection, so break out of loop.
            print(f"Connection closed by master")
            break
        print('data recvd from master', data)
        await handle_propagated_cmds(data)




async def handle_propagated_cmds(data: bytes):
    global num_bytes_processed
    cmds = parse_redis_bytes_multiple_cmd(data)
    print("cmds recvd from master:\n", cmds)
    for message in cmds:
        # for simplicity I am handling only simple SET and INCR, no TTL nothing (unless later challenges require it).
        # This is good enough for POC.
        tokens = list(message)
        first_token = tokens[0].upper()
        print('first token:', first_token)
        match first_token:
            case b'SET':
                key, val = tokens[1], tokens[2]
                set_to_memstore(key, val)
            case b'INCR':
                key = tokens[1]
                num = incr_in_memstore(key)
            case b'REPLCONF':
                # This is the master's way of checking whether replica is in sync. (REPLCONF GETACK *)
                # the replica has to return the offset of the num_bytes it has processed.
                await write_to_master(serialize_msg(["replconf", "ACK", num_bytes_processed], SerializedTypes.ARRAY))

    num_bytes_processed += len(data)


#########################################################################
# Methods on Master end

# Stores replicas connected to this master.
_my_replicas = set()

def get_master_replid():
    return _replication_meta.master_replid


def add_replica_conn(write_conn):
    _my_replicas.add(write_conn)
    print("num replicas connected to master:", len(_my_replicas))


async def propagate_to_replica_if_write_cmd(data: bytes):
    # Only master can propagate commands.
    if not is_master():
        return
    CMDS_TO_PROPAGATE = [b'SET', b'INCR']
    err_flag, message = parse_redis_bytes(data)
    tokens = list(message)
    first_token = tokens[0].upper()
    if first_token in CMDS_TO_PROPAGATE:
        for w in _my_replicas:
            w.write(data)

        # use asyncio.gather to simultaneously drain all replica writers.
        await asyncio.gather(
            *(w.drain() for w in _my_replicas)
        )

