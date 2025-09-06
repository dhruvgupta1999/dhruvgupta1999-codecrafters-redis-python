from dataclasses import dataclass
from enum import Enum
import socket

from app.redis_serialization_protocol import serialize_msg, SerializedTypes, parse_redis_bytes


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


# Methods on Replica end


def get_master_conn(replica_meta):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(replica_meta.master_addr)
    return client_socket

def verify_master_conn_using_ping(conn):
    """
    Send PING and expect PONG
    """
    conn.sendall(serialize_msg(['PING'], SerializedTypes.ARRAY))
    response = conn.recv(1024)
    err_flag, response = parse_redis_bytes(response)
    print("received PING response from master:")
    print(str(response), type(response))
    assert response == b'PONG'

def send_replconf1(conn, listen_port):
    """
    The first time, it'll be sent like this: REPLCONF listening-port <PORT>
    This is the replica notifying the master of the port it's listening on
    (for monitoring/logging purposes, not for actual propagation).
    """
    conn.sendall(serialize_msg(['REPLCONF', 'listening-port', str(listen_port)], SerializedTypes.ARRAY))

def send_replconf2(conn ):
    """
    The second time, it'll be sent like this: REPLCONF capa psync2
    This is the replica notifying the master of its capabilities ("capa" is short for "capabilities")
    You can safely hardcode these capabilities for now, we won't need to use them in this challenge.
    """
    conn.sendall(serialize_msg(['REPLCONF', 'capa', 'psync2'], SerializedTypes.ARRAY))

def send_psync(conn):
    """
    The PSYNC command is used to synchronize the state of the replica with the master.
    The replica sends this command to the master with two arguments:

    1. **Replication ID of the master**
       - For the first connection, the replication ID is set to `'?'` (a question mark).

    2. **Offset of the master**
       - For the first connection, the offset is set to `-1`.

    This allows the master to determine whether a full or partial resynchronization is needed.
    """
    conn.sendall(serialize_msg(['PSYNC', '?', '-1'], SerializedTypes.ARRAY))




#########################################################################
# Methods on Master end