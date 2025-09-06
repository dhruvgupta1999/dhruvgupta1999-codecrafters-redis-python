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
    conn.sendall(serialize_msg(['PING'], SerializedTypes.ARRAY))
    response = conn.recv(1024)
    response = parse_redis_bytes(response)
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








# Methods on Master end