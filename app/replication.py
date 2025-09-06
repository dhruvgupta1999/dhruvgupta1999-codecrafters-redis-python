from dataclasses import dataclass
from enum import Enum
import socket

from app.redis_serialization_protocol import serialize_msg, SerializedTypes


class ReplicationRole(Enum):
    MASTER = 'master'
    SLAVE = 'slave'

@dataclass
class ReplicaMeta:

    role: ReplicationRole

    # Socket addr : (ip, port_id)
    master_addr: tuple[str, int] = None



# Methods on Replica end


def get_master_conn(replica_meta):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(replica_meta.master_addr)
    return client_socket

def send_ping_to_master(conn):
    conn.sendall(serialize_msg(['PING'], SerializedTypes.ARRAY))




# Methods on Master end