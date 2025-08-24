from dataclasses import dataclass
from enum import Enum


class ReplicationRole(Enum):
    MASTER = 'master'
    SLAVE = 'slave'

@dataclass
class ReplicationMeta:

    role: ReplicationRole

    # Socket addr : (ip, port_id)
    master_addr: tuple[str, int] = None