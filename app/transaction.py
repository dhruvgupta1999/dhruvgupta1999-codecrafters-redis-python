from collections import defaultdict
from dataclasses import dataclass

from app.main import handle_command
from app.redis_serialization_protocol import get_resp_array_from_elems, OK_SIMPLE_STRING, serialize_msg, SerializedTypes


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


async def handle_command_when_in_transaction(addr, first_token, msg):
    if first_token == b'EXEC':
        # further calls to handle_command won't be queued.
        TRANSACTION.clients_in_transaction_mode.remove(addr)
        result = [await handle_command(msg, addr) for msg in TRANSACTION.commands_in_q[addr]]

        # Now delete the commands_in_q[addr]
        del TRANSACTION.commands_in_q[addr]

        return get_resp_array_from_elems(result)
    if first_token == b'DISCARD':
        TRANSACTION.discard_transaction(addr)
        return OK_SIMPLE_STRING

    TRANSACTION.commands_in_q[addr].append(msg)
    return serialize_msg('QUEUED', SerializedTypes.SIMPLE_STRING)
