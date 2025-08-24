from collections import defaultdict
from dataclasses import dataclass


@dataclass
class Transaction:

    # This stores which clients are currently in transaction_mode,
    # In transaction mode we queue commands until EXEC is called.
    # To enter transaction mode, use the MULTI command.
    clients_in_transaction_mode: set[str]


    # transaction commands queue (every addr is mapped to the queued commands).
    commands_in_q: defaultdict[str,list]

    def discard_transaction(self, addr):
        self.clients_in_transaction_mode.remove(addr)
        del self.commands_in_q[addr]

    def is_in_transaction_mode(self, addr) -> bool:
        return addr in self.clients_in_transaction_mode


