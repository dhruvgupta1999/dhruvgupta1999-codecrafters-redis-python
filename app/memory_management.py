"""
Logic to manage the memory.
"""
from dataclasses import dataclass
from typing import Any

NO_EXPIRY = -1

@dataclass()
class ValueObj:
    val: Any
    unix_expiry_ms: int
    val_dtype: type

redis_memstore: [bytes, ValueObj] = {}

