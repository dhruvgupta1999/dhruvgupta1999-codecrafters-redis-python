"""
Logic to manage the memory.
"""
from dataclasses import dataclass
from typing import Any

from app.redis_serialization_protocol import NULL_BULK_STRING, DataTypes

NO_EXPIRY = -1

@dataclass()
class ValueObj:
    val: Any
    unix_expiry_ms: int
    val_dtype: type

NULL_VALUE_OBJ = ValueObj(val=NULL_BULK_STRING, unix_expiry_ms=NO_EXPIRY, val_dtype=bytes)

redis_memstore: [bytes, ValueObj] = {}

