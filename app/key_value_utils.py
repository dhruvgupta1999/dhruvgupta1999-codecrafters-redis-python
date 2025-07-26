from dataclasses import dataclass
from enum import Enum
from typing import Any

from app.redis_serialization_protocol import NULL_BULK_STRING, SerializedTypes

NO_EXPIRY = -1

class ValueTypes(Enum):
    STRING=b'string'
    STREAM=b'stream'
    NONE=b'none'

    @classmethod
    def get_type(cls, val):
        if isinstance(val, str) or isinstance(val, bytes):
            return cls.STRING
        raise NotImplementedError()

    def get_serialized_dtype(self):
        serialized_data_type = None
        # Even Null is serialized as a bulk string with -1 as the length.
        if self in (ValueTypes.STRING, ValueTypes.NONE):
            serialized_data_type = SerializedTypes.BULK_STRING
        else:
            raise NotImplementedError(f"ERROR: {self=} serialization type unknown")
        return serialized_data_type

@dataclass
class ValueObj:
    val: Any
    unix_expiry_ms: int
    val_dtype: ValueTypes



NULL_VALUE_OBJ = ValueObj(val=NULL_BULK_STRING, unix_expiry_ms=NO_EXPIRY, val_dtype=ValueTypes.NONE)


