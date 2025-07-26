"""
| Type          | Prefix | Format                                                     |
| ------------- | ------ | ---------------------------------------------------------- |
| Simple String | `+`    | `+OK\r\n`                                                  |
| Error         | `-`    | `-Error message\r\n`                                       |
| Integer       | `:`    | `:1000\r\n`                                                |
| Bulk String   | `$`    | `$6\r\nfoobar\r\n` (or `$-1\r\n` for null)                 |
| Array         | `*`    | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` (or `*-1\r\n` for null) |

clrs -> '\r\n' 

bulk string -> Bulk strings explicitly specify length, so they can include binary data, \r\n, or even null characters.

"""
from enum import Enum
from typing import Any, Iterable

CLRS = b'\r\n'
NULL_BULK_STRING = b'$-1\r\n'
OK_SIMPLE_STRING = b'+OK\r\n'

class SerializedTypes(Enum):
    SIMPLE_STRING=b'+'
    ERROR = b'-'
    INTEGER = b':'
    BULK_STRING = b'$'
    ARRAY = b'*'

# All the functions take in the msg, start_index.
# They only parse the prefix of the msg then return that parsed prefix and the index just after that parsed prefix.


def parse_simple_str(msg, start_index):
    msg_after_start = msg[start_index:]
    end_idx = msg_after_start.find(CLRS)
    return msg_after_start[1:end_idx], end_idx + 2 + start_index

def parse_int(msg, start_index):
    msg_after_start = msg[start_index:]
    end_idx = msg_after_start.find(CLRS)
    print('start_index+1', start_index+1)
    print('end_idx', end_idx)
    return int(msg_after_start[1:end_idx].decode()), end_idx + 2 + start_index

def parse_bulk_str(msg, start_index) -> tuple[bytes, int]:
    """
    Can have arbitrary binary data, do not decode.
    """
    data_len, new_start_idx = parse_int(msg, start_index)
    bulk_str = msg[new_start_idx: new_start_idx + data_len]
    return bulk_str, new_start_idx + data_len + 2

def parse_array(msg, start_index):
    arr_len, new_start_idx = parse_int(msg, start_index)
    result = []
    index = new_start_idx
    for i in range(arr_len):
        e, index = parse_primitive(msg, index)
        result.append(e)
    return result, index

def parse_primitive(msg, start_index):
    data_type = SerializedTypes(msg[start_index:start_index + 1])
    match data_type:
        case SerializedTypes.SIMPLE_STRING:
            return parse_simple_str(msg, start_index)
        case SerializedTypes.INTEGER:
            return parse_int(msg, start_index)
        case SerializedTypes.BULK_STRING:
            return parse_bulk_str(msg, start_index)
        case SerializedTypes.ARRAY:
            return parse_array(msg, start_index)
        case _:
            raise ValueError(f"Unsupported data type: {data_type}")

def parse_redis_bytes(msg) -> tuple[bool, Any]:
    """
    return is_error, msg
    """
    index = 0
    n = len(msg)
    data_type = SerializedTypes(msg[index:index + 1])
    if data_type == SerializedTypes.ERROR:
        # assuming error comes only by itself, without any other data types.
        err_msg = msg[1:-2]
        return True, err_msg
    else:
        return False, parse_primitive(msg, index)[0]



##################################################################################################

def typecast_as_int(token) -> int:
    if isinstance(token, str):
        return int(token)
    if isinstance(token, bytes):
        return int(token.decode())
    if isinstance(token, int):
        return token

def typecast_as_bytes(msg) -> bytes:
    if isinstance(msg, bytes):
        return msg
    if isinstance(msg, int):
        return str(msg).encode()
    if isinstance(msg, str):
        return msg.encode()

def serialize_msg(msg: Any, data_type: SerializedTypes):
    match data_type:
        case SerializedTypes.SIMPLE_STRING:
            msg = typecast_as_bytes(msg)
            return b'+' + msg + CLRS
        case SerializedTypes.INTEGER:
            msg = typecast_as_bytes(msg)
            return b':' + msg + CLRS
        case SerializedTypes.BULK_STRING:
            msg = typecast_as_bytes(msg)
            data_len_as_bytes = typecast_as_bytes(len(msg))
            return b'$' + data_len_as_bytes + CLRS + msg + CLRS
        case SerializedTypes.ARRAY:
            raise NotImplementedError()
        case _:
            raise ValueError(f"Unsupported data type: {data_type}")

