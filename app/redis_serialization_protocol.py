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
from typing import Any 


CLRS = b'\r\n'


class DataTypes(Enum):
    SIMPLE_STRING=b'+'
    ERROR = b'-'
    INTEGER = b':'
    BULK_STRING = b'$'
    ARRAY = b'*'


# All the functions take in the msg, start_index.
# They only parse the prefix of the msg then return that parsed prefix and the index just after that parsed prefix.


def parse_simple_str(msg, start_index):
    end_idx = msg.find(CLRS)
    return msg[start_index+1, end_idx], end_idx + 2

def parse_int(msg, start_index):
    end_idx = msg.find(CLRS)
    return int(msg[start_index+1, end_idx].decode()), end_idx + 2

def parse_bulk_str(msg, start_index) -> tuple[bytes, int]:
    """
    Can have arbitrary binary data, do not decode.
    """
    data_len, new_start_idx = parse_int(msg, start_index+1)
    bulk_str = msg[new_start_idx, new_start_idx + data_len]
    return bulk_str, new_start_idx + data_len + 2

def parse_array(msg, start_index):
    arr_len, new_start_idx = parse_int(msg, start_index+1)
    result = []
    index = new_start_idx
    for i in range(arr_len):
        e, index = parse_primitive(msg, index)
        result.append(e)
    return result

def parse_primitive(msg, start_index):
    data_type = msg[start_index:start_index+1]
    match data_type:
        case DataTypes.SIMPLE_STRING:
            return parse_simple_str(msg, start_index)
        case DataTypes.INTEGER:
            return parse_int(msg, start_index)
        case DataTypes.BULK_STRING:
            return parse_bulk_str(msg, start_index)
        case DataTypes.ARRAY:
            return parse_array(msg, start_index)
        case _:
            raise ValueError(f"Unsupported data type: {data_type}")

def parse_redis_bytes(msg) -> tuple[bool, Any]:
    """
    return is_error, msg
    """
    index = 0
    n = len(msg)
    data_type = DataTypes(msg[index:index+1])
    if data_type == DataTypes.ERROR:
        # assuming error comes only by itself, without any other data types.
        err_msg = msg[1:-2]
        return True, err_msg
    else:
        return False, parse_primitive(msg, index)



##################################################################################################


def serialize_msg(msg: Any, data_type: DataTypes):
    match data_type:
        case DataTypes.SIMPLE_STRING:
            msg = str(msg)
            msg = msg.encode()
            return b'+' + msg + CLRS
        case DataTypes.INTEGER:
            msg = str(msg).encode()
            return 
        case DataTypes.BULK_STRING:
            raise NotImplementedError()
        case DataTypes.ARRAY:
            raise NotImplementedError()
        case _:
            raise ValueError(f"Unsupported data type: {data_type}")
