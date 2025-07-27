import pytest

from app.redis_serialization_protocol import serialize_msg, SerializedTypes


def test_serialize_simple_arr():
    result = serialize_msg([1,2], SerializedTypes.ARRAY)
    assert result == b'*2\r\n$1\r\n1\r\n$1\r\n2\r\n'

def test_serialize_nested_arr():
    result = serialize_msg([1, [2,3]], SerializedTypes.ARRAY)
    assert result == b'*2\r\n$1\r\n1\r\n*2\r\n$1\r\n2\r\n$1\r\n3\r\n'
