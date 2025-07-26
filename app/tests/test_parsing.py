import pytest
from app.redis_serialization_protocol import parse_redis_bytes, SerializedTypes


def test_parse_redis_bytes_simple_string():
    msg = b'+OK\r\n'
    is_error, result = parse_redis_bytes(msg)
    assert not is_error
    assert result == b'OK'

def test_parse_redis_bytes_integer():
    msg = b':1000\r\n'
    is_error, result = parse_redis_bytes(msg)
    assert not is_error
    assert result == 1000

def test_parse_redis_bytes_bulk_string():
    msg = b'$6\r\nfoobar\r\n'
    is_error, result = parse_redis_bytes(msg)
    assert not is_error
    assert result == b'foobar'

def test_parse_redis_bytes_null_bulk_string():
    msg = b'$-1\r\n'
    is_error, result = parse_redis_bytes(msg)
    assert not is_error
    assert result == b''  # Depending on implementation, may be None or b''

def test_parse_redis_bytes_error():
    msg = b'-Error message\r\n'
    is_error, result = parse_redis_bytes(msg)
    assert is_error
    assert result == b'Error message'

def test_parse_redis_bytes_array():
    msg = b'*2\r\n$4\r\nECHO\r\n$9\r\nraspberry\r\n'
    is_error, result = parse_redis_bytes(msg)
    assert not is_error
    assert isinstance(result, list)
    assert result == [b'ECHO', b'raspberry']
