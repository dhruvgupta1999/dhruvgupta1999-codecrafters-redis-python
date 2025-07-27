import pytest


from app.streams_dsa import RedisStream

def test_xadd_with_no_seq_no():
    r = RedisStream()
    result = r.append('0-*', {1:1})
    assert result == ('0-1')
    result = r.append('1-*', {2:1})
    assert result == ('1-0')
    result = r.append('1-*', {3: 1})
    assert result == ('1-1')
