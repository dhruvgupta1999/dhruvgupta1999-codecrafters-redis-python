import pytest


from app.streams_dsa import RedisStream

def test_xadd_with_no_seq_num():
    r = RedisStream()
    result = r.append('0-*', {1:1})
    assert result == ('0-1')
    result = r.append('1-*', {2:1})
    assert result == ('1-0')
    result = r.append('1-*', {3: 1})
    assert result == ('1-1')


def test_xrange():
    r = RedisStream()
    result = r.append('0-*', {1: 1})
    xlist = r.xrange('0', '1')
    assert xlist[0][0] == '0-1'
    assert xlist[0][1] == [1,1]

    result = r.append('1-*', {2: 1})
    xlist = r.xrange('1', '1')
    assert xlist[0][0] == '1-0'
    assert xlist[0][1] == [2, 1]

    result = r.append('1-*', {3: 1})
    xlist = r.xrange('0', '1')
    assert xlist[0][0] == '0-1'
    assert xlist[0][1] == [1, 1]

    assert xlist[2][0] == '1-1'
    assert xlist[2][1] == [3, 1]

def test_xrange_with_seq_limit():

    r = RedisStream()
    result = r.append('0-*', {1: 1})
    xlist = r.xrange('0', '1')

    result = r.append('1-*', {2: 1})
    xlist = r.xrange('1', '1')

    result = r.append('1-*', {3: 1})
    xlist = r.xrange('0', '1-0')
    assert(len(xlist)) == 2
