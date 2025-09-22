import pytest

from app.replication import get_bytes_after_fullresync

CLRS = b"\r\n"

def make_resync_msg(master_id=b"xyz", offset=b"12345", rdb_bytes=b"ABCDEF", extra_cmds=b"PING\r\n+OK\r\n"):
    """
    Helper to build a fake FULLRESYNC message.
    Format:
    FULLRESYNC <master_id> <offset>\r\n<length>\r\n<rdb_snapshot_bytes><any_other_commands>
    """
    length = str(len(rdb_bytes)).encode()
    msg = b" ".join([
        b"FULLRESYNC",
        master_id,
        offset + CLRS + length + CLRS + rdb_bytes + extra_cmds
    ])
    return msg


def test_returns_bytes_after_rdb_when_extra_cmds_exist():
    msg = make_resync_msg(rdb_bytes=b"XYZ", extra_cmds=b"PING\r\n+PONG\r\n")
    result = get_bytes_after_fullresync(msg)
    assert result.startswith(b"PING\r\n+PONG\r\n")


def test_returns_empty_when_no_extra_cmds():
    msg = make_resync_msg(rdb_bytes=b"123456", extra_cmds=b"")
    result = get_bytes_after_fullresync(msg)
    assert result == b""


def test_preserves_additional_tokens():
    # Put some tokens after the third one
    rdb_bytes = b"QWER"
    extra = b"ECHO foo\r\n"
    msg = b" ".join([
        b"FULLRESYNC",
        b"abcd123",
        b"999" + CLRS + str(len(rdb_bytes)).encode() + CLRS + rdb_bytes + extra,
        b"OTHER",
        b"TOKENS"
    ])
    result = get_bytes_after_fullresync(msg)
    # Should include extra after RDB and the other tokens
    assert b"ECHO foo\r\n" in result
    assert b"OTHER TOKENS" in result


def test_handles_multiple_commands_after_rdb():
    msg = make_resync_msg(
        rdb_bytes=b"AAA",
        extra_cmds=b"SET key value\r\nGET key\r\nDEL key\r\n"
    )
    result = get_bytes_after_fullresync(msg)
    assert b"SET key value" in result
    assert b"GET key" in result
    assert b"DEL key" in result


def test_offset_and_length_are_ignored_in_output():
    msg = make_resync_msg(offset=b"7777", rdb_bytes=b"HELLO", extra_cmds=b"PING\r\n")
    result = get_bytes_after_fullresync(msg)
    # Offset/length never appear in result
    assert b"7777" not in result
    assert b"5" not in result  # length of HELLO
    assert result.startswith(b"PING")
