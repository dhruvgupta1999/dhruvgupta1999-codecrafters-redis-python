"""
Streams:

1. Buffer bw producers and consumers. eg: producers can be an IOT device.
2. each stream event is immutable.
3. They are logged as a timeseries:  event1 -> event2 -> event3
(append-only log)

I should be able to query for all events that happened between event1 and event2

Also n entries just after <event> or n entries just before <event>



Approach 1:
A good dsa seems like sorted hashmap:
keys in sorted order
vals are the event data.

But here each lookup is log(n)

Approach 2:
Tries allow lookup in O(1) time technically.
But they work on lexographical stuff.
With numbers the issue is that:
'2' is after '10' even though numerically it comes first.

To solve this.
Let's assume every timestamp_ms fits in a 20 digit number.
If the ts given to us has lesser digits, I append 0s at the beginning.

'00000000000000000002' comes before '00000000000000000010'


Now there is a small nuance.
Entry IDs are always composed of two integers: <millisecondsTime>-<sequenceNumber>

the ids given to us can have
53424-1
53424-2

and so on.

This is basically because at the same ms we got multiple entries.

How to resolve this:

sub-approach 1:
we just put them in a list. at same ms we can get atmost 5-6 entries cuz it's pretty rare.

sub-approach 2:
we use the trie itself a bit more cleverly.
We assume that there can't be more than 100 entries at the same ms.
so the timestamp_id becomes:
<20digits of ms value><2 digits of seq num>

"""
import time
from dataclasses import dataclass, field
from typing import Self

from app.errors import InvalidStreamEventTsId

NUM_DIGITS_TS = 20
NUM_DIGITS_SEQ = 2

def as_x_digit_str(x, val:str) -> str:
    num_dig = len(val)
    if num_dig > x:
        raise ValueError(f"More digits in input than expected: {val}")
    result = ('0' * (x-num_dig)) + val
    return result

def get_unix_time_ms():
    # Get the current Unix timestamp as a floating-point number
    unix_timestamp = time.time()
    unix_ts_ms = int(unix_timestamp * 1000)
    return unix_ts_ms

def _get_trie_key(event_ts_id):
    ts_str, seq_num_str = event_ts_id.split('-')
    trie_key = as_x_digit_str(NUM_DIGITS_TS, ts_str) + as_x_digit_str(NUM_DIGITS_SEQ, seq_num_str)
    return trie_key

def _get_leaf_val_as_array(leaf_node):
    vals = []
    for key, val in leaf_node.val.items():
        vals.extend([key, val])
    sub_result = [leaf_node.event_ts_id, vals]
    return sub_result

####################################################################################################


@dataclass
class _BranchNode:
    """
    node.children['a'].children['b'] -> how to traverse
    """
    children: dict = field(default_factory=dict)

@dataclass
class _LeafNode:
    """
    LeafNodes are connected as a linked list.
    This helps us avoid going up and down the tree repeatedly to get the next/previous nodes
    since we primarily do range queries.
    """
    event_ts_id: str
    val: dict|None
    prev_leaf: Self = None
    next_leaf: Self = None


class RedisStream:

    def __init__(self):
        self._root = _BranchNode()
        self._latest_leaf: _LeafNode = None

    def append(self, event_ts_id: str, val_dict) -> str:
        """
        event_ts_id is a string with a fixed known number of digits.

        1231

        root->1 = branch1
        branch1->2 = branch2
        branch2->3 = branch3
        branch3->1 = leafnode
        """
        event_ts_id = self._resolve_event_ts_id(event_ts_id)
        ts_str, seq_num_str = event_ts_id.split('-')

        self._validate_ts_id(ts_str, seq_num_str)
        trie_key = _get_trie_key(event_ts_id)
        print(f"trie key: {trie_key}")
        cur_node = self._get_branch_node_with_prefix(trie_key[:-1])
        # Last character maps to a leaf node.
        last_ch = trie_key[-1]
        if last_ch in cur_node.children:
            raise ValueError(f"{event_ts_id=} already exists:", cur_node.children[last_ch])
        new_latest_leaf = cur_node.children[last_ch] = _LeafNode(event_ts_id=event_ts_id, val=val_dict, prev_leaf=self._latest_leaf, next_leaf=None)
        if self._latest_leaf:
            self._latest_leaf.next_leaf = new_latest_leaf
        self._latest_leaf = new_latest_leaf
        return f"{ts_str}-{seq_num_str}"

    def _get_first_leaf_after(self, trie_key) -> _LeafNode | None:
        """
        Find first leaf with trie_key >= input trie key
        """
        index = 0
        node = self._root
        while not (isinstance(node, _LeafNode) or (node is None)):
            parent = node
            digit = int(trie_key[index])
            for i in range(digit,10):
                if str(i) in node.children:
                    node = node.children[str(i)]
                    break
            index += 1
            if node == parent:
                return None
        return node

    def xrange(self, start: str, end: str):
        """
        inclusive on start and end
        """
        if self._latest_leaf is None:
            return []
        result = []
        if start == '-':
            start = '0'
        if '-' in start:
            start_trie_key = _get_trie_key(start)
        else:
            # If only ts part is there, then assume seq_num as '0'
            start_trie_key = _get_trie_key(start+'-0')

        if end == '+':
            end_trie_key = _get_trie_key(self._latest_leaf.event_ts_id)
        else:
            if '-' in end:
                end_trie_key = _get_trie_key(end)
            else:
                # Till last seq_num in the end.
                end_trie_key = _get_trie_key(end+'-99')

        first_leaf = self._get_first_leaf_after(start_trie_key)
        if not first_leaf:
            return result

        leaf = first_leaf
        while leaf and _get_trie_key(leaf.event_ts_id) <= end_trie_key:
            sub_result = _get_leaf_val_as_array(leaf)
            result.append(sub_result)
            leaf = leaf.next_leaf

        return result

    def xread(self, start):
        """
        start exclusive
        """
        result = []
        if '-' in start:
            start_trie_key = _get_trie_key(start)
        else:
            # If only ts part is there, then assume seq_num as '0'
            start_trie_key = _get_trie_key(start+'-0')

        first_leaf = self._get_first_leaf_after(start_trie_key)
        if not first_leaf:
            return result

        # start is exclusive in xread
        if _get_trie_key(first_leaf.event_ts_id) == _get_trie_key(start):
            first_leaf = first_leaf.next_leaf

        leaf = first_leaf
        while leaf:
            sub_result = _get_leaf_val_as_array(leaf)
            result.append(sub_result)
            leaf = leaf.next_leaf

        return result


    def _resolve_event_ts_id(self, event_ts_id):
        if event_ts_id == '*':
            ts_str = str(get_unix_time_ms())
            seq_num_str = '*'
        else:
            ts_str, seq_num_str = event_ts_id.split('-')
        trie_key_ts_part = as_x_digit_str(NUM_DIGITS_TS, ts_str)
        if seq_num_str == '*':
            seq_num_str = self._get_next_seq_no(trie_key_ts_part)
        return ts_str + '-' +  seq_num_str

    def _get_next_seq_no(self, trie_key_ts_part):
        cur_node = self._get_branch_node_with_prefix(trie_key_ts_part)
        latest_leaf = self._get_latest_leaf(cur_node)
        if latest_leaf is None:
            seq_no = '1' if trie_key_ts_part == as_x_digit_str(NUM_DIGITS_TS,'0') else '0'
        else:
            last_ts, last_seq_num = latest_leaf.event_ts_id.split('-')
            seq_no = str(int(last_seq_num) + 1)
        return seq_no

    def _get_latest_leaf(self, node: _BranchNode) -> _LeafNode | None:
        """
        Get the last appended leaf in the sub-tree of node.
        """
        while not (isinstance(node, _LeafNode) or (node is None)):
            parent = node
            for i in range(9,-1,-1):
                if str(i) in node.children:
                    node = node.children[str(i)]
                    break
            if node == parent:
                return None
        return node

    def _get_branch_node_with_prefix(self, trie_key_prefix):
        """
        If the branch doesn't exist, create it. (MAY have to change this later).
        """
        # We don't want to reach leaf node.
        assert len(trie_key_prefix) < NUM_DIGITS_TS + NUM_DIGITS_SEQ
        cur_node = self._root
        for ch in trie_key_prefix:
            if ch not in cur_node.children:
                cur_node.children[ch] = _BranchNode()
            cur_node = cur_node.children[ch]
        return cur_node

    def _validate_ts_id(self, ts, seq_no):
        ts, seq_no = int(ts), int(seq_no)
        if (ts, seq_no) <= (0,0) :
            raise InvalidStreamEventTsId("ERR The ID specified in XADD must be greater than 0-0")
        if not self._latest_leaf:
            return
        latest_ts, latest_seq_num = self._latest_leaf.event_ts_id.split('-')
        if (int(ts), int(seq_no)) <= ( int(latest_ts), int(latest_seq_num)):
            raise InvalidStreamEventTsId("ERR The ID specified in XADD is equal or smaller than the target stream top item")

    def pretty_print(self):
        print("Here is the redis stream")
        cur_leaf = self._latest_leaf
        if not cur_leaf:
            print("Stream is empty right now.")
        while cur_leaf:
            print(cur_leaf.event_ts_id, cur_leaf.val)
            cur_leaf = cur_leaf.prev_leaf
