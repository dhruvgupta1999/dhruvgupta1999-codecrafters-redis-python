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
from dataclasses import dataclass, field
from typing import Self

NUM_DIGITS_TS = 20
NUM_DIGITS_SEQ = 2

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
    event_ts_id: bytes
    val: dict|None
    prev_leaf: Self = None
    next_leaf: Self = None


class RedisStream:

    def __init__(self):
        self._root = _BranchNode()
        self._first_leaf = _LeafNode(val=None, prev_leaf=None, next_leaf=None)
        self._latest_leaf: _LeafNode = self._first_leaf

    def append(self, event_ts_id: bytes, val_dict):
        """
        event_ts_id is a string with a fixed known number of digits.

        1231

        root->1 = branch1
        branch1->2 = branch2
        branch2->3 = branch3
        branch3->1 = leafnode
        """

        internal_event_ts_id = self.get_internal_event_ts_id(event_ts_id)
        cur_node = self._root
        for ch in internal_event_ts_id[:-1]:
            if ch not in cur_node.children:
                cur_node.children[ch] = _BranchNode()
            cur_node = cur_node.children[ch]
        # Last character maps to a leaf node.
        last_ch = internal_event_ts_id[-1]
        if last_ch in cur_node.children:
            raise ValueError(f"{event_ts_id=} already exists:", cur_node.children[last_ch])
        cur_node.children[last_ch] = _LeafNode(event_ts_id=event_ts_id, val=val_dict, prev_leaf=self._latest_leaf, next_leaf=None)
        self._latest_leaf.next_leaf = cur_node.children[last_ch]

    def get_internal_event_ts_id(self, event_ts_id: bytes):
        event_ts_id_str = event_ts_id.decode()
        ts_ms, seq_no = '-'.split(event_ts_id_str)
        ts_ms = as_x_digit_str(NUM_DIGITS_TS, ts_ms)
        seq_no = as_x_digit_str(NUM_DIGITS_SEQ, seq_no)
        internal_event_ts_id = ts_ms + seq_no
        return internal_event_ts_id

    def pretty_print(self):
        idx = 0
        print("Here is the redis stream")
        cur_leaf = self._first_leaf.next_leaf
        if not cur_leaf:
            print("Stream is empty right now.")
        while cur_leaf:
            print(idx, cur_leaf.val)
            idx += 1
            cur_leaf = cur_leaf.next_leaf

def as_x_digit_str(x, val:str) -> str:
    num_dig = len(val)
    if num_dig > x:
        raise ValueError(f"More digits in input than expected: {val}")
    result = ('0' * (x-num_dig)) + val
    return result