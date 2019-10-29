"""A multiprocessing Queue but is "fair" to every flow/session/channel."""


import sys
from heapq import heappop, heappush
from multiprocessing.queues import Queue, _sentinel


if sys.version_info[0] == 3:
    from queue import PriorityQueue as PQueue, Empty, Full
else:
    from Queue import PriorityQueue as PQueue, Empty, Full


class PrioritizedItem(object):
    """Adds ordering to item, without including item in the ordering."""

    __slots__ = ["priority", "item"]

    def __init__(self, priority, item):
        self.priority = priority
        self.item = item

    def __lt__(self, other):
        return self.priority < other.priority

    def __repr__(self):
        return "<PrioritizedItem priority=%r item=%r>" % (self.priority, self.item)


class PriorityQueueBuffer(object):
    """Implement deque interface used by Queue, but each item must have a priority.

    Items must be of the form (priority, item) where:
     * priority is orderable
     * item is picklable
    """

    def __init__(self):
        self.clear()

    def append(self, obj):
        # nop if closed (should it raise?)
        if self._put_closed:
            raise Full()

        # extract flow
        try:
            priority, obj = obj
        except Exception:
            if obj is not _sentinel:
                raise

            # Queue has passed us _sentinel as obj, close the workers!
            self._put_closed = True
            return

        # finally put item!
        self._priority_queue.put(PrioritizedItem(priority, obj))

    def popleft(self):
        # if it's closed then no more items
        if self._get_closed:
            raise IndexError()  # deque().popleft()

        try:
            # get item
            item = self._priority_queue.get(block=False)

        except Empty:
            # if we previously received sentinel, return it
            if self._put_closed:
                self._get_closed = True
                return _sentinel

            raise IndexError()  # deque().popleft()

        # unwrap item
        return item.item

    def clear(self):
        self._priority_queue = PQueue()
        self._put_closed = False
        self._get_closed = False


class PriorityQueue2(Queue):
    def _after_fork(self):
        super(PriorityQueue2, self)._after_fork()
        self._buffer = PriorityQueueBuffer()

    def set_flow_weight(self, flow, weight):
        self._buffer.set_flow_weight(flow, weight)

    def get_flow_weight(self, flow):
        self._buffer.get_flow_weight(flow)


if sys.version_info[0] == 3:
    from multiprocessing import get_context

    class PriorityQueue3(PriorityQueue2):
        def __init__(self, *args, **kwargs):
            super(PriorityQueue3, self).__init__(*args, ctx=get_context(), **kwargs)

    PriorityQueue = PriorityQueue3
else:
    PriorityQueue = PriorityQueue2
