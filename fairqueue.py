"""A multiprocessing Queue but is "fair" to every flow/session/channel.

See https://en.wikipedia.org/wiki/Fair_queuing.
"""


import sys
import time
from multiprocessing.queues import Queue


if sys.version_info[0] == 3:
    from queue import PriorityQueue, Empty
else:
    from Queue import PriorityQueue, Empty


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


class FairQueueBuffer(object):
    """Implement deque interface used by Queue, but each item must have a flow.

    Items must be of the form (flow, item) where:
     * flow has __hash__
     * item is picklable
    """

    _default_weight = 1.0

    def __init__(self):
        self._flow_weight = {}
        self.clear()

    def clear(self):
        self._priority_queue = PriorityQueue()
        self._flow_finish = {}

    def set_flow_weight(self, flow, weight):
        if weight == self._default_weight:
            self._flow_weight.pop(flow, None)
        else:
            self._flow_weight[flow] = float(weight)

    def get_flow_weight(self, flow):
        return self._flow_weight.get(flow, self._default_weight)

    def append(self, obj):
        # extract flow
        try:
            flow, obj = obj
        except Exception:
            flow, obj = obj, obj

        # calculate finish time
        start = max(self._flow_finish.get(flow, 0), time.time())
        self._flow_finish[flow] = finish = start + (1.0 / self.get_flow_weight(flow))

        # finally put item!
        self._priority_queue.put(PrioritizedItem(finish, obj))

    def popleft(self):
        try:
            return self._priority_queue.get(block=False).item
        except Empty:
            raise IndexError()  # deque().popleft()


class FairQueue2(Queue):
    def _after_fork(self):
        super(FairQueue2, self)._after_fork()
        self._buffer = FairQueueBuffer()

    def set_flow_weight(self, flow, weight):
        self._buffer.set_flow_weight(flow, weight)

    def get_flow_weight(self, flow):
        self._buffer.get_flow_weight(flow)


if sys.version_info[0] == 3:
    from multiprocessing import get_context

    class FairQueue3(FairQueue2):
        def __init__(self, *args, **kwargs):
            super(FairQueue3, self).__init__(*args, ctx=get_context(), **kwargs)

    FairQueue = FairQueue3
else:
    FairQueue = FairQueue2


if __name__ == "__main__":
    import time
    from multiprocessing import Process
    import random
    from collections import defaultdict

    def worker(queue):
        f = defaultdict(int)
        for item in iter(queue.get, None):
            f[item] += 1
            print("get %s <- %s" % (item, f))
            # time.sleep(0.1)

    if __name__ == "__main__":
        # q = FairQueueBuffer()
        q = FairQueue()
        f = defaultdict(int)

        q.set_flow_weight(2, 2)

        worker_process = Process(target=worker, args=(q,))
        worker_process.daemon = True
        worker_process.start()

        for i in range(20):
            flow = (i % 4) + 1
            # flow = random.randint(1, 4)
            f[flow] += 1
            print("put %s" % flow)
            q.put((flow, flow))
            # q.append((flow, flow))
        print(f)
        # print({f_flow: (f_freq / 20) for f_flow, f_freq in f.items()})
        # print(q._priority_queue.queue)

        # q.put((4, 4))
        # q.put((4, 4))
        # q.put((4, 4))
        # q.put((4, 4))
        # q.put((3, 3))
        # q.put((3, 3))
        # q.put((3, 3))
        # q.put((2, 2))
        # q.put((2, 2))
        # q.put((1, 1))

        # f = defaultdict(int)
        # for i in range(20):
        #     flow = q.popleft()
        #     f[flow] += 1
        #     print("get %s -> %s" % (flow, {f_flow: (f_freq / i) if i else None for f_flow, f_freq in f.items()}))

        while not q.empty():
            pass

        q.close()
        q.join_thread()
