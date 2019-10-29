"""A multiprocessing Queue but is "fair" to every flow/session/channel."""


import sys
from multiprocessing.queues import Queue, _sentinel


if sys.version_info[0] == 3:
    from queue import PriorityQueue, SimpleQueue, Empty, Full
else:
    from Queue import PriorityQueue, Queue as SimpleQueue, Empty, Full


class PrioritizedItem(object):
    """Adds ordering to item, without including item in the ordering."""

    __slots__ = ["priority", "item", "flow"]

    def __init__(self, priority, item, flow):
        self.priority = priority
        self.item = item
        self.flow = flow

    def __lt__(self, other):
        return self.priority < other.priority

    def __repr__(self):
        return "<PrioritizedItem priority=%r item=%r flow=%r>" % (self.priority, self.item, self.flow)


class ExclusiveQueueBuffer(object):
    """Implement deque interface used by Queue, but each item must have a flow.

    # Items must be of the form (flow, item) where:
    #  * flow ???
    #  * item is picklable
    """

    _default_weight = 1.0

    def __init__(self):
        self._flow_weight = {}
        self.clear()

    def set_flow_weight(self, flow, weight):
        assert weight
        if weight == self._default_weight:
            self._flow_weight.pop(flow, None)
        else:
            self._flow_weight[flow] = float(weight)

    def get_flow_weight(self, flow):
        return self._flow_weight.get(flow, self._default_weight)

    def append(self, obj):
        # nop if closed (should it raise?)
        if self._put_closed:
            raise Full()

        # extract flow
        try:
            flow, obj = obj
        except Exception:
            if obj is not _sentinel:
                raise

            # Queue has passed us _sentinel as obj, close the workers!
            self._put_closed = True
            return

        # get queue for flow
        try:
            flow_queue = self._flow_queues[flow]
        except KeyError:
            # add it to the queue
            flow_queue = self._flow_queues[flow] = SimpleQueue()
            self.unlock_flow(flow)

        # add obj to flow queue
        flow_queue.put(obj)

    def popleft(self):
        # if it's closed then no more items
        if self._get_closed:
            raise IndexError()  # deque().popleft()

        # visit each queue once only
        for _ in range(self._priority_queue.qsize()):
            try:
                # pop an item
                item = self._priority_queue.get(block=False)

            except Empty:
                # if we previously received sentinel, return it
                if self._put_closed:
                    self._get_closed = True
                    return _sentinel

                raise IndexError()  # deque().popleft()

            else:
                flow_queue = item.item
                try:
                    return flow_queue.get(block=False)
                except Empty:
                    # put flow back into queue
                    self.unlock_flow(item.flow)

        # nothing found
        raise IndexError()  # deque().popleft()

    def unlock_flow(self, flow):
        # ugh! need to send this from the worker back to the master!
        try:
            flow_queue = self._flow_queues[flow]
        except KeyError:
            return
        if flow_queue not in self._priority_queue.queue:
            count = self._flow_count[flow] = self._flow_count.get(flow, 0) + 1
            priority = count / self.get_flow_weight(flow)
            self._priority_queue.put(PrioritizedItem(priority, flow_queue, flow))

    def clear(self):
        self._priority_queue = PriorityQueue()
        self._flow_queues = {}
        self._flow_count = {}
        self._put_closed = False
        self._get_closed = False


class ExclusiveQueue2(Queue):
    def _after_fork(self):
        super(ExclusiveQueue2, self)._after_fork()
        self._buffer = ExclusiveQueueBuffer()

    def set_flow_weight(self, flow, weight):
        self._buffer.set_flow_weight(flow, weight)

    def get_flow_weight(self, flow):
        self._buffer.get_flow_weight(flow)

    def unlock_flow(self, flow):
        self._buffer.unlock_flow(flow)


if sys.version_info[0] == 3:
    from multiprocessing import get_context

    class ExclusiveQueue3(ExclusiveQueue2):
        def __init__(self, *args, **kwargs):
            super(ExclusiveQueue3, self).__init__(*args, ctx=get_context(), **kwargs)

    ExclusiveQueue = ExclusiveQueue3
else:
    ExclusiveQueue = ExclusiveQueue2


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
            time.sleep(0.1)

    if __name__ == "__main__":
        # q = ExclusiveQueueBuffer()
        q = ExclusiveQueue()
        f = defaultdict(int)

        q.set_flow_weight(2, 2)

        for i in range(20):
            flow = random.randint(1, 4)
            # flow = (i % 4) + 1
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

        worker_process = Process(target=worker, args=(q,))
        worker_process.daemon = True
        worker_process.start()

        # f = defaultdict(int)
        # for i in range(20):
        #     flow = q.popleft()
        #     f[flow] += 1
        #     print("get %s -> %s" % (flow, {f_flow: (f_freq / i) if i else None for f_flow, f_freq in f.items()}))

        while not q.empty():
            pass

        q.close()
        q.join_thread()
