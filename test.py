import time
from multiprocessing import Process, Queue

# from priorityqueue import PriorityQueue
PriorityQueue = Queue


def worker(queue):
    for item in iter(queue.get, None):
        print('worker got %s' % str(item))


if __name__ == '__main__':
    pr_queue = PriorityQueue()

    pr_queue.put((1, 'second'))
    pr_queue.put((0, 'first'))

    worker_process = Process(target=worker, args=(pr_queue,))
    worker_process.daemon = True
    worker_process.start()

    while not pr_queue.empty():
        pass

    pr_queue.close()
    pr_queue.join_thread()
