import threading
from contextlib import contextmanager
from threading import Lock

# dictionary to store locks by id
locks = {}
lock_lock = Lock()


@contextmanager
def thread_safe(id):
    id = str(id)
    # acquire a lock for this id
    lock_lock.acquire()
    if id not in locks:
        locks[id] = threading.Lock()
    lock = locks[id]
    lock.acquire()
    lock_lock.release()
    print(locks)

    try:
        print(f"Executing critical section for id {id}")
        yield
    finally:
        # release the lock for this id
        lock.release()