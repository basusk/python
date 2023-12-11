import threading
import requests
import queue
POOL_SIZE = 4

def myFunc(inq, outq):  # worker thread deals only with queues
    while True:
        url = inq.get()  # Blocks until something available
        if url is None:
            break
        response = requests.get(url.strip(), timeout=(2, 5))
        outq.put((url, response, threading.currentThread().name))


class Writer(threading.Thread):
    def __init__(self, q):
        super().__init__()
        self.results = open("myresults","a") # "a" to append results
        self.queue = q
    def run(self):
        while True:
            url, response, threadname = self.queue.get()
            if response is None:
                self.results.close()
                break
            print("****url is:",url, ", response is:", response.status_code, response.url, "thread", threadname, file=self.results)

#load up a queue with your data, this will handle locking
inq = queue.Queue()  # could usefully limit queue size here
outq = queue.Queue()

# start the Writer
writer = Writer(outq)
writer.start()

# make the Pool of workers
threads = []
for i in range(POOL_SIZE):
    thread = threading.Thread(target=myFunc, name=f"worker{i}", args=(inq, outq))
    thread.start()
    threads.append(thread)

# push the work onto the queues
with open("mylines.txt","r") as worker_data: # open my input file.
    for url in worker_data:
        inq.put(url.strip())
for thread in threads:
    inq.put(None)

# close the pool and wait for the workers to finish
for thread in threads:
    thread.join()

# Terminate the writer
outq.put((None, None, None))
writer.join()
