from queue import Queue
from threading import Thread


class Writer:
    def __init__(self, filename: str):
        self.filename = filename
        self.queue: Queue = Queue()

    # TODO: encapsulate rows with quotes that don't break
    def writer_func(self):
        with open(self.filename, "wb") as f:
            while True:
                chunk = self.queue.get()
                if chunk == "EOF":
                    break

                for row in zip(*chunk):
                    f.write(b",".join(row) + b"\n")

    def start(self):
        self.writer_thread = Thread(target=self.writer_func)
        self.writer_thread.start()

    def join(self):
        self.writer_thread.join()

    def write_chunk(self, chunk):
        self.queue.put(chunk)

    def end(self):
        self.queue.put("EOF")
