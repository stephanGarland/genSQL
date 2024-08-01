from queue import Queue
from threading import Thread
from typing import Any, List, Tuple


class Writer:
    def __init__(self, filename: str):
        self.filename = filename
        self.queue: Queue = Queue()

    def writer_func(self) -> None:
        with open(self.filename, "wb") as f:
            while True:
                chunk: Tuple[List[bytes], ...] = self.queue.get()
                if chunk == "EOF":
                    break
                flattened_row: List = []
                for row in zip(*chunk):
                    for item in row:
                        flattened_row.extend(item)
                    f.write(b",".join(flattened_row) + b"\n")
                    flattened_row.clear()

    def start(self):
        self.writer_thread = Thread(target=self.writer_func)
        self.writer_thread.start()

    def join(self):
        self.writer_thread.join()

    def write_chunk(self, chunk):
        self.queue.put(chunk)

    def end(self):
        self.queue.put("EOF")
