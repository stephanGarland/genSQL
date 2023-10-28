from collections import defaultdict
from graphlib import TopologicalSorter
from queue import Queue
from threading import Thread

from gensql.test_gen import Geo

CHUNK_SIZE = 10_000
NUM_ROWS = 1_000_000
THREADING_BUFFER_SIZE = 10_000


class ThreadedFileWriter:
    def __init__(self, country: str, file_name: str, num_rows: int):
        self.buffer_size = THREADING_BUFFER_SIZE
        self.chunk = []
        self.country = country
        self.file_name = file_name
        self.geo = Geo(CHUNK_SIZE, country, num_rows)
        self.num_rows = num_rows
        self.queue = Queue()

    def csv_header_writer(self, col_names: list):
        with open(self.file_name, "w") as f:
            f.write(",".join(col_names) + "\n")

    def writer_thread(self):
        with open(self.file_name, "a") as f:
            while True:
                chunks = self.queue.get()
                if chunks == "EOF":
                    break
                for chunk in chunks:
                    for row in chunk:
                        f.write(
                            ",".join(
                                map(
                                    lambda item: '"' + item.replace('"', '""') + '"',
                                    row,
                                )
                            )
                            + "\n"
                        )

    def generate_topo_graph(self, cols: dict[str, dict[str, str]]) -> set:
        graph = defaultdict(set)
        for k, v in cols.items():
            graph[k].add(v.get("depends_on"))

        ts = TopologicalSorter(graph)

        return set(ts.static_order())

    def generate_col_order(self, cols: dict[str, dict[str, str]]) -> dict:
        col_order = {}
        for i, col in enumerate(cols):
            col_order[col] = i

        return col_order

    def generate_data(self, col_order, data_type, num_rows, return_dict, *args):
        fn_map = {
            "city": self.geo.make_city,
            "country": self.geo.make_country,
            "phone": self.geo.make_phone,
        }
        return_dict[col_order] = fn_map[data_type](*args)

    def make_with_threads(self, data_dict: dict):
        threads = []
        return_dict = {}
        topo_graph = self.generate_topo_graph(data_dict)
        col_order = self.generate_col_order(data_dict)
        topo_graph.discard(None)
        for col in topo_graph:
            t = Thread(
                target=self.generate_data,
                args=(
                    col_order[col],
                    col,
                    self.num_rows,
                    return_dict,
                    self.country
                    #data_dict[col].get("depends_on"),
                ),
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        sorted_data = [return_dict[x] for x in sorted(return_dict.keys())]
        combined_data = list(zip(*sorted_data))
        rows = [list(zip(*x)) for x in combined_data]

        return rows

    def row_builder(self):
        data_dict = {
            "phone": {"depends_on": "country"},
            "country": {None: None},
            "city": {"depends_on": "country"},
        }
        self.csv_header_writer(data_dict.keys())
        writer = Thread(target=self.writer_thread)
        writer.start()
        rows = self.make_with_threads(data_dict)
        for row in rows:
            self.chunk.append(row)
            if len(self.chunk) >= self.buffer_size:
                self.queue.put(self.chunk)
                self.chunk = []
        if self.chunk:
            self.queue.put(self.chunk)
        self.queue.put("EOF")
        writer.join()


if __name__ == "__main__":
    t = ThreadedFileWriter("us", "test.csv", NUM_ROWS)
    t.row_builder()
