from collections import defaultdict
from graphlib import TopologicalSorter
from queue import Queue
from threading import Thread

from library.generators.geo import Geo
from library.generators.email import Email
from library.generators.name import FirstName, LastName

CHUNK_SIZE = 10_000
NUM_ROWS = 1_000_000
THREADING_BUFFER_SIZE = 10_000


class ThreadedFileWriter:
    # TODO: break this class up
    def __init__(self, country: str, file_name: str, num_rows: int):
        self.buffer_size = THREADING_BUFFER_SIZE
        self.chunk = []
        self.country = country
        self.file_name = file_name
        self.first_name = FirstName(NUM_ROWS)
        self.last_name = LastName(NUM_ROWS)
        self.email = Email(NUM_ROWS)
        self.geo = Geo(country, NUM_ROWS)
        self.num_rows = num_rows
        self.writer_queue = Queue()

    def csv_header_writer(self, col_names: list):
        with open(self.file_name, "w") as f:
            f.write(",".join(col_names) + "\n")

    def writer_thread(self):
        with open(self.file_name, "a") as f:
            while True:
                chunks = self.writer_queue.get()
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
            graph[k]
            for depend in v.get("depends_on", []):
                if depend is not None:
                    graph[depend].add(k)

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
            "email": self.email.make_email,
            "first_name": self.first_name.make_name,
            "last_name": self.last_name.make_name,
            "phone": self.geo.make_phone,
        }
        return_dict[col_order] = fn_map[data_type](*args)

    def resolve_arg(self, arg_str, context):
        if isinstance(arg_str, str) and arg_str.startswith("self."):
            resolved_arg = getattr(context, arg_str[5:])
        elif isinstance(arg_str, list) and arg_str[0].startswith("self."):
            resolved_arg = [getattr(context, x[5:]) for x in arg_str]
        else:
            resolved_arg = arg_str
        return resolved_arg

    def make_with_threads(self, data_dict: dict):
        threads = []
        return_dict = {}
        topo_graph = self.generate_topo_graph(data_dict)
        col_order = self.generate_col_order(data_dict)
        for col in topo_graph:
            resolved_arg = self.resolve_arg(data_dict[col].get("args", ""), self)
            t = Thread(
                target=self.generate_data,
                args=(col_order[col], col, self.num_rows, return_dict, resolved_arg),
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
            "first_name": {},
            "last_name": {},
            "email": {"args": ["self.first_name", "self.last_name"], "depends_on": ["first_name", "last_name"]},
            "phone": {"args": "self.country", "depends_on": ["country"]},
            "country": {"depends_on": ["city"]},
            "city": {},
        }
        self.csv_header_writer(data_dict.keys())
        writer = Thread(target=self.writer_thread)
        writer.start()
        rows = self.make_with_threads(data_dict)
        for row in rows:
            self.chunk.append(row)
            if len(self.chunk) >= self.buffer_size:
                self.writer_queue.put(self.chunk)
                self.chunk = []
        if self.chunk:
            self.writer_queue.put(self.chunk)
        self.writer_queue.put("EOF")
        writer.join()


if __name__ == "__main__":
    t = ThreadedFileWriter("United States", "test.csv", NUM_ROWS)
    t.row_builder()
