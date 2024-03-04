import ctypes
import sqlite3
from collections import defaultdict
from graphlib import TopologicalSorter
from multiprocessing import resource_tracker, shared_memory
from queue import Queue
from threading import Thread

from library.generators.email import Email
from library.generators.geo import Geo
from library.generators.name import FirstName, LastName

DB_PATH = "db/gensql.db"
NUM_ROWS = 10_000_000
SH_MEM_NAME_FNAME = "SHM_FNAME"
SH_MEM_NAME_LNAME = "SHM_LNAME"
SH_MEM_SZ = 1 << 20
SIZEOF_CHAR = 4
NUM_CHARS = SH_MEM_SZ // SIZEOF_CHAR
THREADING_BUFFER_SIZE = 10_000


class SQLiteColumns:
    def __init__(self, db_path: str):
        conn = sqlite3.connect(db_path)
        self.cursor = conn.cursor()

    def get_first_names(self):
        q_fname = "SELECT MAX(LENGTH(first_name)) FROM person_name UNION SELECT first_name FROM person_name"
        self.cursor.execute(q_fname)
        res = self.cursor.fetchall()
        max_fname, fnames = res[0], res[1:]
        return max_fname, fnames

    def get_last_names(self):
        q_lname = "SELECT MAX(LENGTH(last_name)) FROM person_name UNION SELECT last_name FROM person_name"
        self.cursor.execute(q_lname)
        res = self.cursor.fetchall()
        max_lname, lnames = res[0], res[1:]
        return max_lname, lnames


class CreateSharedMem:
    def __init__(self):
        sqlite_cols = SQLiteColumns(DB_PATH)
        self.max_fname, self.first_names = sqlite_cols.get_first_names()
        self.max_lname, self.last_names = sqlite_cols.get_last_names()

        self.shm_first_names = shared_memory.SharedMemory(
            name=SH_MEM_NAME_FNAME, create=True, size=SH_MEM_SZ
        )
        self.shm_last_names = shared_memory.SharedMemory(
            name=SH_MEM_NAME_LNAME, create=True, size=SH_MEM_SZ
        )

        self.lib = ctypes.CDLL("char_shuffle.so")
        self.lib.shuffle_fname.argtypes = [ctypes.c_int32, ctypes.c_int32]
        self.lib.shuffle_lname.argtypes = [ctypes.c_int32, ctypes.c_int32]

    def fill_shmem(self, shm, content, max_len):
        offset = 0
        for x in content:
            encoded = x[0].encode("utf-8").ljust(max_len, b"\x00")
            shm.buf[offset : offset + max_len] = encoded
            offset += max_len


class ThreadedFileWriter:
    def __init__(self, file_name: str, writer_queue: Queue):
        self.file_name = file_name
        self.writer_queue = writer_queue

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


class ColumnOrderGenerator:
    def __init__(self, cols: dict[str, dict[str, str]]):
        self.cols = cols

    def generate_topo_graph(self) -> set:
        graph: dict = defaultdict(set)
        for k, v in self.cols.items():
            graph[k]
            for depend in v.get("depends_on", []):
                if depend is not None:
                    graph[depend].add(k)

        ts = TopologicalSorter(graph)
        return set(ts.static_order())

    def generate_col_order(self) -> dict[str, int]:
        col_order = {}
        for i, col in enumerate(self.cols):
            col_order[col] = i

        return col_order


class RowGenerator:
    def __init__(self, country: str, file_name: str, num_rows: int, shm):
        self.col_order_gen = ColumnOrderGenerator
        self.shm = shm
        self.chunk: list = []
        self.country = country
        self.file_name = file_name
        self.first_name = FirstName(NUM_ROWS, self.shm.lib.shuffle_fname)
        self.last_name = LastName(NUM_ROWS, self.shm.lib.shuffle_lname)
        self.email = Email(NUM_ROWS)
        self.geo = Geo(country, NUM_ROWS)
        self.num_rows = num_rows
        self.writer_queue = Queue()
        self.writer = ThreadedFileWriter(file_name, self.writer_queue)

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
        # TODO: clean this up, ideally making the contract more definite
        # so these checks don't need to happen at all
        if isinstance(arg_str, str) and arg_str.startswith("self."):
            resolved_arg = getattr(context, arg_str[5:])
        elif isinstance(arg_str, str) and arg_str.startswith("shm."):
            resolved_arg = getattr(shm, arg_str[4:])
        elif isinstance(arg_str, list) and arg_str[0].startswith("self."):
            resolved_arg = [getattr(context, x[5:]) for x in arg_str]
        else:
            resolved_arg = arg_str
        return resolved_arg

    def make_with_threads(self, data_dict: dict):
        cog_inst = self.col_order_gen(data_dict)
        threads = []
        return_dict = {}
        topo_graph = cog_inst.generate_topo_graph()
        col_order = cog_inst.generate_col_order()
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
            "first_name": {"args": "shm.shm_first_names"},
            "last_name": {"args": "shm.shm_last_names"},
            "email": {
                "args": ["self.first_name", "self.last_name"],
                "depends_on": ["first_name", "last_name"],
            },
            "phone": {"args": "self.country", "depends_on": ["country"]},
            "country": {"depends_on": ["city"]},
            "city": {},
        }
        self.writer.csv_header_writer(data_dict.keys())
        writer = Thread(target=self.writer.writer_thread)
        writer.start()
        rows = self.make_with_threads(data_dict)
        for row in rows:
            self.chunk.append(row)
            if len(self.chunk) >= THREADING_BUFFER_SIZE:
                self.writer_queue.put(self.chunk)
                self.chunk = []
        if self.chunk:
            self.writer_queue.put(self.chunk)
        self.writer_queue.put("EOF")
        writer.join()


if __name__ == "__main__":
    shm = CreateSharedMem()
    # TODO: dynamically calculate this
    max_len = 16
    # shm.max_fname[0] == 15
    shm.fill_shmem(shm.shm_first_names, shm.first_names, max_len)
    shm.fill_shmem(shm.shm_last_names, shm.last_names, max_len)

    shm.lib.shuffle_fname(len(shm.first_names), max_len)
    shm.lib.shuffle_lname(len(shm.last_names), max_len)

    row_gen = RowGenerator(
        country="United States", file_name="test.csv", num_rows=NUM_ROWS, shm=shm
    )
    row_gen.row_builder()
    shm.shm_first_names.close()
    shm.shm_last_names.close()
    shm.shm_first_names.unlink()
    shm.shm_last_names.unlink()
