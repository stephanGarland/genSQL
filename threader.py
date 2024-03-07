import ctypes
import sqlite3
from collections import defaultdict, deque
from graphlib import TopologicalSorter
from multiprocessing import shared_memory
from queue import Queue
from random import getrandbits
from threading import Event, Thread

from library.generators.email import Email
from library.generators.geo import Geo
from library.generators.word import Word
from utilities import constants as const

# TODO: replace this with argparse, as is done in legacy
NUM_ROWS = 1_000_000


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

    def get_words(self):
        q_words = "SELECT MAX(LENGTH(word)) FROM word UNION SELECT word FROM word"
        self.cursor.execute(q_words)
        res = self.cursor.fetchall()
        max_word, words = res[0], res[1:]
        return max_word, words


class CreateSharedMem:
    def __init__(self):
        sqlite_cols = SQLiteColumns(const.SQLITE_DB)
        self.max_fname, self.f_names = sqlite_cols.get_first_names()
        self.max_lname, self.l_names = sqlite_cols.get_last_names()
        self.max_word, self.words = sqlite_cols.get_words()
        self.shm_fname = shared_memory.SharedMemory(
            name=const.SH_MEM_NAME_FNAME, create=True, size=const.SH_MEM_SZ
        )
        self.shm_lname = shared_memory.SharedMemory(
            name=const.SH_MEM_NAME_LNAME, create=True, size=const.SH_MEM_SZ
        )
        self.shm_words = shared_memory.SharedMemory(
            name=const.SH_MEM_NAME_WORDS, create=True, size=const.SH_MEM_SZ
        )

        self.lib = ctypes.CDLL("library/char_shuffle.so")

        self.lib.get_shared_mem_ptr.argtypes = [ctypes.c_char_p]
        self.lib.get_shared_mem_ptr.restype = ctypes.c_void_p
        self.lib.shuffle_data.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        self.seeds = defaultdict(deque)

    def fill_shmem(self, shm, content, max_len):
        offset = 0
        for x in content:
            encoded = x[0].encode("utf-8").ljust(max_len, b"\x00")
            shm.buf[offset : offset + max_len] = encoded
            offset += max_len

    def shuffle_data(self, rowcount, max_word_len, shm_name, seed=None):
        # TODO: put these into constants
        if not len(shm_name) > 6:
            shm_name = "/SHM_" + shm_name.upper()
        elif not shm_name.startswith("/"):
            shm_name = "/" + shm_name.upper()
        if not seed:
            if self.seeds.get("PROVIDED"):
                seed = self.seeds[shm_name].popleft()
            else:
                seed = getrandbits(32)
                self.seeds[shm_name].append(seed)
        shm_name_c = shm_name.encode("utf-8")
        self.lib.shuffle_data(rowcount, max_word_len, shm_name_c, seed)

    def sort_data(self, rowcount, max_word_len, shm_name):
        if not len(shm_name) > 6:
            shm_name = "/SHM_" + shm_name.upper()
        elif not shm_name.startswith("/"):
            shm_name = "/" + shm_name.upper()
        shm_name_c = shm_name.encode("utf-8")
        self.lib.sort_data(rowcount, max_word_len, shm_name_c)


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
        self.first_name = Word(
            NUM_ROWS, self.shm, "fname", self.shm.shuffle_data, buffer_data=True
        )
        self.last_name = Word(
            NUM_ROWS, self.shm, "lname", self.shm.shuffle_data, buffer_data=True
        )
        self.email = Email(
            NUM_ROWS,
            self.shm,
            "words",
            self.shm.shuffle_data,
            self.first_name,
            self.last_name,
        )
        # self.geo = Geo(country, NUM_ROWS)
        self.num_rows = num_rows
        self.writer_queue: Queue = Queue()
        self.writer = ThreadedFileWriter(file_name, self.writer_queue)

    # TODO: Remove *args
    def generate_data(self, col_order, data_type, num_rows, return_dict, *args):
        # TODO: this must be passed in or deduced
        fn_map = {
            # "city": self.geo.make_city,
            # "country": self.geo.make_country,
            "email": self.email.generate,
            "first_name": self.first_name.generate,
            "last_name": self.last_name.generate,
            # "phone": self.geo.make_phone,
        }
        generator = fn_map[data_type](*args)
        data = []
        for chunk in generator:
            data.extend(chunk)
        return_dict[col_order] = data

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
        return_dict: dict = {}
        topo_graph = cog_inst.generate_topo_graph()
        col_order = cog_inst.generate_col_order()

        events = {col: Event() for col in topo_graph}

        for col in topo_graph:
            resolved_arg = self.resolve_arg(data_dict[col].get("args", ""), self)
            dependencies = data_dict[col].get("depends_on", [])
            event = events[col]

            def make_target_func(
                col, num_rows, return_dict, resolved_arg, event, dependencies
            ):
                def target_func():
                    for dep in dependencies:
                        events[dep].wait()

                    self.generate_data(
                        col_order[col], col, num_rows, return_dict, resolved_arg
                    )

                    event.set()

                return target_func

            t = Thread(
                target=make_target_func(
                    col, self.num_rows, return_dict, resolved_arg, event, dependencies
                )
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
            "email": {"depends_on": ["first_name", "last_name"]},
            # "email": {
            #    "args": ["self.first_name", "self.last_name"],
            #    "depends_on": ["first_name", "last_name"],
            # },
            # "phone": {"args": "self.country", "depends_on": ["country"]},
            # "country": {"depends_on": ["city"]},
            # "city": {},
        }
        self.writer.csv_header_writer(data_dict.keys())
        writer = Thread(target=self.writer.writer_thread)
        writer.start()
        rows = self.make_with_threads(data_dict)
        for row in rows:
            self.chunk.append(row)
            if len(self.chunk) >= const.THREADING_BUFFER_SIZE:
                self.writer_queue.put(self.chunk)
                self.chunk = []
        if self.chunk:
            self.writer_queue.put(self.chunk)
        self.writer_queue.put("EOF")
        writer.join()


if __name__ == "__main__":
    import pickle

    shm = CreateSharedMem()
    # with open("seeds.dat", "r+b") as f:
    #    shm.seeds = pickle.load(f)
    #    shm.seeds["PROVIDED"] = True
    # TODO: dynamically calculate this
    max_len = 16
    # shm.max_fname[0] == 15
    shm.fill_shmem(shm.shm_fname, shm.f_names, max_len)
    shm.fill_shmem(shm.shm_lname, shm.l_names, max_len)

    shm.fill_shmem(shm.shm_words, shm.words, max_len)

    shm.shuffle_data(len(shm.f_names), max_len, const.SH_MEM_NAME_FNAME)

    shm.shuffle_data(
        len(shm.l_names),
        max_len,
        const.SH_MEM_NAME_LNAME,
    )

    shm.shuffle_data(len(shm.words), max_len, const.SH_MEM_NAME_WORDS)
    row_gen = RowGenerator(
        country="United States", file_name="test.csv", num_rows=NUM_ROWS, shm=shm
    )
    row_gen.row_builder()
    shm.shm_fname.close()
    shm.shm_lname.close()
    shm.shm_words.close()
    shm.shm_fname.unlink()
    shm.shm_lname.unlink()
    shm.shm_words.unlink()
    # with open("seeds.dat", "w+b") as f:
    #    pickle.dump(shm.seeds, f)
