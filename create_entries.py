import argparse
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from math import floor
from multiprocessing import Value
from os import urandom
import random

class Allocator:
    def __init__(self, id_max: int):
        self.id_max = [x for x in range(1, id_max)]
        self.ids = deque(self.id_max)

    def allocate(self) -> int|None:
        try:
            return self.ids.popleft()
        except IndexError:
            return None

class Runner:
    def __init__(self):
        with open("first_names.txt", "r") as f:
            self.first_names = f.read().splitlines()
        with open("last_names.txt", "r") as f:
            self.last_names = f.read().splitlines()

    def args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-m", "--multi", action="store_true", help="Enable multiprocessing"
        )
        parser.add_argument(
            "-n", "--num", type=int, default=1000, help="The number of rows to generate"
        )
        parser.add_argument(
            "-t",
            "--threading",
            action="store_true",
            help="Enable threading",
        )
        parser.add_argument(
            "-w",
            "--workers",
            type=int,
            default=8,
            help="The number of workers to spawn",
        )

        return parser.parse_args()

    def sample(self, iterable: list, num_rows: int) -> str:
        idx = floor(random.random() * num_rows)
        return iterable[idx]

    def make_row(self, rows: list):
        if args.threading or args.multi:
            allocate = Allocator(args.num)
        num_rows_first_names = len(self.first_names)
        num_rows_last_names = len(self.last_names)
        for i in range(1, args.num):
            random_first = self.sample(self.first_names, num_rows_first_names)
            random_last = self.sample(self.last_names, num_rows_last_names)
            if args.threading or args.multi:
                rows.append(f"{random_first},{random_last},{allocate.allocate()}\n")
            else:
                rows.append(f"{random_first},{random_last},{i}\n")

    def run(self, args: argparse.Namespace):
        rows = []
        random.seed(urandom(4))
        if args.threading:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(self.make_row(rows), i) for i in range(args.num)]
                wait(futures)
        elif args.multi:
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(self.make_row(rows), i) for i in range(args.num)]
                wait(futures)
        else:
            self.make_row(rows)
        with open("rows.txt", "w+") as f:
            f.writelines(rows)


if __name__ == "__main__":
    runner = Runner()
    args = runner.args()
    runner.run(args)
