import argparse
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from random import random


class Allocate:
    def __init__(self, id_max: int):
        self.id_max = [x for x in range(1, id_max)]
        self.ids = deque(self.id_max)

    def allocate(self):
        try:
            return self.ids.popleft()
        except IndexError:
            return None


class Runner:
    def __init__(self):
        self.allocate = Allocate(10**6)
        with open("names.txt", "r") as f:
            self.names = f.read().splitlines()

    def args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-m",
            "--multi",
            action="store_true",
            help="Enable multiprocessing"
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

    # https://stackoverflow.com/a/42532968/4221094
    def sample(self, iterable, n):
        reservoir = []
        for t, item in enumerate(iterable):
            if t < n:
                reservoir.append(item)
            else:
                m = int(t * random())
                if m < n:
                    reservoir[m] = item
        return reservoir

    def make_row(self, rows: list):
        rows.append(
            f"{','.join(self.sample(self.names, 2))},{self.allocate.allocate()}\n"
        )

    def run(self, args):
        rows = []
        if args.threading:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [
                    executor.submit(self.make_row(rows), i) for i in range(args.num)
                ]
                wait(futures)
        elif args.multi:
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                futures = [
                    executor.submit(self.make_row(rows), i) for i in range(args.num)
                ]
                wait(futures)
        else:
            for i in range(args.num):
                self.make_row(rows)
        with open("rows.txt", "w+") as f:
            f.writelines(rows)


if __name__ == "__main__":
    runner = Runner()
    args = runner.args()
    runner.run(args)
