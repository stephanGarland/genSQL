import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from random import random

class Runner:
    def __init__(self):
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
 
    def sample(self, iterable, n):
        reservoir = []
        for i in iterable:
            reservoir.append(i)
        
        reservoir_len = len(reservoir)
        
        for idx, item in enumerate(iterable):
            j = int(random() * reservoir_len)
            reservoir[j] = item
        return reservoir[:n]

    def make_row(self, rows: list):
        for i in range(1, args.num):
            rows.append(
                f"{','.join(self.sample(self.names, 2))},{i}\n"
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
            self.make_row(rows)
        with open("rows.txt", "w+") as f:
            f.writelines(rows)


if __name__ == "__main__":
    runner = Runner()
    args = runner.args()
    runner.run(args)
