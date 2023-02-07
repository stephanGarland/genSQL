import ctypes
import numpy as np
from numpy.random import randint
import random
import sys

class Test:
    def __init__(self, size):
        self.rands = randint(2**32, size=size)

    def fast_rand(self, rng):
        rnd_32 = self.rands[rng-1]
        multiresult = rnd_32 * rng
        return multiresult >> 32

    def fast_shuffle(self, arr, size):
        for i in range(size, 0, -1):
            next_pos = self.fast_rand(i)
            tmp = arr[i-1]
            val = arr[next_pos]
            arr[i-1] = val
            arr[next_pos] = tmp

    def np_run(self):
        arr = np.arange(0, int(sys.argv[1]), dtype=np.uint32)
        g = np.random.default_rng()
        g.shuffle(arr)

    def fast_run(self):
        size = int(sys.argv[1])
        arr = [x for x in range(0, size)]
        self.fast_shuffle(arr, size)

    def run(self):
        size = int(sys.argv[1])
        arr = [x for x in range(0, size)]
        random.shuffle(arr)
class C:
    def __init__(self):
        lib = ctypes.CDLL("foo.so")
        lib.shuf.argtypes = [ctypes.POINTER(ctypes.c_uint), ctypes.c_uint]
        lib.fill_array(self.arr, len(self.arr))
        print(self.arr)
        self.arr_ptr = ctypes.byref(ctypes.c_uint32())
        lib.shuf(self.arr_ptr, 25)
        print(self.arr)

if __name__ == "__main__":
    #size = int(sys.argv[1])
    #t = Test(size)
    c = C()
    #if sys.argv[2] == "np":
    #    t.np_run()
    #elif sys.argv[2] == "c":
    #    t.fast_run()
    #else:
    #    t.run()

