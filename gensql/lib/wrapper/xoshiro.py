import ctypes

from gensql.utils.utilities import get_abs_path_to_so


def get_lib():
    lib = ctypes.CDLL(get_abs_path_to_so(__file__))
    lib.fill_array.argtypes = [
        ctypes.c_uint32,
        ctypes.c_uint64,
        ctypes.c_uint64,
    ]
    lib.fill_array.restype = ctypes.POINTER(ctypes.c_uint64)
    lib.free_array.argtypes = [ctypes.POINTER(ctypes.c_uint64)]
    lib.free_array.restype = None
    lib.seed_rng()

    return lib
