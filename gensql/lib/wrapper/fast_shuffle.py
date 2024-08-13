import ctypes

from gensql.utils.utilities import get_abs_path_to_so


def get_lib() -> ctypes.CDLL:
    lib = ctypes.CDLL(get_abs_path_to_so(__file__))
    lib.shuf.argtypes = [
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_uint32,
        ctypes.c_uint32,
    ]
    return lib
