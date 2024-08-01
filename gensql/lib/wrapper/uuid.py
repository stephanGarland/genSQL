import ctypes

from gensql.utils.utilities import get_abs_path_to_so


def get_lib():
    lib = ctypes.CDLL(get_abs_path_to_so(__file__))
    lib.fill_array.argtypes = [
        ctypes.c_uint32,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint32,
    ]
    lib.fill_array.restype = ctypes.POINTER(ctypes.c_char_p)
    lib.free_array.argtypes = [ctypes.POINTER(ctypes.c_char_p)]
    lib.free_array.restype = None

    return lib
