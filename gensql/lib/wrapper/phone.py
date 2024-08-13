import ctypes

from gensql.utils.utilities import get_abs_path_to_so


class UInt128(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint64), ("high", ctypes.c_uint64)]


def get_lib() -> ctypes.CDLL:
    lib = ctypes.CDLL(get_abs_path_to_so(__file__))

    lib.precompute_mod_u64.argtypes = [ctypes.c_uint64]
    lib.precompute_mod_u64.restype = UInt128

    lib.fill_array.argtypes = [
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.c_uint32,
        ctypes.c_bool,
    ]
    lib.fill_array.restype = ctypes.POINTER(ctypes.c_char)

    lib.free_array.argtypes = [ctypes.POINTER(ctypes.c_char)]
    lib.free_array.restype = None

    return lib
