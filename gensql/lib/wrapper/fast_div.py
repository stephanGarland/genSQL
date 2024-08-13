import ctypes

from gensql.utils.utilities import get_abs_path_to_so


def get_lib() -> ctypes.CDLL:
    lib = ctypes.CDLL(get_abs_path_to_so(__file__))
    lib.precompute_mod_u32.argtypes = [ctypes.c_uint32]
    lib.precompute_mod_u32.restype = ctypes.c_uint64

    lib.fastdiv_u32.argtypes = [ctypes.c_uint32, ctypes.c_uint64]
    lib.fastdiv_u32.restype = ctypes.c_uint32

    lib.fastmod_u32.argtypes = [ctypes.c_uint32, ctypes.c_uint64, ctypes.c_uint32]
    lib.fastmod_u32.restype = ctypes.c_uint32

    lib.is_divisible.argtypes = [ctypes.c_uint32, ctypes.c_uint64]
    lib.is_divisible.restype = ctypes.c_bool

    return lib
