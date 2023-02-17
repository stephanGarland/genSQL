import ctypes

ARR_LEN = 3628800

lib = ctypes.CDLL("./permutate.so")
lib.generate_permutations.restype = ctypes.POINTER(ctypes.c_int)

digits_list_ptr = lib.generate_permutations()
digits = (ctypes.c_int * ARR_LEN).from_address(ctypes.addressof(digits_list_ptr.contents))

breakpoint()
