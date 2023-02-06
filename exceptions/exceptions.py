from copy import deepcopy
import os
from pprint import pprint
from stat import filemode


class BaseError(Exception):
    """Base class for GenSQL exceptions"""

class SchemaValidationError(BaseError):
    """The provided schema is incorrectly formatted"""

    def __init__(self, errors):
        self.msg = ""
        output_dict = deepcopy(errors["schema"])
        for schema_col, schema_col_props in errors["schema"].items():
            for err, err_col_props in errors.items():
                err_key = f"\u2192 {err[0]}"
                err_val = f"\u21b3 {err[1]}"
                if err_col_props == schema_col_props:
                    output_dict[err_key] = errors["schema"][err[0]]
                    del output_dict[err[0]]
                    output_dict[err_key][err_val] = f"{errors[(err[0], err[1])]}"
                    del output_dict[err_key][err[1]]
        super(SchemaValidationError, self).__init__(self.msg)
        pprint(output_dict, width=120)

class OutputFilePermissionError(BaseError):
    """Lack permissions to write file to desired path"""
    def __init__(self, filename, msg=None):
        self.filename = filename
        self.dir_path = os.path.dirname(self.filename)
        self.dir_stat = os.stat(self.dir_path)
        self.dir_perms = filemode(self.dir_stat.st_mode)
        self.self_gid = os.getgid()
        self.self_uid = os.getuid()
        self.owner_gid = self.dir_stat.st_gid
        self.owner_uid = self.dir_stat.st_uid
        if msg:
            self.msg = msg
        else:
            self.msg = (
                f"unable to write to {self.filename}\n"
                f"target dir permissions: {self.dir_perms}\n"
                f"owner uid: {self.owner_uid}\t"
                f"self uid: {self.self_uid}\n"
                f"owner gid: {self.owner_gid}\t"
                f"self gid: {self.self_gid}"
            )
        super(OutputFilePermissionError, self).__init__(self.msg)

    def __reduce__(self):
        return (OutputFilePermissionError, self.msg)

class OverwriteFileError(BaseError):
    """Was asked to overwrite a file without having --force set"""

    def __init__(self, filename, msg=None):
        self.filename = filename
        if msg:
            self.msg = msg
        else:
            self.msg = f"--force not set, refusing to overwrite {filename}"
        super(OverwriteFileError, self).__init__(self.msg)

    def __reduce__(self):
        return (OverwriteFileError, self.msg)
