from library.db.sqlite.connection import SQLite

class BaseGenerator(SQLite):
    def __init__(self, num_rows: int):
        super().__init__()
        self.chunk_size = num_rows // 10
        self.num_rows = num_rows
