import sqlite3

from .constants import SQLITE_DB

class SQLiteMixin:
    def __init__(self):
        self.conn = sqlite3.connect(SQLITE_DB)
        self.cursor = self.conn.cursor()
