import sqlite3

from utilities import constants

class SQLite:
    def __init__(self):
        self.conn = sqlite3.connect(constants.SQLITE_DB)
        self.cursor = self.conn.cursor()
