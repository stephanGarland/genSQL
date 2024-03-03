from collections import deque
from math import ceil

from .base import BaseGenerator

from utilities import utilities

class Email(BaseGenerator):
    def __init__(self, num_rows: int):
        super().__init__(num_rows)
        q1 = f"""SELECT word FROM word_list"""
        self.cursor.execute(q1)
        self.sample = utilities.Utilities.sample
        self.word_list = [x[0] for x in self.cursor.fetchall()]
        self.len_word_list = len(self.word_list)
        self.conn.close()

    def make_email(self, *args):
        for i in range(0, self.num_rows, self.chunk_size):
            word_list = deque()
            first_names = [x.lower() for x in args[0][0].first_names]
            last_names = [x.lower() for x in args[0][1].last_names]
            for j in range(self.chunk_size):
                random_email_domain = self.sample(self, self.word_list, self.len_word_list)
                word_list.append(f"{first_names[j]}.{last_names[j]}@{random_email_domain}.com")
            yield word_list

