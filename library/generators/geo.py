from collections import deque
from math import ceil

from .base import BaseGenerator
from utilities.constants import (
    MIN_PHONE_NUMBER,
    MAX_PHONE_NUMBER,
    PHONE_NUMBERS,
)
from random import shuffle
from utilities import utilities

class Geo(BaseGenerator):
    def __init__(self, country: str, num_rows: int):
        super().__init__(num_rows)
        self.allocator = utilities.Allocator
        self.country = country
        q1 = f"""SELECT c.city, c.country FROM city c WHERE c.country = '{self.country}'"""
        self.cursor.execute(q1)
        cc = self.cursor.fetchall()
        self.city, self.country = zip(*cc)
        # only return countries with a matching city
        q2 = """SELECT DISTINCT cc.code, c.country FROM country cc JOIN city c ON c.country = cc.country"""
        self.cursor.execute(q2)
        result = self.cursor.fetchall()
        self.cc_map = {v:k.lower() for k,v in result}
        self._prepare_city()
        self._prepare_country()
        self._prepare_phone_allocator()
        self.conn.close()

    def _prepare_city(self):
        num_needed = ceil(self.num_rows / len(self.city))
        self.city = self.city * num_needed

    def _prepare_country(self):
        num_needed = ceil(self.num_rows / len(self.country))
        self.country = self.country * num_needed

    def _prepare_phone_allocator(self):
        deques_needed = ceil(self.num_rows / (MAX_PHONE_NUMBER - MIN_PHONE_NUMBER) * 2)
        self.random_phone = self.allocator(
            MIN_PHONE_NUMBER, MAX_PHONE_NUMBER, ranged_arr=True, shuffle=True
        )
        if deques_needed > 1:
            for _ in range(deques_needed):
                self.random_phone.ids += self.allocator(
                    MIN_PHONE_NUMBER,
                    MAX_PHONE_NUMBER,
                    ranged_arr=True,
                    shuffle=True,
                ).ids

    def make_city(self, *args):
        for i in range(0, self.num_rows, self.chunk_size):
            yield deque(self.city[i:i+self.chunk_size])

    def make_country(self, *args):
        for i in range(0, self.num_rows, self.chunk_size):
            yield deque(self.country[i:i+self.chunk_size])

    def make_phone(self, *args) -> deque:
        for i in range(0, self.num_rows, self.chunk_size):
            phones = deque()
            for _ in range(self.chunk_size):
                country = "".join(args[0])
                phone_val_1 = self.random_phone.allocate()
                phone_val_2 = self.random_phone.allocate()
                phone_str = f"{phone_val_1}{phone_val_2}"
                phone = PHONE_NUMBERS.get(self.cc_map[country], lambda x: x)(phone_str)
                phones.append(phone)

            yield phones