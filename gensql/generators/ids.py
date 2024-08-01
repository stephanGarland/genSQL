from .base import BaseGenerator


class SSN(BaseGenerator):
    def __init__(self, num_rows: int):
        super().__init__(num_rows)

    def generate_chunk(self, *args):
        pass
