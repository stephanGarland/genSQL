from typing import Dict, List, Tuple


class ColumnReorder:
    def __init__(self, original_order: List[str], desired_order: List[str]):
        self.reorder_map = self._create_reorder_map(original_order, desired_order)

    def _create_reorder_map(
        self, original_order: List[str], desired_order: List[str]
    ) -> Dict[int, int]:
        return {desired_order.index(col): i for i, col in enumerate(original_order)}

    def reorder_chunk(self, chunk: Tuple[List[bytes], ...]) -> Tuple[List[bytes], ...]:
        return tuple(chunk[self.reorder_map[i]] for i in range(len(chunk)))
