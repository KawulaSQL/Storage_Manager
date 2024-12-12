from typing import Dict, List, Tuple


class HashIndex:
    """
    Represents a hash-based index for a specific column in a table.
    """

    def __init__(self):
        self.index: Dict[int, List[Tuple[int, int]]] = {}

    def add(self, key: int, record_tuple: Tuple[int, int]) -> None:
        """
        Add a key-record pair to the hash index.
        """
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(record_tuple)

    def find(self, key: int) -> List[Tuple[int, int]]:
        """
        Retrieve record IDs matching a specific key.

        :param key: The hashed key to search for.
        :return: A list of record IDs.
        """
        return self.index.get(key, [])
