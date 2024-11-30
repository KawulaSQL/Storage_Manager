import hashlib
from typing import Dict, List
import Block
from ..lib.Schema import Schema
import Attribute
from ..StorageManager import StorageManager
import pickle


class HashIndex:
    """
    Represents a hash-based index for a specific column in a table.
    """

    def __init__(self):
        self.index: Dict[int, List[int]] = {}

    def add(self, key: int, record_id: int) -> None:
        """
        Add a key-record pair to the hash index.

        :param key: The hashed key from the column value.
        :param record_id: The record's identifier.
        """
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(record_id)

    def find(self, key: int) -> List[int]:
        """
        Retrieve record IDs matching a specific key.

        :param key: The hashed key to search for.
        :return: A list of record IDs.
        """
        return self.index.get(key, [])


def set_index(table: str, column: str, index_type: str) -> None:
    """
    Create an index on a specified column of a table.

    :param table: The name of the table.
    :param column: The column to index.
    :param index_type: The type of index (baru hash).
    :raises ValueError: If the index type is unsupported.
    """
    if index_type != "hash":
        raise ValueError("Unsupported index type. Only 'hash' is implemented.")

    storageManager = StorageManager()
    schema = storageManager.get_table_schema(table)
    records = storageManager.get_table_data(table)
    metadata = schema.get_metadata()
    attrCount = -1
    for i in range (len(metadata)) :
        if metadata[i][0] == column :
            attrCount = i 

    if (attrCount == -1) :
        raise Exception("There is no such column!")
            
    hash_index = HashIndex()
    
    for i in range(len(records)) :
        key = int(hashlib.sha256(records[i][attrCount]).hexdigest(), 16) % (2**32)
        hash_index.add(key, i)

    path = table + "-" + column + ".pickle"

    with open(path, "wb") as file:  # Open in binary write mode
        pickle.dump(hash_index, file)


# Example usage
# Assuming `table.bin` is the table file and `name` is the column to index
index = set_index("table.bin", "name", "hash")
