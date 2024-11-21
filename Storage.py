import os

from model.Serializer import Serializer
from model.Model import DataWrite, DataRetrieval


class Storage:
    """
    Storage Manager for DBMS.
    """

    def __init__(self):
        self.storage_path = './storage'

    def write_block(self, data: DataWrite) -> int:
        """
        Write data to storage
        """

        # file path
        table_file = os.path.join(self.storage_path, f"{data.table}.bin")

        # check if file exist
        if not os.path.exists(table_file):
            raise FileNotFoundError(f"Table file '{data.table}' not found.")

        if data.query_type == "update":
            pass

        elif data.query_type == "insert":
            # Open the file to add data
            with open(table_file, 'ab') as file:
                row_added = 0

                # Convert each new value to binary and write to file
                for value in data.new_value:
                    binary_data = Serializer.serialize(value)
                    file.write(binary_data)
                    row_added += 1

            return row_added

        else:
            raise ValueError(f"Unsupported query type {data.query_type}.")

    def read_block(self, data_retrieval: DataRetrieval):
        pass

