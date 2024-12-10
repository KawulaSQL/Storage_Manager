import os
from typing import List, Tuple, Dict, Any
import json
import math

from lib import Block
from lib.TableFileManager import TableFileManager
from lib.Schema import Schema
from lib.Attribute import Attribute
from lib.Condition import Condition
from lib.Index import HashIndex
from lib.Block import Block

import hashlib
import pickle
import struct
import os

class StorageManager:
    """
    Representation of Storage Manager Component of a DBMS
    """

    def __init__(self, base_path: str) -> None:
        """
        Initialize the StorageManager with the given base path.
        Ensures the information_schema table exists.

        :param base_path: Path where table files are stored.
        """
        self.base_path: str = base_path
        TableFileManager.base_path = base_path

        self.information_schema: TableFileManager
        self.tables: Dict[str, TableFileManager] = {}

        self._initialize_information_schema()

        self.index: HashIndex

    def _initialize_information_schema(self) -> None:
        """
        Initialize or load the information_schema table that holds table names.
        """
        schema = Schema([Attribute("table_name", "varchar", 50)])
        self.information_schema = TableFileManager("information_schema", schema)
        self.tables["information_schema"] = self.information_schema

        table_names = self.get_table_data("information_schema")
        for table_name in table_names:
            table_name = table_name[0]
            self.tables[table_name] = TableFileManager(table_name)

    def create_table(self, table_name: str, schema: Schema) -> None:
        """
        Creates a new table with the given name and schema.

        :param table_name: Name of the table to create.
        :param schema: The schema object that defines the table structure.
        """
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists.")

        table_manager = TableFileManager(table_name, schema)
        self.tables[table_name] = table_manager

        self.__add_table_to_information_schema(table_name)

    def get_table_data(
        self, table_name: str, condition: Condition | None = None
    ) -> List[Tuple[Any, ...]]:
        """
        Retrieves all records from a specified table.

        :param condition: Condition
        :param table_name: The name of the table to fetch data from.
        :return: A list of tuples containing the table records.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found.")

        records = self.tables[table_name].read_table()
        attributes = [
            col[0] for col in self.get_table_schema(table_name).get_metadata()
        ]
        types = [col[1] for col in self.get_table_schema(table_name).get_metadata()]

        if condition and table_name != "information_schema":
            try:
                if condition.operand1["isAttribute"]:
                    col_1 = attributes.index(condition.operand1["value"])
                    condition.operand1["type"] = types[col_1]
                else:
                    col_1 = condition.operand1["value"]

                if condition.operand2["isAttribute"]:
                    col_2 = attributes.index(condition.operand2["value"])
                    condition.operand1["type"] = types[col_2]
                else:
                    col_2 = condition.operand2["value"]

            except Exception:
                raise ValueError("There's an error in the column input")

            # print(condition.operand1, condition.operand2) # testing purposes

            
            if condition.operand1["type"] != condition.operand2["type"]:
                raise ValueError(
                    f"TypeError: {condition.operand1['type']} with {condition.operand2['type']}"
                )

            temp_rec = []
            if condition.operand1["isAttribute"]:
                if condition.operand2["isAttribute"]:
                    for i in range(len(records)):
                        if condition.evaluate(records[i][col_1], records[i][col_2]):
                            temp_rec.append(records[i])

                else:  # Either col_2 string or integer
                    is_indexed = self.get_index(table_name, condition.operand1["value"], condition.operand2["value"], condition.operand2["type"])
                    print("is_indexed = ", is_indexed) 
                    if (is_indexed == None) :
                        for i in range(len(records)):
                            if condition.evaluate(records[i][col_1], col_2):
                                temp_rec.append(records[i])
                    else :
                        temp_rec = is_indexed
            else:  # Either col_1 string or integer
                if condition.operand2["isAttribute"]:
                    is_indexed = self.get_index(table_name, condition.operand2["value"], condition.operand1["value"], condition.operand1["type"])
                    print("is_indexed = ", is_indexed) 
                    if (is_indexed == None) :
                        for i in range(len(records)):
                            if condition.evaluate(col_1, records[i][col_2]):
                                temp_rec.append(records[i])
                    else :
                        temp_rec = is_indexed
                else:  # Either col_2 string or integer
                    for i in range(len(records)):
                        if condition.evaluate(col_1, col_2):
                            temp_rec.append(records[i])
            records = temp_rec

        return records

    def update_table_record(self, table_name: str, condition: Condition) -> int:
        # TODO: update_table_record, NOTE FUNGSI INI BAKAL DIUBAH LAGI PARAMETER/NAMANYA
        pass

    def insert_into_table(self, table_name: str, values: List[Tuple[Any, ...]]) -> None:
        """
        Insert tuples of data into the specified table.

        :param table_name: Name of the table to insert to.
        :param values: Tuples of data to be inserted.
        """
        if table_name not in self.tables:
            raise ValueError(f"{table_name} not in database")
        self.tables[table_name].write_table(values)

    def delete_table(self, table_name: str) -> None:
        """
        Deletes a table from the storage.

        :param table_name: The name of the table to delete.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found.")

        # remove table from information_schema
        self.delete_table_record(
            "information_schema", Condition("table_name", "=", f"'{table_name}'")
        )

        # delete table file manager
        self.tables.pop(table_name)

        # remove table from storage
        table_file = f"{self.base_path}/{table_name}_table.bin"
        try:
            os.remove(table_file)
            print("Table deleted successfully.")
        except FileNotFoundError:
            raise ValueError(f"Table {table_name} not found.")

    def delete_table_record(self, table_name: str, condition: Condition) -> int:
        """
        Deletes records from a table based on a condition.

        :param condition: The condition of delete.
        :param table_name: The name of the table to delete records from.
        :return: numbers of records effected
        """
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found.")

        col_names = [col[0] for col in self.get_table_schema(table_name).get_metadata()]
        print(col_names)

        if condition.operand1["isAttribute"]:
            if condition.operand1["value"] not in col_names:
                raise ValueError(f"Column {condition.operand1['value']} not found.")
            col_1 = col_names.index(condition.operand1["value"])

            if condition.operand2["isAttribute"]:
                if condition.operand2["value"] not in col_names:
                    raise ValueError(f"Column {condition.operand2['value']} not found.")
                col_2 = col_names.index(condition.operand1["value"])
            else:
                col_2 = condition.operand2

            return self.tables[table_name].delete_record(col_1, col_2, condition)

        else:
            raise ValueError(f"Column {condition.operand1['value']} not found.")

    def get_table_schema(self, table_name):
        """
        Retrieves the schema of a specified table.

        :return: Schema of the table requested.
        """
        if table_name not in self.tables:
            raise ValueError(f"{table_name} not in database")
        return self.tables[table_name].schema

    def list_tables(self) -> List[str]:
        """
        Lists all table names stored in the information_schema.

        :return: A list of table names.
        """
        table_data = self.get_table_data("information_schema")
        return [table[0] for table in table_data]

    def get_stats(self) -> str:
        """
        Gets the statistic of every table in a storage.

        A table statistic consists of:
        nr: number of tuples in a relation r.
        br: number of blocks containing tuples of r.
        lr: size of tuple of r.
        fr: blocking factor of r - i.e., the number of tuples of r that fit into one block.
        V(A,r): number of distinct values that appear in r for attribute A; same as the size of A(r).

        :return: A JSON structured of key(table name) -> value(table statistic).
        """
        stats = {}
        for table_name, tb_manager in self.tables.items():
            if table_name != "information_schema":
                table_stats = {
                    "n_r": tb_manager.record_count,
                    "b_r": tb_manager.block_count,
                    "l_r": tb_manager.get_max_record_size(),
                    "f_r": math.ceil(tb_manager.record_count / tb_manager.block_count),
                    "v_a_r": tb_manager.get_unique_attr_count(),
                }

                stats[table_name] = table_stats

        return json.dumps(stats, indent=4)

    def write_buffer(self):
        # TODO: write buffer add params
        """
        Writes buffer to disk with the given identifier.

        :param identifier: block identifier for file name.
        """
        pass

    def read_buffer(self):
        # TODO: read buffer add params
        """
        Reads buffer from disk with the given identifier.
        """
        pass

    # ===== Private Methods ===== #

    def __initialize_information_schema(self) -> None:
        """
        Initialize or load the information_schema table that holds table names.
        """
        schema = Schema([Attribute("table_name", "varchar", 50)])
        self.information_schema = TableFileManager("information_schema", schema)
        self.tables["information_schema"] = self.information_schema

        table_names = self.get_table_data("information_schema")
        for table_name in table_names:
            table_name = table_name[0]
            self.tables[table_name] = TableFileManager(table_name)

    def __add_table_to_information_schema(self, table_name: str) -> None:
        """
        Adds a table name to the information_schema table.

        :param table_name: Name of the table to add.
        """
        self.information_schema.write_table([(table_name,)])

    def set_index(self, table_name: str, column: str, index_type: str) -> None:
        """
        Create an index on a specified column of a table.

        :param table_name: The name of the table.
        :param column: The column to index.
        :param index_type: The type of index (baru hash).
        :raises ValueError: If the index type is unsupported.
        """
        if table_name not in self.tables:
            raise ValueError(f"{table_name} not in database")

        if index_type != "hash":
            raise ValueError("Unsupported index type. Only 'hash' is implemented.")

        schema = self.get_table_schema(table_name)
        metadata = schema.get_metadata()
        # records = self.get_table_data(table)
        attrCount = -1
        dtype = "null"
        for i in range(len(metadata)):
            if metadata[i][0] == column:
                attrCount = i
                dtype = metadata[i][1]

        if attrCount == -1:
            raise Exception("There is no such column!")

        self.index = HashIndex()
        tfm = TableFileManager(table_name, schema)
        # records: List[Tuple[Any, ...]] = []
        current_block = 0

        while current_block < tfm.block_count:
            block = Block.read_block(tfm.file_path, current_block)
            offset = 0

            if current_block == 0:
                header_length = int.from_bytes(block.data[4:8], byteorder="little")
                offset = header_length

            while offset < block.header["free_space_offset"]:
                record_bytes = bytearray()

                offset_note = offset
                while block.data[offset] != 0xCC:
                    record_bytes.append(block.data[offset])
                    offset += 1

                record_bytes.append(block.data[offset])
                offset += 1

                record = tfm.serializer.deserialize(record_bytes)
                # records.append(record)

                print("isi record : ")
                print(record)
                # record = tfm.serializer.serialize(record)
                if dtype == "int":
                    key = int(
                        hashlib.sha256(int(record[attrCount]).to_bytes()).hexdigest(),
                        16,
                    ) % (2**32)
                elif dtype == "float":
                    key = int(
                        hashlib.sha256(
                            struct.pack("f", float(record[attrCount]))
                        ).hexdigest(),
                        16,
                    ) % (2**32)
                elif dtype == "char":
                    key = int(
                        hashlib.sha256(
                            str(record[attrCount]).encode("utf-8")
                        ).hexdigest(),
                        16,
                    ) % (2**32)
                elif dtype == "varchar":
                    key = int(
                        hashlib.sha256(
                            str(record[attrCount]).encode("utf-8")
                        ).hexdigest(),
                        16,
                    ) % (2**32)
                # key = int(hashlib.sha256(record[attrCount]).hexdigest(), 16) % (2**32)
                self.index.add(key, (current_block, offset_note))

            current_block += 1

        # path = "./storage" + table_name + "-" + column + "-hash" + ".pickle"
        path = os.path.join("./storage", f"{table_name}-{column}-hash.pickle")

        with open(path, "wb") as file:  # Open in binary write mode
            pickle.dump(self.index, file)

    def get_index(self, table: str, column: str, value: str | int | float, dtype: str) :
        print("masuk getIndex")
        file_path = os.path.join("./storage", f"{table}-{column}-hash.pickle")
        if (not (os.path.isfile(file_path))) :
            return None 
        
        with open(file_path, "rb") as file:  # Open in binary read mode
            self.index = pickle.load(file)
        
        if dtype == "int":
            key = int(hashlib.sha256(int(value).to_bytes()).hexdigest(), 16) % (2**32)
        elif dtype == "float":
            key = int(hashlib.sha256(struct.pack('f', float(value))).hexdigest(), 16) % (2**32)
        elif dtype == "char":
            key = int(hashlib.sha256(str(value).encode('utf-8')).hexdigest(), 16) % (2**32)
        elif dtype == "varchar":
            key = int(hashlib.sha256(str(value).encode('utf-8')).hexdigest(), 16) % (2**32)
        
        index_result = self.index.find(key)
        print("index_result : ", index_result) 
        
        schema = self.get_table_schema(table)
        # metadata = schema.get_metadata()
        tfm = TableFileManager(table, schema)

        result = []
        for i in range(len(index_result)) :
            current_block = index_result[i][0]
            offset = index_result[i][1]
            block = Block.read_block(tfm.file_path, current_block)
            
            record_bytes = bytearray()

            while block.data[offset] != 0xCC:
                record_bytes.append(block.data[offset])
                offset += 1

            record_bytes.append(block.data[offset])

            record = tfm.serializer.deserialize(record_bytes)

            result.append(record)
            print("record : ", record)
        
        return result
        
# sm = StorageManager("./storage")
# sm.set_index("coba", "nilai", "hash")

    def update_table(self, table_name: str, condition: Condition, update_values: dict):
        """Update records in a table based on a condition."""
        table_file_manager = self.tables[table_name]
        
        schema = table_file_manager.schema
        col_1_index = next(
            (i for i, attr in enumerate(schema.attributes) 
            if attr.name == condition.operand1['value']), 
            None
        )
        
        if col_1_index is None:
            raise ValueError(f"Column {condition.operand1['value']} not found in table")
        
        if condition.operand2['isAttribute']:
            col_2 = next(
                (i for i, attr in enumerate(schema.attributes) 
                if attr.name == condition.operand2['value']), 
                None
            )
            if col_2 is None:
                raise ValueError(f"Column {condition.operand2['value']} not found in table")
        else:
            col_2 = condition.operand2
        
        return table_file_manager.update_record(col_1_index, col_2, condition, update_values)


# if __name__ == "__main__":
#     storage = StorageManager("storage")
#     storage.create_table("test", Schema([Attribute("id", "int", None), Attribute("name", "varchar", 50)]))
#     storage.insert_into_table("test", [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David"), (5, "Eve")])
#
#     print(storage.get_table_data("test"))
#
#     storage.delete_table("test")
#     # storage.delete_table_record("test", Condition("id", "=", "2"))
#
#     try:
#         print(storage.get_table_data("test"))
#     except ValueError as e:
#         print(e)
#
#     print(storage.list_tables())
