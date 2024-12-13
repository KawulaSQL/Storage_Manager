from typing import List, Tuple, Dict, Any
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

        self.__initialize_information_schema()

        self.index: HashIndex | None = None

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

    def get_table_data(self, table_name: str, condition: Condition | None = None,
                    projection=None) -> List[Tuple[Any, ...]]:
        """
        Retrieves all records from a specified table.

        :param condition: Condition
        :param table_name: The name of the table to fetch data from.
        :param projection: The name of columns selected to be displayed.
        :return: A list of tuples containing the table records.
        """
        if projection is None:
            projection = []
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found.")

        records = self.tables[table_name].read_table()
        attributes = [col[0] for col in self.get_table_schema(table_name).get_metadata()]
        types = [col[1] for col in self.get_table_schema(table_name).get_metadata()]

        if condition and table_name != "information_schema":
            temp_rec = []
            for record in records:
                context = {}
                for attr, value in zip(attributes, record):
                    context[attr] = value

                try:
                    if condition.evaluate(context):
                        temp_rec.append(record)
                except ValueError as e:
                    raise ValueError(f"Error evaluating condition: {e}")

            records = temp_rec

        if len(projection) > 0 and table_name != "information_schema":
            for att in projection:
                if att not in attributes:
                    raise ValueError(f"The column {att} is not in {table_name}")

            filtered_records = []
            for record in records:
                record_holder = []
                for att in projection:
                    record_holder.append(record[attributes.index(att)])
                filtered_records.append(tuple(record_holder))
            records = filtered_records

        return records
    
    def get_joined_table(self, table_names: List[str], join_attributes: List[Tuple[str, str]], table_conditions: List[Condition], global_condition: Condition | None, projection: List[str] = None):
        """
        Retrieves records from multiple tables joined on attribute. Join ordering is based on the order of the tables in table_names

        :param table_names: Name of the tables to be joined.
        :param join_attributes: List of joined attributes written with the syntax ('table_name.attribute_name', 'table2_name.attribute_name')
        :param table_conditions: Where clause of the initial data fetched from each table
        :param global_condition: Where clause involving attributes from multiple table
        :param projection: Selected columns
        :return: A list of tuples containing the joined table records
        """

        if len(table_names) < 2:
            raise ValueError("At least two tables are required for join operation")
        
        if len(table_names) != len(table_conditions):
            raise ValueError("Number of table conditions must be equal number of table")
        
        if len(join_attributes) != len(table_names) - 1:
            raise ValueError("Number of join attributes must be one less than number of tables")

        table_records = []
        table_schemas = []
        for i, table_name in enumerate(table_names):
            condition = table_conditions[i-1] if i > 0 else None
            records = self.get_table_data(table_name, condition)
            
            schema = self.get_table_schema(table_name).get_metadata()
            attributes = [col[0] for col in schema]
            
            table_records.append(records)
            table_schemas.append((table_name, attributes))

        result_records = []
        
        def parse_table_attribute(table_attr: str) -> Tuple[str, str]:
            """Parse table.attribute format"""
            parts = table_attr.split('.')
            if len(parts) != 2:
                raise ValueError(f"Invalid attribute format: {table_attr}")
            return parts[0], parts[1]

        result_records = table_records[0]
        table1_name, table1_attr = parse_table_attribute(join_attributes[0][0])
        result_records = table_records[table_names.index(table1_name)]
        table1_idx = next(j for j, (name, attrs) in enumerate(table_schemas) if name == table1_name)
        result_attr = list(map(lambda x : f"{table1_name}.{x}", table_schemas[table1_idx][1]))

        processed_tables = {table1_name}

        for i in range(1, len(table_names)):
            table1_name, table1_attr = parse_table_attribute(join_attributes[i-1][0])
            table2_name, table2_attr = parse_table_attribute(join_attributes[i-1][1])

            if (table2_name in processed_tables) :
                table1_name, table1_attr, table2_name, table2_attr = table2_name, table2_attr, table1_name, table1_attr
            elif (table1_name not in processed_tables) :
                raise ValueError("Bad Join Order")

            table2_idx = next(j for j, (name, attrs) in enumerate(table_schemas) if name == table2_name)

            table1_attr_idx = result_attr.index(f"{table1_name}.{table1_attr}")
            table2_attr_idx = table_schemas[table2_idx][1].index(table2_attr)

            new_result_records = []
            for r1 in result_records:
                for r2 in table_records[table2_idx]:
                    if r1[table1_attr_idx] == r2[table2_attr_idx]:
                        new_result_records.append(r1 + r2)

            result_records = new_result_records
            result_attr += list(map(lambda x : f"{table2_name}.{x}", table_schemas[table2_idx][1]))
            processed_tables.add(table2_name)

        if global_condition:
            filtered_records = []
            for record in result_records:
                context = {}
                for j, attr in enumerate(result_attr):
                    context[attr] = record[j]

                try:
                    if global_condition.evaluate(context):
                        filtered_records.append(record)
                except ValueError as e:
                    raise ValueError(f"Error evaluating global condition: {e}")

            result_records = filtered_records

        if projection :
            result_records = list(filter(lambda x : x in projection, result_records))

        return (result_records, projection if projection else result_attr)

    def insert_into_table(self, table_name: str, values: List[Tuple[Any, ...]]) -> int:
        """
        Insert tuples of data into the specified table.

        :param table_name: Name of the table to insert to.
        :param values: Tuples of data to be inserted.
        """
        if table_name not in self.tables:
            raise ValueError(f"{table_name} not in database")
        self.tables[table_name].write_table(values)

        self.update_index(table_name)
        return len(values)

    def delete_table(self, table_name: str) -> None:
        """
        Deletes a table from the storage.

        :param table_name: The name of the table to delete.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found.")
        
        self.delete_index(table_name)

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
    

    def delete_table_record(self, table_name: str, condition: Condition | None = None) -> int:
        """
        Deletes records from a table based on a condition.

        :param condition: The condition of delete.
        :param table_name: The name of the table to delete records from.
        :return: numbers of records effected
        """
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found.")
        
        returned = self.tables[table_name].delete_record(condition)
        self.update_index(table_name)
        return returned

    def update_table(self, table_name: str, update_values: dict, condition: Condition | None = None) -> int:
        """Update records in a table based on a condition."""
        table_file_manager = self.tables[table_name]
        return table_file_manager.update_record(update_values, condition)

    def get_table_schema(self, table_name) -> Schema:
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

    def get_stats(self) -> dict[str, dict[str, int | Any]]:
        """
        Gets the statistic of every table in a storage.

        A table statistic consists of:
        n_r: number of tuples in a relation r.
        b_r: number of blocks containing tuples of r.
        l_r: size of tuple of r.
        f_r: blocking factor of r - i.e., the number of tuples of r that fit into one block.
        v_a_r: number of distinct values that appear in r for attribute A; same as the size of A(r).

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

        return stats

    def update_index(self, table_name: str) -> None:
        if table_name not in self.tables:
            raise ValueError(f"{table_name} not in database")

        schema = self.get_table_schema(table_name)
        metadata = schema.get_metadata()

        for i in range(len(metadata)):
            file_path = os.path.join("./storage", f"{table_name}-{metadata[i][0]}-hash.pickle")
            if os.path.isfile(file_path):
                self.set_index(table_name, metadata[i][0], "hash")

    def delete_index(self, table_name: str) -> None:
        if table_name not in self.tables:
            raise ValueError(f"{table_name} not in database")

        schema = self.get_table_schema(table_name)
        metadata = schema.get_metadata()

        for i in range(len(metadata)):
            file_path = os.path.join("./storage", f"{table_name}-{metadata[i][0]}-hash.pickle")
            if os.path.isfile(file_path):
                os.remove(file_path)

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
        attr_count = -1
        dtype = "null"
        for i in range(len(metadata)):
            if metadata[i][0] == column:
                attr_count = i
                dtype = metadata[i][1]

        if attr_count == -1:
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
                        hashlib.sha256(int(record[attr_count]).to_bytes()).hexdigest(),
                        16,
                    ) % (2 ** 32)
                elif dtype == "float":
                    key = int(
                        hashlib.sha256(
                            struct.pack("f", float(record[attr_count]))
                        ).hexdigest(),
                        16,
                    ) % (2 ** 32)
                elif dtype == "char":
                    key = int(
                        hashlib.sha256(
                            str(record[attr_count]).encode("utf-8")
                        ).hexdigest(),
                        16,
                    ) % (2 ** 32)
                elif dtype == "varchar":
                    key = int(
                        hashlib.sha256(
                            str(record[attr_count]).encode("utf-8")
                        ).hexdigest(),
                        16,
                    ) % (2 ** 32)
                else:
                    raise ValueError("Unsupported Data Type")

                # key = int(hashlib.sha256(record[attr_count]).hexdigest(), 16) % (2**32)
                self.index.add(key, (current_block, offset_note))

            current_block += 1

        # path = "./storage" + table_name + "-" + column + "-hash" + ".pickle"
        path = os.path.join("./storage", f"{table_name}-{column}-hash.pickle")

        with open(path, "wb") as file:  # Open in binary write mode
            pickle.dump(self.index, file)

    def get_index(self, table_name: str, column: str, value: str | int | float, dtype: str):
        print("masuk getIndex")
        file_path = os.path.join("./storage", f"{table_name}-{column}-hash.pickle")
        if not (os.path.isfile(file_path)):
            return None

        with open(file_path, "rb") as file:  # Open in binary read mode
            self.index = pickle.load(file)

        if dtype == "int":
            key = int(hashlib.sha256(int(value).to_bytes()).hexdigest(), 16) % (2 ** 32)
        elif dtype == "float":
            key = int(hashlib.sha256(struct.pack('f', float(value))).hexdigest(), 16) % (2 ** 32)
        elif dtype == "char":
            key = int(hashlib.sha256(str(value).encode('utf-8')).hexdigest(), 16) % (2 ** 32)
        elif dtype == "varchar":
            key = int(hashlib.sha256(str(value).encode('utf-8')).hexdigest(), 16) % (2 ** 32)
        else:
            raise ValueError("Unsupported Data Type")

        index_result = self.index.find(key)
        print("index_result : ", index_result)

        schema = self.get_table_schema(table_name)
        metadata = schema.get_metadata()
        attr_count = -1
        for i in range(len(metadata)):
            if metadata[i][0] == column:
                attr_count = i

        tfm = TableFileManager(table_name, schema)

        result = []
        for i in range(len(index_result)):
            current_block = index_result[i][0]
            offset = index_result[i][1]
            block = Block.read_block(tfm.file_path, current_block)

            record_bytes = bytearray()

            while block.data[offset] != 0xCC:
                record_bytes.append(block.data[offset])
                offset += 1

            record_bytes.append(block.data[offset])

            record = tfm.serializer.deserialize(record_bytes)

            if record[attr_count] == value:
                result.append(record)
            print("record : ", record)

        return result

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
            if (table_name[0] == "'" and table_name[-1] == "'") :
                table_name = table_name[1:-1]
            self.tables[table_name] = TableFileManager(table_name)

    def __add_table_to_information_schema(self, table_name: str) -> None:
        """
        Adds a table name to the information_schema table.

        :param table_name: Name of the table to add.
        """
        self.information_schema.write_table([(table_name,)])
