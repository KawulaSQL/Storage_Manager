from typing import List, Tuple, Dict, Any
import json
import math

from lib.TableFileManager import TableFileManager
from lib.Schema import Schema
from lib.Attribute import Attribute
from lib.Condition import Condition


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

    def list_tables(self) -> List[str]:
        """
        Lists all table names stored in the information_schema.

        :return: A list of table names.
        """
        table_data = self.get_table_data("information_schema")
        return [table[0] for table in table_data]

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

        self.add_table_to_information_schema(table_name)

    def get_table_data(self, table_name: str, condition: Condition | None = None) -> List[Tuple]:
        # TODO: conditional select
        """
        Retrieves all records from a specified table.

        :param table_name: The name of the table to fetch data from.
        :return: A list of tuples containing the table records.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found.")

        return self.tables[table_name].read_table()

    def insert_into_table(self, table_name: str, values: List[Tuple[Any, ...]]) -> None:
        """
        Insert tuples of data into the specified table.

        :param table_name: Name of the table to insert to.
        :param values: Tuples of data to be inserted.
        """
        if table_name not in self.tables:
            raise ValueError(f"{table_name} not in database")
        self.tables[table_name].write_table(values)

    def update_table_record(self, table_name: str) -> int:
        # TODO: update_table_record, NOTE FUNGSI INI BAKAL DIUBAH LAGI PARAMETER/NAMANYA
        pass

    def delete_table(self, table_name: str) -> None:
        # TODO: delete_table
        """
        Deletes a table from the storage.

        :param table_name: The name of the table to delete.
        """
        pass

    def delete_table_record(self, table_name: str, condition: Condition) -> int:
        # TODO: delete_table_record
        """
        Deletes records from a table based on a condition.

        :param table_name: The name of the table to delete records from.
        :param Condition: The condition of delete.
        :return: numbers of records effected
        """
        pass

        return 0

    def get_stats(self):
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

    def add_table_to_information_schema(self, table_name: str) -> None:
        """
        Adds a table name to the information_schema table.

        :param table_name: Name of the table to add.
        """
        self.information_schema.write_table([(table_name,)])

    def get_table_schema(self, table_name):
        """
        Retrieves the schema of a specified table.

        :return: Schema of the table requested.
        """
        if table_name not in self.tables:
            raise ValueError(f"{table_name} not in database")
        return self.tables[table_name].schema
