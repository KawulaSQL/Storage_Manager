import sys
import os
from typing import List, Tuple, Any

from .RecordSerializer import RecordSerializer
from .Block import Block, BLOCK_SIZE
from .Schema import Schema
from .Condition import Condition
from .Expression import ExpressionParser

sys.path.append("./Failure_Recovery")
from Failure_Recovery.FailureRecoveryManager import FailureRecoveryManager


class TableFileManager:
    """
    Manages storage, retrieval, and management of tables stored as binary files.
    """

    base_path: str = "storage"

    def __init__(self, table_name: str, schema: Schema = None, block_size: int = BLOCK_SIZE) -> None:
        """
        Initialize the TableFileManager.

        :param table_name: The name of the table.
        :param schema: The schema of the table as a list of tuples (name, type, size).
        :param block_size: The size of each block, in bytes.
        :raises ValueError: If schema is not provided for a new table.
        """
        self.table_name: str = table_name

        if (table_name[0] == "'" and table_name[-1] == "'") :
            table_name = table_name[1:-1]

        self.block_size: int = block_size
        self.file_path: str = f"{TableFileManager.base_path}/{table_name}_table.bin"
        self.schema: Schema = schema if schema else Schema([])

        self.failure_recovery: FailureRecoveryManager = FailureRecoveryManager()  # NANTI KALO UDAH INTEGRASI DIJADIIN PARAMETER TERUS DIPASS SAMA PROCESSOR, JADI FAILUREMANAGER NYA SAMA, GA BIKIN BARU

        if os.path.exists(self.file_path):
            self.__read_header()
            self.serializer: RecordSerializer = RecordSerializer(self.schema.get_metadata())
        elif schema:
            self.schema: Schema = schema
            self.record_count: int = 0
            self.block_count: int = 1
            self.serializer: RecordSerializer = RecordSerializer(schema.get_metadata())
            self.__write_header()
        else:
            raise ValueError("Schema must be provided when creating a new table.")

    def init_file(self) -> None:
        """
        Initialize the table file.

        :return: None
        """
        with open(self.file_path, "wb") as _:
            pass

    def write_table(self, records: List[Tuple[Any, ...]]) -> None:
        """
        Write records to the binary file, appending new blocks as needed.

        :param records: A list of records, each as a tuple matching the schema.
        :return: None
        :raises ValueError: If a record cannot be serialized.
        """
        current_page = self.block_count - 1
        block = self.__get_block(current_page)

        for record in records:
            record_bytes = self.serializer.serialize(record)

            try:
                block.add_record(record_bytes)
            except ValueError:
                self.set_buffer(self.table_name, current_page, block)
                block = Block()
                current_page += 1
                self.block_count += 1
                block.reset_header()
                block.add_record(record_bytes)

        if block.header["record_count"] > 0:
            self.__set_block(current_page, block)

        self.record_count += len(records)
        self.__update_header()

    def read_table(self) -> List[Tuple[Any, ...]]:
        """
        Read all records from the binary file.

        :return: A list of records as tuples matching the schema.
        :raises ValueError: If a record cannot be deserialized.
        """
        records: List[Tuple[Any, ...]] = []
        current_block = 0

        while current_block < self.block_count:
            block: Block = self.__get_block(current_block)
            offset = 0

            if current_block == 0:
                header_length = int.from_bytes(block.data[4:8], byteorder='little')
                offset = header_length

            while offset < block.header["free_space_offset"]:
                record_bytes = bytearray()

                while block.data[offset] != 0xCC:
                    record_bytes.append(block.data[offset])
                    offset += 1

                record_bytes.append(block.data[offset])
                offset += 1

                record = self.serializer.deserialize(record_bytes)
                records.append(record)

            current_block += 1

        return records

    def delete_record(self, condition: Condition) -> int:
        """
        Delete records that match the specified condition within the existing blocks.

        :param condition: Condition to evaluate for deleting records
        :return: Number of rows deleted
        """
        rows_effected = 0
        current_block = 0

        rewrite_block = Block()
        rewrite_block_num = -1

        first_block = self.__get_block(0)
        header_length = int.from_bytes(first_block.data[4:8], byteorder='little')
        rewrite_block.add_record(first_block.data[0:header_length])

        attributes = [attr[0] for attr in self.schema.get_metadata()]

        while current_block < self.block_count:
            block: Block = self.__get_block(current_block)
            block.init_cursor()
            offset = 0

            if current_block == 0:
                header_length = int.from_bytes(block.data[4:8], byteorder='little')
                block.cursor = header_length
                offset = header_length

            while offset < block.header["free_space_offset"]:
                record_bytes = bytearray()

                while block.data[offset] != 0xCC:
                    record_bytes.append(block.data[offset])
                    offset += 1
                record_bytes.append(block.data[offset])
                offset += 1

                record = self.serializer.deserialize(record_bytes)

                context = {}
                for attr, value in zip(attributes, record):
                    context[attr] = value

                try:
                    should_delete = condition.evaluate(context)
                except ValueError as e:
                    raise ValueError(f"Error evaluating condition: {e}")

                if rewrite_block_num == -1 and should_delete:
                    rewrite_block_num = current_block
                    if current_block != 0:
                        header_length = int.from_bytes(block.data[4:8], byteorder='little')
                        rewrite_block.data[0:len(rewrite_block.data) - header_length] = rewrite_block.data[
                                                                                        header_length:]
                        rewrite_block.header["free_space_offset"] -= header_length

                if not should_delete:
                    serialized_record = self.serializer.serialize(record)

                    if rewrite_block.capacity() < len(serialized_record):
                        self.__set_block(rewrite_block_num, rewrite_block)

                        rewrite_block = Block()
                        rewrite_block_num += 1

                    rewrite_block.add_record(serialized_record)
                else:
                    rows_effected += 1

            current_block += 1

        if rewrite_block.header["record_count"] > 0:
            self.__set_block(rewrite_block_num, rewrite_block)

        for i in range(rewrite_block_num + 1, self.block_count) :
            empty_block = Block()
            self.__set_block(i, empty_block)

        self.block_count = rewrite_block_num + 1
        self.record_count -= rows_effected

        self.__update_header()

        return rows_effected

    def update_record(self, update_values: dict[str, Any], condition: Condition | None = None) -> int:
        """
        Update records that match the specified condition.

        :param update_values: Dictionary of column names and their new values to update
        :param condition: Condition to evaluate for updating records
        :return: Number of rows affected by the update operation
        """

        parser = ExpressionParser()

        records = self.read_table()
        rows_affected = 0
        attributes = [attr[0] for attr in self.schema.get_metadata()]

        updated_records = []

        for record in records:
            record_list = list(record)

            context = {}
            for attr, value in zip(attributes, record):
                context[attr] = value

            if not condition or condition.evaluate(context) :
                for col_name, new_value in update_values.items():
                    col_index = next(
                        (i for i, attr in enumerate(self.schema.attributes)
                            if attr.name == col_name),
                        None
                    )
                    if col_index is not None:
                        record_list[col_index] = parser.evaluate(new_value, context)
                rows_affected += 1

            updated_records.append(tuple(record_list))

        for i in range(self.block_count) :
            empty_block = Block()
            self.__set_block(i, empty_block)

        self.block_count = 1
        self.record_count = 0

        with open(self.file_path, 'wb') as _:
            pass

        self.__write_header()
        self.write_table(updated_records)

        return rows_affected

    def get_max_record_size(self) -> int:
        """
        Calculate the maximum size of a single record based on the schema.

        :return: The maximum size of a record, in bytes.
        :raises ValueError: If an unsupported data type is encountered.
        """
        record_size = 0
        for attr in self.schema.attributes:
            if attr.dtype == 'int':
                record_size += 4
            elif attr.dtype == 'float':
                record_size += 4
            elif attr.dtype == 'char':
                record_size += attr.size
            elif attr.dtype == 'varchar':
                record_size += 2 + attr.size
            else:
                raise ValueError(f"Unsupported data type: {attr.dtype}")
        return record_size

    def get_unique_attr_count(self):
        """
        Calculate the amount of unique values per attribute.

        :return: A JSON structured of key(table name) -> value(unique count).
        """
        records = self.read_table()
        attr_names = [attr[0] for attr in self.schema.get_metadata()]
        attr_record = [[] for _ in range(len(attr_names))]

        for rec in records:
            for i in range(len(rec)):
                attr_record[i].append(rec[i])

        attr_count = {}

        for i in range(len(attr_names)):
            attr_count[attr_names[i]] = len(
                list(set(attr_record[i])))  # Counts the length of the unique attribute records

        return attr_count

    def get_buffer(self, table_name: str, block_num: int) -> Block:
        return self.failure_recovery.buffer.get(table_name, block_num)

    def set_buffer(self, table_name: str, block_num: int, block: Block):
        self.failure_recovery.buffer.set(table_name, block_num, block)

    # ===== Private Functions =====

    def __write_header(self) -> None:
        """
        Write the table header to the binary file.

        :return: None
        """
        self.init_file()
        header = bytearray()

        # Add magic number
        header.extend(b"HEAD")

        # Placeholder for header length (to be updated later)
        header.extend((0).to_bytes(4, byteorder='little'))

        # Add metadata
        header.extend(self.record_count.to_bytes(4, byteorder='little'))
        header.extend(self.block_count.to_bytes(2, byteorder='little'))

        # Serialize schema
        schema_bytes = self.schema.serialize()
        schema_length = len(schema_bytes)
        header.extend(schema_length.to_bytes(2, byteorder='little'))  # Length of schema in bytes
        header.extend(len(self.schema.attributes).to_bytes(2, byteorder='little'))  # Number of attributes
        header.extend(schema_bytes)

        # Add sentinel
        header.extend(b"\xCC")

        # Calculate and update header length
        header_length = len(header)
        header[4:8] = header_length.to_bytes(4, byteorder='little')

        # Write header to the first block
        block = Block()
        block.add_record(header)
        block.write_block(self.file_path, 0)
        self.set_buffer(self.table_name, 0, block)

    def __read_header(self) -> None:
        """
        Read and parse the table header from the binary file.

        :return: None
        :raises ValueError: If the header is invalid.
        """
        block = self.__get_block(0)

        magic = block.read(4)
        if magic != b"HEAD":
            raise ValueError("Invalid table file: missing header.")

        header_length = int.from_bytes(block.read(4), byteorder='little')

        self.record_count = int.from_bytes(block.read(4), byteorder='little')
        self.block_count = int.from_bytes(block.read(2), byteorder='little')

        schema_length = int.from_bytes(block.read(2), byteorder='little')
        num_attributes = int.from_bytes(block.read(2), byteorder='little')

        schema_data = block.read(schema_length)
        self.schema = Schema.deserialize(schema_data)

        if len(self.schema.attributes) != num_attributes:
            raise ValueError("Schema attribute count does not match expected value.")

        sentinel = block.read(1)
        if sentinel != b"\xCC":
            raise ValueError("Invalid table file: missing sentinel.")
        
    def __get_block(self, block_number) -> Block :
        block = self.get_buffer(self.table_name, block_number)
        if block is None:
            block = Block.read_block(self.file_path, block_number)
            self.set_buffer(self.table_name, block_number, block)
        return block

    def __set_block(self, block_number, block: Block) -> None :
        self.set_buffer(self.table_name, block_number, block)
        block.write_block(self.file_path, block_number)

    def __update_header(self) -> None:
        """
        Update the metadata in the table header.

        :return: None
        """
        block = self.__get_block(0)
        block.data[8:12] = self.record_count.to_bytes(4, "little")
        block.data[12:14] = self.block_count.to_bytes(2, "little")
        block.write_block(self.file_path, 0)
        self.__set_block(0, block)
