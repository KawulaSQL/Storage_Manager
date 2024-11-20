import struct
from typing import Generic, TypeVar

# Type for Generic class
T = TypeVar('T')


class Condition:
    """
    Query Condition.
    """

    def __init__(self, column, operation, operand):
        self.column: str = column
        self.operation: str = operation  # enum {'=', '<>', '>', '>=', '<', '<='}
        self.operand: int | str = operand


class DataRetrieval:
    """
    Data Retrieval Query Representation.
    """

    def __init__(self, tables: list[str], column: list[str], conditions: list[Condition]):
        self.tables: list[str] = tables
        self.column: list[str] = column
        self.conditions: list[Condition] = conditions


class DataDeletion:
    """
    Data Deletion Query Representation.
    """

    def __init__(self, table: str, conditions: list[Condition]):
        self.table: str = table
        self.conditions: list[Condition] = conditions


class DataWrite(Generic[T]):
    """
    Data Write Query Representation.

    Example of use: DataWrite[str](...)
    """

    def __init__(self, tables: list[str], column: list[str], conditions: list[Condition], new_value: list[T] | None):
        self.tables: list[str] = tables
        self.column: list[str] = column
        self.conditions: list[Condition] = conditions
        self.new_value: list[T] | None = new_value


class Storage:
    """
    Storage Manager for DBMS.
    """

    def __init__(self):
        self.storage_path = './storage'

    def write_block(self, data_write: DataWrite):
        pass

    def read_block(self, data_retrieval: DataRetrieval):
        pass

    @staticmethod
    def __convert_to_binary(data: int | float | str) -> bytes:
        """
        Convert data types (int, float, str) to binary representation.
        """
        if isinstance(data, int):
            # Integer
            byte_length = (data.bit_length() + 7) // 8 or 1
            return data.to_bytes(byte_length, byteorder='big', signed=True)

        elif isinstance(data, float):
            # Float
            return struct.pack('>d', data)

        elif isinstance(data, str):
            # String
            return data.encode('utf-8')

        else:
            raise ValueError(f"Unsupported data type: {type(data).__name__}")

    @staticmethod
    def __convert_from_binary(binary_data: bytes, data_type: int | float | str) -> int | float | str:
        """
        Converts binary data to original data type (int, float, str).
        """
        if data_type == int:
            # Convert binary to integer
            return int.from_bytes(binary_data, byteorder='big', signed=True)

        elif data_type == float:
            # Convert binary to float
            return struct.unpack('>d', binary_data)[0]

        elif data_type == str:
            # Convert binary to string
            return binary_data.decode('utf-8')

        else:
            raise ValueError(f"Unsupported data type: {data_type.__name__}")
