from typing import Generic, TypeVar, Callable, Any
import struct
import os

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

    :var table: table name
    :var column: column names
    :var query_type: query type, enum 'insert' or 'update'
    :var new_value_func: function to generate new value for Update query
    :var conditions: condition for Update query
    """

    def __init__(self, table: str,
                 column: list[str],
                 query_type: str,
                 new_value_func: Callable[[..., Any], T] = None,
                 new_value: list[T] = None,
                 conditions: list[Condition] | None = None):
        self.table: str = table                                                 # table name
        self.column: list[str] = column                                         # column names
        self.query_type: str = query_type                                       # query type, enum 'insert' or 'update'
        self.new_value_func: Callable[[..., Any], T] | None = new_value_func    # function to generate new value for Update query
        self.new_value: list[T] | None = new_value                              # tuples of value for insert query
        self.conditions: list[Condition] | None = conditions                    # condition for Update query

        # check type enum
        if self.query_type not in ['insert', 'update']:
            raise ValueError(f"Unsupported query type: {self.query_type}")


class Storage:
    """
    Storage Manager for DBMS.
    """

    def __init__(self):
        self.storage_path = './storage'

    def write_block(self, data_write: DataWrite) -> int:
        """
        Write data to storage
        """
        if data_write.query_type != 'insert':
            raise ValueError(f"UPDATE BELUM DIBUAT")

        # file path
        table_file = os.path.join(self.storage_path, f"{data_write.table}.bin")

        # Check file exists, if not, create empty file
        if not os.path.exists(table_file):
            with open(table_file, 'wb') as file:
                pass  # create empty file

        # Open the file to add data
        with open(table_file, 'ab') as file:
            total_bytes_written = 0

            # Convert each new value to binary and write to file
            for value in data_write.new_value:
                binary_data = self.__convert_to_binary(value)
                file.write(binary_data)
                total_bytes_written += len(binary_data)

        return total_bytes_written

    def read_block(self, data_retrieval: DataRetrieval):
        pass

    @staticmethod
    def __convert_to_binary(data: int | float | str) -> bytes:
        """
        Converts data types (int, float, str) to binary representation.
        """
        if isinstance(data, int):
            if data.bit_length() > 32:
                raise ValueError("Integer value is too large to be stored in 32 bits.")

            # Convert integer to binary (4 bytes for 32bit integer)
            # using big endian byte order
            return data.to_bytes(4, byteorder='big', signed=True)

        elif isinstance(data, float):
            # Convert float to binary (double precision, 8 bytes)
            return struct.pack('>d', data)

        elif isinstance(data, str):
            # Convert string to binary (UTF-8 encoded)
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


if __name__ == '__main__':
    # Create a sample DataWrite object for 'insert' query
    data_write = DataWrite(
        table="employees",
        column=["id", "name", "salary"],
        query_type="insert",
        new_value=[1, "John Doe", 50000]
    )

    # Create the storage manager
    storage_manager = Storage()

    # Write the data to the storage file
    bytes_written = storage_manager.write_block(data_write)

    print(f"Total bytes written: {bytes_written}")
