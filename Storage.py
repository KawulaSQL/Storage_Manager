from typing import Generic, TypeVar

# Type for Generic class
T = TypeVar('T')


class Condition:
    """
    Query Condition
    """
    def __init__(self, column, operation, operand):
        self.column: str = column
        self.operation: str = operation  # enum {'=', '<>', '>', '>=', '<', '<='}
        self.operand: int | str = operand


class DataRetrieval:
    """
    Data Retrieval Query Representation
    """
    def __init__(self, tables: list[str], column: list[str], conditions: list[Condition]):
        self.tables: list[str] = tables
        self.column: list[str] = column
        self.conditions: list[Condition] = conditions


class DataWrite(Generic[T]):
    """
    Data Write Query Representation
    """
    def __init__(self, tables: list[str], column: list[str], conditions: list[Condition], new_value: list[T] | None):
        self.tables: list[str] = tables
        self.column: list[str] = column
        self.conditions: list[Condition] = conditions
        self.new_value: list[T] | None = new_value


class DataDeletion:
    """
    Data Deletion Query Representation
    """
    def __init__(self, table: str, conditions: list[Condition]):
        self.table: str = table
        self.conditions: list[Condition] = conditions


class Storage:
    """
    Storage Manager for DBMS
    """

    def __init__(self):
        self.storage_path = './storage'
