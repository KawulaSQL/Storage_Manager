from dataclasses import dataclass, field
from typing import Generic, TypeVar, Callable, Any

# Type for Generic class
T = TypeVar('T')


@dataclass
class Rows(Generic[T]):
    """
    Rows of data.

    :var data: list of data
    :var rows_count: number of rows
    """
    data: list[T]
    rows_count: int


@dataclass
class Condition:
    """
    Query Condition.
    """
    column: str
    operation: str  # enum {'=', '<>', '>', '>=', '<', '<='}
    operand: int | str


@dataclass
class DataRetrieval:
    """
    Data Retrieval Query Representation.
    """
    tables: list[str]
    column: list[str]
    conditions: list[Condition] = field(default_factory=list)


@dataclass
class DataDeletion:
    """
    Data Deletion Query Representation.
    """
    table: str
    conditions: list[Condition]


@dataclass
class DataWrite(Generic[T]):
    """
    Data Write Query Representation.

    :var table: table name
    :var column: column names
    :var query_type: query type (insert, update)
    :var new_value: new values (list of T for insert/update, a single T for update-set)
    :var conditions: conditions
    """
    table: str
    column: list[str]
    query_type: str     # enum {'insert', 'update'}
    new_value: list[T] | T = None
    conditions: list[Condition] = None

    # enum check
    def __post_init__(self):
        valid_query_types = {'insert', 'update'}
        if self.query_type not in valid_query_types:
            raise ValueError(f"Invalid query_type: {self.query_type}. Must be one of {valid_query_types}.")