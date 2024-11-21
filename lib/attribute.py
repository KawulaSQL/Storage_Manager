class Attribute:
    """
    Represents a single column in the table schema.
    """

    def __init__(self, name: str, dtype: str, size: int) -> None:
        """
        Initialize an Attribute.

        :param name: The name of the column.
        :param dtype: The data type of the column ('int', 'float', 'char', 'varchar').
        :param size: The size of the column (in bytes).
        """

        if (dtype not in ["int", "float", "char", "varchar"]) :
            raise ValueError("Unsupported Data Type")

        self.name: str = name
        self.dtype: str = dtype
        self.size: int = size

        if (dtype == "int" or dtype == "float") :
            self.size = 4
        elif (dtype == "char") :
            self.size = 1

    def __repr__(self) -> str:
        """
        Return a string representation of the attribute.

        :return: String representation.
        """
        return f"Attribute(name={self.name}, dtype={self.dtype}, size={self.size})"
    
    def __str__(self) -> str:
        """
        Return a user-friendly string representation of the attribute.

        :return: String representation in a readable format.
        """
        return f"Name: {self.name}, Type: {self.dtype}, Size: {self.size} bytes"
