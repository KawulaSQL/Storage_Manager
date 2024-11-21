from typing import List

from .attribute import Attribute

class Schema:
    """
    Represents the schema of a table as a collection of attributes.
    """

    def __init__(self, attributes: List[Attribute]) -> None:
        """
        Initialize a Schema.

        :param attributes: List of Attribute objects.
        """
        self.attributes: List[Attribute] = attributes

    def add_attribute(self, name: str, dtype: str, size: int) -> None:
        """
        Add a new attribute to the schema.

        :param name: Name of the attribute.
        :param dtype: Data type of the attribute ('int', 'float', 'char', 'varchar').
        :param size: Size of the attribute (in bytes).
        """
        self.attributes.append(Attribute(name, dtype, size))

    def get_metadata(self) -> List[tuple]:
        """
        Get the schema metadata as a list of tuples.

        :return: List of tuples (name, dtype, size) for each attribute.
        """
        return [(attr.name, attr.dtype, attr.size) for attr in self.attributes]

    def serialize(self) -> bytearray:
        """
        Serialize the schema into a byte array for storage.

        :return: Serialized schema as a bytearray.
        """
        schema_bytes = bytearray()
        for attr in self.attributes:
            schema_bytes.extend(len(attr.name).to_bytes(2, byteorder='little'))
            schema_bytes.extend(attr.name.encode('utf-8'))
            schema_bytes.extend(len(attr.dtype).to_bytes(2, byteorder='little'))
            schema_bytes.extend(attr.dtype.encode('utf-8'))
            schema_bytes.extend(attr.size.to_bytes(2, byteorder='little'))
        return schema_bytes

    @staticmethod
    def deserialize(data: bytearray) -> "Schema":
        """
        Deserialize schema metadata from a byte array.

        :param data: Serialized schema data.
        :return: Schema object.
        """
        attributes = []
        offset = 0
        while offset < len(data):
            name_len = int.from_bytes(data[offset:offset + 2], byteorder='little')
            offset += 2
            name = data[offset:offset + name_len].decode('utf-8')
            offset += name_len

            dtype_len = int.from_bytes(data[offset:offset + 2], byteorder='little')
            offset += 2
            dtype = data[offset:offset + dtype_len].decode('utf-8')
            offset += dtype_len

            size = int.from_bytes(data[offset:offset + 2], byteorder='little')
            offset += 2

            attributes.append(Attribute(name, dtype, size))

        return Schema(attributes)
    
    def __repr__(self) -> str:
        """
        Return a string representation of the schema.

        :return: String representation of the schema.
        """
        return f"Schema(attributes={repr(self.attributes)})"

    def __str__(self) -> str:
        """
        Return a user-friendly string representation of the schema.

        :return: String representation listing each attribute.
        """
        return "\n".join(str(attr) for attr in self.attributes)
