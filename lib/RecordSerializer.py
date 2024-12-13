from typing import List, Tuple, Any, ByteString


class DtypeEncoder:
    """
    A utility class to encode and decode primitive data types into and from bytes.
    """

    def __init__(self) -> None:
        """
        Initialize the encoder.
        """
        pass

    def encodeInt(self, num: int, bytes: int, signed: bool) -> bytes:
        """
        Encode an integer into a byte array.

        :param num: The integer to encode.
        :param bytes: The number of bytes to use.
        :param signed: Whether the integer is signed.
        :return: The encoded integer as bytes.
        """
        return num.to_bytes(bytes, byteorder='little', signed=signed)

    def decodeInt(self, byte_data: ByteString, offset: int, bytes: int, signed: bool) -> Tuple[int, int]:
        """
        Decode an integer from a byte array.

        :param byte_data: The byte array containing the data.
        :param offset: The offset to start reading from.
        :param bytes: The number of bytes to read.
        :param signed: Whether the integer is signed.
        :return: A tuple containing the decoded integer and the new offset.
        """
        value = int.from_bytes(byte_data[offset:offset + bytes], byteorder='little', signed=signed)
        return value, offset + bytes

    def encodeFloat(self, num: float) -> bytes:
        """
        Encode a float into a byte array using its IEEE 754 representation.

        :param num: The float to encode.
        :return: The encoded float as bytes.
        """
        int_rep = self.__float_to_int(num)
        return self.encodeInt(int_rep, 4, signed=True)

    def decodeFloat(self, byte_data: ByteString, offset: int) -> Tuple[float, int]:
        """
        Decode a float (4 bytes) from a byte array.

        :param byte_data: The byte array containing the data.
        :param offset: The offset to start reading from.
        :return: A tuple containing the decoded float and the new offset.
        """
        int_rep, new_offset = self.decodeInt(byte_data, offset, 4, signed=True)
        return self.__int_to_float(int_rep), new_offset

    def encodeChar(self, char: str, size: int = 1) -> bytes:
        """
        Encode a single character into a byte array, padded to the required size.

        :param char: The character to encode.
        :param size: The size of the output byte array.
        :return: The encoded character as bytes.
        """
        encoded = char.encode('utf-8')
        return encoded.ljust(size, b'\x00')

    def decodeChar(self, byte_data: ByteString, offset: int, size: int) -> Tuple[str, int]:
        """
        Decode a character from a byte array.

        :param byte_data: The byte array containing the data.
        :param offset: The offset to start reading from.
        :param size: The size of the encoded character.
        :return: A tuple containing the decoded character and the new offset.
        """
        value = byte_data[offset:offset + size].decode('utf-8').rstrip('\x00')
        return value, offset + size

    def encodeVarChar(self, char: str, max_size: int) -> bytes:
        """
        Encode a variable-length string with its length (2 bytes) and the actual string data.

        :param char: The string to encode.
        :param max_size: The maximum allowed size of the string.
        :return: The encoded string as bytes.
        :raises ValueError: If the string length exceeds max_size.
        """
        if (char[0] == '\'' and char[-1] == '\'') :
            char = char[1:-1]
        encoded = char.encode('utf-8')
        length = len(encoded)
        if length > max_size:
            raise ValueError(f"String length {length} exceeds maximum allowed size {max_size}.")
        length_bytes = self.encodeInt(length, 2, signed=False)
        return length_bytes + encoded

    def decodeVarChar(self, byte_data: ByteString, offset: int) -> Tuple[str, int]:
        """
        Decode a variable-length string from a byte array.

        :param byte_data: The byte array containing the data.
        :param offset: The offset to start reading from.
        :return: A tuple containing the decoded string and the new offset.
        """
        length, new_offset = self.decodeInt(byte_data, offset, 2, signed=False)
        value = byte_data[new_offset:new_offset + length].decode('utf-8')
        value = f"'{value}'"
        return value, new_offset + length

    def __float_to_int(self, num: float) -> int:
        """
        Convert a float to its integer representation (IEEE 754 format).

        :param num: The float to convert.
        :return: The integer representation of the float.
        """
        import struct
        return struct.unpack('<I', struct.pack('<f', num))[0]

    def __int_to_float(self, int_rep: int) -> float:
        """
        Convert an integer (IEEE 754 format) to a float.

        :param int_rep: The integer representation of the float.
        :return: The converted float.
        """
        import struct
        return struct.unpack('<f', struct.pack('<I', int_rep))[0]


class RecordSerializer:
    """
    A class to serialize and deserialize records based on a schema.
    """

    def __init__(self, schema: List[Tuple[str, str, int]]) -> None:
        """
        Initialize the serializer with a schema.

        :param schema: The schema for the records, as a list of tuples (name, type, size).
        """
        self.schema = schema
        self.encoder = DtypeEncoder()

    def serialize(self, record: Tuple[Any, ...]) -> bytearray:
        """
        Serialize a record into a binary format.

        :param record: The record to serialize, as a tuple of values.
        :return: The serialized record as a bytearray.
        :raises ValueError: If the record contains invalid data for the schema.
        """
        record_bytes = bytearray()
        for value, (name, dtype, size) in zip(record, self.schema):
            if dtype == 'int':
                value = int(value)
                record_bytes.extend(self.encoder.encodeInt(value, size, True))
            elif dtype == 'float':
                value = float(value)
                record_bytes.extend(self.encoder.encodeFloat(value))
            elif dtype == 'char':
                assert len(value) == 1
                record_bytes.extend(self.encoder.encodeChar(value, size))
            elif dtype == 'varchar':
                assert len(value) <= size
                record_bytes.extend(self.encoder.encodeVarChar(value, size))
            else:
                raise ValueError(f"Unsupported data type: {dtype}")
        return bytearray([ord("R"), ord("C")]) + record_bytes + bytearray([0xCC])

    def deserialize(self, record_bytes: ByteString) -> Tuple[Any, ...]:
        """
        Deserialize a binary format into a record.

        :param record_bytes: The binary data to deserialize.
        :return: The deserialized record as a tuple of values.
        :raises ValueError: If the binary format is invalid or does not match the schema.
        """
        if record_bytes[0:2] != b"RC":
            raise ValueError("Invalid Record Header")
        if record_bytes[-1] != 0xCC:
            raise ValueError("Invalid Sentinel")

        record = []
        offset = 2

        for name, dtype, size in self.schema:
            if dtype == 'int':
                value, offset = self.encoder.decodeInt(record_bytes, offset, size, signed=True)
            elif dtype == 'float':
                value, offset = self.encoder.decodeFloat(record_bytes, offset)
            elif dtype == 'char':
                value, offset = self.encoder.decodeChar(record_bytes, offset, size)
            elif dtype == 'varchar':
                value, offset = self.encoder.decodeVarChar(record_bytes, offset)
            else:
                raise ValueError(f"Unsupported data type: {dtype}")

            record.append(value)

        return tuple(record)
