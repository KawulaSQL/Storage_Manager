import struct


class Serializer:
    """
    Serializer class is responsible for serializing and deserializing the data.
    """

    # SEMENTARA -- GANTI AJA KALO PERLU
    @staticmethod
    def serialize(data: object) -> bytes:
        return Serializer.__convert_to_binary(data)

    # SEMENTARA -- GANTI AJA KALO PERLU
    @staticmethod
    def deserialize(binary_data: bytes, data_type: type) -> object:
        return Serializer.__convert_from_binary(binary_data, data_type)

    # SEMENTARA -- GANTI AJA KALO PERLU
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

    # SEMENTARA -- GANTI AJA KALO PERLU
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
