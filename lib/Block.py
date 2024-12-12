from typing import ByteString

# Constants for block sizes
BLOCK_SIZE = 4096  # 4KB
DATA_SIZE = BLOCK_SIZE - 12  # Data size excluding the header (12 bytes)


class Block:
    """
    Represents a single block/page in the storage system.

    A block contains a header with metadata and a data region for storing serialized records.
    The block size is fixed at 4KB.
    """

    def __init__(self) -> None:
        """
        Initialize a new block with default values.
        """
        self.header = {
            "page_id": 0,
            "record_count": 0,
            "free_space_offset": 0
        }
        self.data: bytearray = bytearray(DATA_SIZE)
        self.cursor: int = 0
        self.reset_header()

    def reset_header(self) -> None:
        """
        Reset the header and clear the data region.
        """
        self.header["free_space_offset"] = 0
        self.header["record_count"] = 0
        self.data = bytearray(DATA_SIZE)

    def add_record(self, record_bytes: ByteString) -> None:
        """
        Add a serialized record to the block.

        :param record_bytes: The serialized record as bytes.
        :raises ValueError: If there is not enough free space in the block.
        """
        record_size = len(record_bytes)
        if self.header["free_space_offset"] + record_size > DATA_SIZE:
            raise ValueError("Page is full, cannot add record.")
        
        start = self.header["free_space_offset"]
        self.data[start:start + record_size] = record_bytes
        self.header["free_space_offset"] += record_size
        self.header["record_count"] += 1

    def to_bytes(self) -> bytearray:
        """
        Serialize the block (header + data) into bytes.

        :return: The serialized block as a bytearray.
        """
        header_bytes = bytearray()
        header_bytes.extend(self.header["page_id"].to_bytes(4, 'little'))
        header_bytes.extend(self.header["record_count"].to_bytes(4, 'little'))
        header_bytes.extend(self.header["free_space_offset"].to_bytes(4, 'little'))
        return header_bytes + self.data

    def from_bytes(self, data: ByteString) -> None:
        """
        Deserialize bytes back into block data and header.

        :param data: The serialized block data as bytes.
        """
        self.header["page_id"] = int.from_bytes(data[:4], 'little')
        self.header["record_count"] = int.from_bytes(data[4:8], 'little')
        self.header["free_space_offset"] = int.from_bytes(data[8:12], 'little')
        self.data[:] = data[12:]

    def capacity(self) -> int:
        """
        Calculate the remaining free space in the block.

        :return: The number of free bytes available in the block.
        """
        return DATA_SIZE - self.header["free_space_offset"]

    @staticmethod
    def read_block(file_path: str, block_num: int) -> "Block":
        """
        Read a block from a file.

        :param file_path: The path to the binary file.
        :param block_num: The block number (zero-indexed) to read.
        :return: The deserialized Block object.
        """
        with open(file_path, "rb") as fd:
            fd.seek(block_num * BLOCK_SIZE)
            block = Block()
            block.from_bytes(fd.read(BLOCK_SIZE))
            return block

    def write_block(self, file_path: str, block_num: int) -> None:
        """
        Write the block to a file.

        :param file_path: The path to the binary file.
        :param block_num: The block number (zero-indexed) to write to.
        """
        with open(file_path, "r+b") as fd:
            fd.seek(block_num * BLOCK_SIZE)
            fd.write(self.to_bytes())

    def init_cursor(self) -> None:
        """
        Initialize the cursor for sequential reading of the block's data.
        """
        self.cursor = 0

    def read(self, num_bytes: int) -> bytearray:
        """
        Read a specified number of bytes from the block's data, starting from the cursor.

        :param num_bytes: The number of bytes to read.
        :return: The read bytes as a bytearray.
        """
        old_cursor = self.cursor
        self.cursor += num_bytes
        return self.data[old_cursor:old_cursor + num_bytes]
