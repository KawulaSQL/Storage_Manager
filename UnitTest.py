import unittest
import os
import shutil
import json
import uuid
import sys
from lib.Schema import Schema
from lib.Attribute import Attribute
from lib.Condition import Condition
from StorageManager import StorageManager


class TestStorageManager(unittest.TestCase):
    TEST_BASE_PATH = "test_storage"

    @classmethod
    def setUpClass(cls):
        """
        Set up the test environment. Create a base directory for test files.
        """
        if not os.path.exists(cls.TEST_BASE_PATH):
            os.makedirs(cls.TEST_BASE_PATH)

    @classmethod
    def tearDownClass(cls):
        """
        Tear down the test environment. Remove the test base directory.
        """
        if os.path.exists(cls.TEST_BASE_PATH):
            shutil.rmtree(cls.TEST_BASE_PATH)

    def setUp(self):
        """
        Set up a clean StorageManager instance before each test.
        Remove the information_schema_table.bin file if it exists and reinitialize StorageManager.
        """
        information_schema_file = os.path.join(
            self.TEST_BASE_PATH, "information_schema_table.bin"
        )
        if os.path.exists(information_schema_file):
            os.remove(information_schema_file)

        # Reinitialize the StorageManager after cleanup
        self.storage_manager = StorageManager(self.TEST_BASE_PATH)

    def generate_unique_table_name(self, base_name="test_table"):
        """
        Generate a unique table name for each test case.
        """
        return f"{base_name}_{uuid.uuid4().hex[:8]}"

    def test_initialize_information_schema(self):
        """
        Test if the information_schema table is properly initialized
        by checking the existence of its binary file.
        """
        expected_file_path = os.path.join(
            self.TEST_BASE_PATH, "information_schema_table.bin"
        )
        self.assertTrue(
            os.path.exists(expected_file_path),
            "information_schema table binary file does not exist in the storage directory.",
        )

    def test_create_table(self):
        """
        Test creating a new table and verifying its existence.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int"), Attribute("name", "varchar", 50), Attribute("score", "float"), Attribute("grade", "char")])
        self.storage_manager.create_table(table_name, schema)

        # check if the table is created
        tables = self.storage_manager.list_tables()
        self.assertIn("'" + table_name + "'", tables)

        # check if the file is created in the storage
        table_file = os.path.join(self.TEST_BASE_PATH, f"{table_name}_table.bin")
        self.assertTrue(
            os.path.exists(table_file), f"Table file {table_file} should exist."
        )

        # verify the table scheme
        table_schema = self.storage_manager.get_table_schema(table_name)
        self.assertEqual(table_schema, schema)

    def test_insert_and_retrieve_data(self):
        """
        Test inserting data into a table and retrieving it.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int", 4), Attribute("name", "varchar", 50)])
        self.storage_manager.create_table(table_name, schema)

        data = [(1, "Alice"), (2, "Bob")]
        self.storage_manager.insert_into_table(table_name, data)

        retrieved_data = self.storage_manager.get_table_data(table_name)
        modified_data = list(map(lambda x : (x[0], "'" + x[1] + "'"), data))
        self.assertEqual(modified_data, retrieved_data)

    def test_get_table_schema(self):
        """
        Test retrieving the schema of a table.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int", 4), Attribute("name", "varchar", 50)])
        self.storage_manager.create_table(table_name, schema)

        retrieved_schema = self.storage_manager.get_table_schema(table_name)
        self.assertEqual(retrieved_schema, schema)

    def test_table_already_exists(self):
        """
        Test trying to create a table that already exists.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int", 4), Attribute("name", "varchar", 50)])
        self.storage_manager.create_table(table_name, schema)

        with self.assertRaises(ValueError) as context:
            self.storage_manager.create_table(table_name, schema)

        self.assertEqual(
            str(context.exception),
            f"Table {table_name} already exists.",
            "The exception message for a duplicate table creation is incorrect.",
        )

    def test_get_table_not_found(self):
        """
        Test accessing a table that does not exist.
        """
        with self.assertRaises(ValueError) as context:
            self.storage_manager.get_table_data("non_existent_table")
        self.assertEqual(str(context.exception), "Table non_existent_table not found.")

    def test_get_stats(self):
        """
        Test retrieving statistics for a table.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int", 4), Attribute("name", "varchar", 50)])
        self.storage_manager.create_table(table_name, schema)

        data = [(1, "Alice"), (2, "Bob"), (3, "Alice")]
        self.storage_manager.insert_into_table(table_name, data)

        stats = self.storage_manager.get_stats()

        self.assertIn(table_name, stats)
        self.assertEqual(stats[table_name]["n_r"], 3)  # Number of records
        self.assertGreater(stats[table_name]["b_r"], 0)  # Blocks should be > 0
        self.assertGreater(stats[table_name]["l_r"], 0)  # Tuple size should be > 0

        self.assertIsInstance(stats[table_name]["v_a_r"], dict)
        for column, unique_count in stats[table_name]["v_a_r"].items():
            self.assertGreater(
                unique_count,
                0,
                f"Column '{column}' should have more than 0 unique values.",
            )

    def test_delete_table(self):
        """
        Test if Table can be deleted.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int", 4), Attribute("name", "varchar", 50)])
        self.storage_manager.create_table(table_name, schema)

        # Verify the table exists
        tables = self.storage_manager.list_tables()
        self.assertIn(
            "'" + table_name + "'", tables, f"Table {table_name} should exist before deletion."
        )

        self.storage_manager.delete_table(table_name)

        # check file system
        table_file = os.path.join(self.TEST_BASE_PATH, f"{table_name}_table.bin")
        self.assertFalse(
            os.path.exists(table_file),
            f"Table file {table_file} should have been deleted.",
        )

        # check instance
        tables = self.storage_manager.list_tables()
        self.assertNotIn(
            "'" + table_name + "'", tables, f"Table {table_name} should have been deleted."
        )

        # Verify the table data is no longer accessible
        with self.assertRaises(ValueError):
            self.storage_manager.get_table_data(table_name)

    def test_delete_records(self):
        """
        Test if table can delete records using conditions.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int", 4), Attribute("name", "varchar", 50), Attribute("age", "int", 4)])
        self.storage_manager.create_table(table_name, schema)

        # Verify the table exists
        tables = self.storage_manager.list_tables()
        self.assertIn(
            "'" + table_name + "'", tables, f"Table {table_name} should exist before deletion."
        )

        # insert records
        self.storage_manager.insert_into_table(table_name, [(1, "Agus", 20), (2, "Bagas", 21), (3, "Ciko", 21), (4, "Dito", 21), (5, "Eko", 19)])

        # check if insert correct
        self.assertEqual(self.storage_manager.get_table_data(table_name), [(1, "'Agus'", 20), (2, "'Bagas'", 21), (3, "'Ciko'", 21), (4, "'Dito'", 21), (5, "'Eko'", 19)])

        # delete records
        self.storage_manager.delete_table_record(table_name, Condition("id", "=", "3"))

        # check records
        self.assertEqual(self.storage_manager.get_table_data(table_name), [(1, "'Agus'", 20), (2, "'Bagas'", 21), (4, "'Dito'", 21), (5, "'Eko'", 19)])

        # delete records
        self.storage_manager.delete_table_record(table_name, Condition("age", ">=", "20"))

        # check records
        self.assertEqual(self.storage_manager.get_table_data(table_name), [(5, "'Eko'", 19)])

    def test_update_records(self):
        """
        Test if table can update records using conditions.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int", 4), Attribute("name", "varchar", 50), Attribute("age", "int", 4)])
        self.storage_manager.create_table(table_name, schema)

        # Verify the table exists
        tables = self.storage_manager.list_tables()
        self.assertIn(
            "'" + table_name + "'", tables, f"Table {table_name} should exist before deletion."
        )

        # insert records
        self.storage_manager.insert_into_table(table_name, [(1, "Agus", 20), (2, "Bagas", 21), (3, "Ciko", 21), (4, "Dito", 21), (5, "Eko", 19)])

        # check if insert correct
        self.assertEqual(self.storage_manager.get_table_data(table_name), [(1, "'Agus'", 20), (2, "'Bagas'", 21), (3, "'Ciko'", 21), (4, "'Dito'", 21), (5, "'Eko'", 19)])

        # update records
        self.storage_manager.update_table(table_name, {"age" : "age ^ (5 - 3) - 100"} , Condition("id", "=", "4"))
        self.assertEqual(self.storage_manager.get_table_data(table_name), [(1, "'Agus'", 20), (2, "'Bagas'", 21), (3, "'Ciko'", 21), (4, "'Dito'", 341), (5, "'Eko'", 19)])

    def test_set_and_get_index(self):
        """
        Test creating a hash index on a table column and retrieving records using the index.
        """
        table_name = self.generate_unique_table_name("test_table")
        schema = Schema([Attribute("id", "int", 4), Attribute("name", "varchar", 50)])
        self.storage_manager.create_table(table_name, schema)
        # Insert data into the table
        data = [(1, "Alice"), (2, "Bob"), (3, "Alice")]
        self.storage_manager.insert_into_table(table_name, data)
        # Create a hash index on the 'name' column
        self.storage_manager.set_index(table_name, "name", "hash")
        # Check if the index file was created
        index_file_path = os.path.join("./storage", f"{table_name}-name-hash.pickle")
        print(index_file_path)
        self.assertTrue(os.path.isfile(index_file_path), "Hash index file was not created.")
        # Retrieve records with 'name' = 'Alice' using the index
        result = self.storage_manager.get_index(table_name, "name", "'Alice'", "varchar")
        # Expected records with 'name' = 'Alice'
        expected = [(1,"'Alice'"), (3, "'Alice'")]
        self.assertEqual(result, expected, "get_index did not return the expected records.")

class ColoredTextTestResult(unittest.TextTestResult):
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

    def addSuccess(self, test):
        super().addSuccess(test)
        print(f" {self.GREEN}SUCCESS: {test}{self.RESET}")
        sys.stdout.flush()

    def addFailure(self, test, err):
        super().addFailure(test, err)
        print(f" {self.RED}FAIL: {test}{self.RESET}")
        sys.stdout.flush()

    def addError(self, test, err):
        super().addError(test, err)
        print(f" {self.RED}ERROR: {test}{self.RESET}")
        sys.stdout.flush()


class ColoredTextTestRunner(unittest.TextTestRunner):
    resultclass = ColoredTextTestResult


if __name__ == "__main__":
    unittest.main(testRunner=ColoredTextTestRunner())
