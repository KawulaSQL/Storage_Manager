#!/usr/bin/python3

import re
from StorageManager import StorageManager
from lib.Schema import Schema
from lib.Attribute import Attribute
from lib.Condition import Condition


class TestDriver:
    def __init__(self, base_path: str) -> None:
        self.base_path = base_path
        self.storage_manager = StorageManager(base_path)

    def parse_create_table(self, statement: str):
        schema_match = re.search(r"CREATE TABLE\s+(\w+)\s*\((.*)\)", statement, re.DOTALL | re.IGNORECASE)
        
        if not schema_match:
            print("Error: Invalid CREATE TABLE statement.")
            return

        table_name = schema_match.group(1).strip()
        schema_str = schema_match.group(2).strip()

        attributes = []
        for attribute_str in schema_str.split(','):
            attribute_str = attribute_str.strip()
            match = re.match(r"(\w+)\s+(\w+)(?:\((\d+)\))?", attribute_str)
            if match:
                name = match.group(1)
                dtype = match.group(2).lower()
                size = int(match.group(3)) if match.group(3) else None
                attributes.append(Attribute(name, dtype, size))
            else:
                print(f"Error: Invalid attribute definition '{attribute_str}'")
                return

        try:
            self.storage_manager.create_table(table_name, Schema(attributes))
            print(f"{table_name} successfully created")
        except ValueError as e :
            print(e)

    def parse_select_no_join(self, statement: str) -> None:
        """Parse SELECT * FROM table_name statement and print the result."""
        match = re.search(r"SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?", statement, re.IGNORECASE)
        if not match:
            print("Error: Invalid SELECT statement.")
            return

        column_selected = match.group(1)
        table_name = match.group(2)
        try:
            where_clause = match.group(3)
        except:
            where_clause = None 

        try:
            if where_clause:
                comparison_operators = ['<=', '>=', '!=', '==', '=', '<', '>']
                
                for op in comparison_operators:
                    if op in where_clause:
                        parts = where_clause.split(op)
                        
                        operand1 = parts[0].strip()
                        operand2 = parts[1].strip()

                        condition = Condition(operand1, op, operand2)
                        
                        table_data = self.storage_manager.get_table_data(table_name, condition)
                        break
            else: 
                table_data = self.storage_manager.get_table_data(table_name)

            if (len(table_data) == 0) :
                print("No Record found")
                return

            schema = self.storage_manager.get_table_schema(table_name)
            all_columns = [attr[0] for attr in schema.get_metadata()]
            
            if column_selected.strip() == "*":
                column_names = all_columns
            else:
                column_names = [col.strip() for col in column_selected.split(",")]
                if not set(column_names).issubset(all_columns):
                    print(f"Error: Some specified columns do not exist in table '{table_name}'.")
                    return

            column_widths = [len(name) for name in column_names]
            for row in table_data:
                filtered_row = [row[all_columns.index(col)] for col in column_names]
                column_widths = [max(width, len(str(value))) for width, value in zip(column_widths, filtered_row)]

            row_format = " | ".join(f"{{:<{width}}}" for width in column_widths)
            separator = "-+-".join("-" * width for width in column_widths)

            print(row_format.format(*column_names))
            print(separator)
            for row in table_data:
                filtered_row = [row[all_columns.index(col)] for col in column_names]
                print(row_format.format(*filtered_row))
            
            # print(self.storage_manager.get_stats()) # testing purposes
        except ValueError as e:
            print(e)

    def parse_select(self, statement: str) -> None:
        """
        Parse SELECT statement supporting:
        1. Single table SELECT 
        2. Multiple JOIN ON syntax
        3. Optional WHERE condition as global condition
        Format: 
        SELECT columns FROM table1 [JOIN table2 ON table1.attr = table2.attr]+ [WHERE condition]
        """
        select_match = re.match(
            r"SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+))*(?:\s+WHERE\s+(.*))?", 
            statement, 
            re.IGNORECASE
        )

        if ("join" not in statement.lower()) :
            self.parse_select_no_join(statement)
            return
        
        if not select_match:
            print("Error: Invalid SELECT statement.")
            return

        column_selected = select_match.group(1)
        tables = [select_match.group(2)]
        join_attributes = []
        where_clause = select_match.group(6)

        join_pattern = re.compile(r"JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)")
        joins = join_pattern.findall(statement)
        
        for join in joins:
            tables.append(join[0])
            join_attributes.append((join[1], join[2]))

        try:
            table_conditions = [None] * len(tables)
            global_condition = None

            if where_clause:
                comparison_operators = ['<=', '>=', '!=', '==', '=', '<', '>']
                
                for op in comparison_operators:
                    if op in where_clause:
                        parts = where_clause.split(op)
                        operand1 = parts[0].strip()
                        operand2 = parts[1].strip()
                        global_condition = Condition(operand1, op, operand2)
                        break

            if len(tables) == 1:
                table_data = self.storage_manager.get_table_data(tables[0], global_condition)
                
            else:
                table_data = self.storage_manager.get_joined_table(
                    table_names=tables, 
                    join_attributes=join_attributes, 
                    table_conditions=table_conditions, 
                    global_condition=global_condition
                )

            if len(table_data) == 0:
                print("No Record found")
                return

            all_columns = []
            for table in tables:
                schema = self.storage_manager.get_table_schema(table)
                table_columns = [f"{table}.{attr[0]}" for attr in schema.get_metadata()]
                all_columns.extend(table_columns)
            
            if column_selected.strip() == "*":
                column_names = all_columns
            else:
                column_names = [col.strip() for col in column_selected.split(",")]
                if not set(column_names).issubset(all_columns):
                    print(f"Error: Some specified columns do not exist in tables {tables}.")
                    return

            column_widths = [len(name) for name in column_names]
            for row in table_data:
                filtered_row = [row[all_columns.index(col)] for col in column_names]
                column_widths = [max(width, len(str(value))) for width, value in zip(column_widths, filtered_row)]

            row_format = " | ".join(f"{{:<{width}}}" for width in column_widths)
            separator = "-+-".join("-" * width for width in column_widths)

            print(row_format.format(*column_names))
            print(separator)
            for row in table_data:
                filtered_row = [row[all_columns.index(col)] for col in column_names]
                print(row_format.format(*filtered_row))
            
        except ValueError as e:
            print(e)

    def parse_insert(self, statement: str) -> None:
        """Parse INSERT INTO table_name VALUES (...) statement and insert data."""
        table_name_match = re.search(r"INSERT INTO (\w+) VALUES \((.*?)\)", statement, re.DOTALL | re.IGNORECASE)
        if not table_name_match:
            print("Error: Invalid INSERT statement.")
            return

        table_name = table_name_match.group(1)
        values_str = table_name_match.group(2)
        
        values_list = []
        for row_str in values_str.split('),'):
            row_values = row_str.strip().strip('()').split(',')
            values_list.append([v.strip() for v in row_values])

        self.storage_manager.insert_into_table(table_name, values_list)
        print(f"Data inserted into '{table_name}' successfully.")

    def parse_schema(self, statement: str) -> None:
        """Parse SCHEMA table_name statement and display the schema."""
        table_name_match = re.search(r"SCHEMA (\w+)", statement, re.IGNORECASE)
        if not table_name_match:
            print("Error: Invalid SCHEMA statement.")
            return

        table_name = table_name_match.group(1)
        try:
            schema = self.storage_manager.get_table_schema(table_name)
            metadata = schema.get_metadata()

            if not metadata:
                print(f"No schema found for table '{table_name}'.")
                return

            headers = ["Name", "Type", "Size"]
            column_widths = [len(header) for header in headers]

            for attr in metadata:
                column_widths = [max(width, len(str(value))) for width, value in zip(column_widths, attr)]

            row_format = " | ".join(f"{{:<{width}}}" for width in column_widths)
            separator = "-+-".join("-" * width for width in column_widths)

            print(row_format.format(*headers))
            print(separator)
            for attr in metadata:
                print(row_format.format(*attr))

        except ValueError as e:
            print(e)

    def parse_update(self, statement: str) -> None:
        """Parse UPDATE table_name SET column1=value1, column2=value2 WHERE condition statement."""
        update_match = re.search(r"UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$", statement, re.IGNORECASE)

        table_name = update_match.group(1)
        set_clause = update_match.group(2)
        where_clause = update_match.group(3)

        update_values = {}
        for assignment in set_clause.split(','):
            assignment = assignment.strip()
            col_match = re.match(r"(\w+)\s*=\s*(.+)", assignment)
            if not col_match:
                print(f"Error: Invalid assignment '{assignment}'")
                return
            
            column = col_match.group(1)
            value = col_match.group(2).strip()
            update_values[column] = value

        try:
            if where_clause:
                comparison_operators = ['<=', '>=', '!=', '==', '=', '<', '>']
                
                for op in comparison_operators:
                    if op in where_clause:
                        parts = where_clause.split(op)
                        
                        operand1 = parts[0].strip()
                        operand2 = parts[1].strip()

                        condition = Condition(operand1, op, operand2)
                        
                        rows_affected = self.storage_manager.update_table(table_name, update_values, condition)
                        break
            else: 
                rows_affected = self.storage_manager.update_table(table_name, update_values)
            print(f"{rows_affected} row(s) updated in '{table_name}'.")
        except ValueError as e :
            print(e)


    def _parse_value(self, value: str):
        """Helper method to parse and convert values."""
        if (value.startswith("'") and value.endswith("'")) or \
        (value.startswith('"') and value.endswith('"')):
            return value.strip("'\"")
        
        if value.isdigit():
            return int(value)
        
        try:
            return float(value)
        except ValueError:
            return value
        
    def parse_delete(self, statement: str) -> None:
        """Parse DELETE FROM table_name WHERE condition statement."""
        delete_match = re.search(r"DELETE FROM\s+(\w+)\s+WHERE\s+(.+)", statement, re.IGNORECASE)
        
        if not delete_match:
            print("Error: Invalid DELETE statement.")
            return

        table_name = delete_match.group(1)
        try:
            where_clause = delete_match.group(2)
        except:
            where_clause = None 

        try:
            if where_clause:
                comparison_operators = ['<=', '>=', '!=', '==', '=', '<', '>']
                
                for op in comparison_operators:
                    if op in where_clause:
                        parts = where_clause.split(op)
                        
                        operand1 = parts[0].strip()
                        operand2 = parts[1].strip()

                        condition = Condition(operand1, op, operand2)
                        
                        rows_affected = self.storage_manager.delete_table_record(table_name, condition)
                        break
            else: 
                rows_affected = self.storage_manager.delete_table_record(table_name)
            print(f"{rows_affected} row(s) deleted from '{table_name}'.")
        except ValueError as e:
            print(e)

    def run(self) -> None:
        """Run the CLI driver."""
        while True:
            statement = input("KWL> ").strip()

            if statement.lower() == "exit":
                print("Exiting KWL driver...")
                break

            elif statement.lower().startswith("create table"):
                self.parse_create_table(statement)

            elif statement.lower().startswith("select"):
                self.parse_select(statement)

            elif statement.lower().startswith("insert into"):
                self.parse_insert(statement)

            elif statement.lower().startswith("schema"):
                self.parse_schema(statement)

            elif statement.lower().startswith("update"):
                self.parse_update(statement)

            elif statement.lower().startswith("delete from"):
                self.parse_delete(statement)

            else:
                print("Error: Unsupported KWL statement.")


if __name__ == "__main__":
    base_path = "storage"
    KWL_driver = TestDriver(base_path)
    KWL_driver.run()