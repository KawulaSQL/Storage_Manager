from lib.Attribute import Attribute
import re

class Condition:
    """
    A class to represent a condition in a query.

    :param column1: The name of the first column.
    :param operator: The comparison operator.
    :param column2_or_value: The second column name or a value to compare against.
    """

    def __init__(self, operand1: str, operator: str, operand2: str):
        """
        Initialize a Condition instance.


        :param operand1: The name of the first column or value.
        :param operator: The comparison operator.
        :param operand2: The second column name or a value to compare against.
        """
        self.operand1: str | int | float = self.classify_operand(operand1)
        self.operator: str = operator  # enum of <, >, =, <=, >=, !=
        self.operand2: str | int | float = self.classify_operand(operand2)

        if operator not in ["<", ">", "=", "<=", ">=", "!="]:
            raise ValueError("Invalid operator")

    def __str__(self):
        operand2 = (
            f'"{self.operand2}"'
            if isinstance(self.operand2, str)
            else self.operand2
        )
        return f"{self.operand1} {self.operator} {operand2}"

    def evaluate(self, value1, value2):
        """
        Evaluate the condition based on the operator.
        """
        # Determine the second value to compare
        # value2 = self.column2_or_value if value2 is None else value2

        if self.operator == "<":
            return value1 < value2
        elif self.operator == ">":
            return value1 > value2
        elif self.operator == "=":
            return value1 == value2
        elif self.operator == "<=":
            return value1 <= value2
        elif self.operator == ">=":
            return value1 >= value2
        elif self.operator == "!=":
            return value1 != value2
        else:
            raise ValueError("Invalid operator")
    
    def classify_operand(self, operand):
        if operand.replace('.', '', 1).isdigit():
            if '.' in operand:
                return {
                    "value" : float(operand), 
                    "isAttribute": False,
                    "type": "float"
                    }
            else:
                return {
                    "value" : int(operand), 
                    "isAttribute": False,
                    "type": "int"
                    }
            
        if operand.startswith("'") and operand.endswith("'"):
            return {
                "value": operand.strip("'"),
                "isAttribute": False,
                "type": "varchar"
                }
        
        return {
            "value": operand,
            "isAttribute": True
                }
