from typing import Any, Dict, Union
from lib.Expression import ExpressionParser  # Assuming the previous implementation is in this file

class Condition:
    """
    A class to represent a condition in a query with enhanced expression parsing.

    :param operand1: The first operand (column or expression)
    :param operator: The comparison operator
    :param operand2: The second operand (column or expression)
    """

    def __init__(self, operand1: str, operator: str, operand2: str):
        """
        Initialize a Condition instance.

        :param operand1: The first operand (column or expression)
        :param operator: The comparison operator
        :param operand2: The second operand (column or expression)
        """
        self.expression_parser = ExpressionParser()

        self.operand1: str = operand1
        self.operator: str = operator
        self.operand2: str = operand2

        if operator not in ["<", ">", "=", "<=", ">=", "!="]:
            raise ValueError("Invalid operator")

    def __str__(self):
        """
        String representation of the condition.
        """
        operand2 = (
            f'"{self.operand2["value"]}"'
            if isinstance(self.operand2["value"], str)
            else self.operand2["value"]
        )
        return f"{self.operand1['value']} {self.operator} {operand2}"

    def evaluate(self, context: Dict[str, Union[int, float, str]] = None) -> bool:
        """
        Evaluate the condition based on the operator and operands.

        :param context: Dictionary of attribute values for resolution
        :return: Boolean result of the condition
        """
        if context is None:
            context = {}

        try:
            value1 = self.expression_parser.evaluate(self.operand1, context)
            value2 = self.expression_parser.evaluate(self.operand2, context)
        except Exception as e:
            raise ValueError(f"Error evaluating expression: {e}")

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