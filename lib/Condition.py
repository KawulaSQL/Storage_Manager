class Condition:
    """
    A class to represent a condition in a query.

    :param column1: The name of the first column.
    :param operator: The comparison operator.
    :param column2_or_value: The second column name or a value to compare against.
    """

    def __init__(self, column1: str, operator: str, column2_or_value):
        """
        Initialize a Condition instance.


        :param column1: The name of the first column.
        :param operator: The comparison operator.
        :param column2_or_value: The second column name or a value to compare against.
        """
        self.column1: str = column1
        self.operator: str = operator  # enum of <, >, =, <=, >=, !=
        self.column2_or_value = column2_or_value

        if operator not in ["<", ">", "=", "<=", ">=", "!="]:
            raise ValueError("Invalid operator")

    def __str__(self):
        column2_or_value = (
            f'"{self.column2_or_value}"'
            if isinstance(self.column2_or_value, str)
            else self.column2_or_value
        )
        return f"{self.column1} {self.operator} {column2_or_value}"

    def evaluate(self, value1, value2=None):
        """
        Evaluate the condition based on the operator.
        """
        # Determine the second value to compare
        value2 = self.column2_or_value if value2 is None else value2

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
