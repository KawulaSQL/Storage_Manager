class Condition:
    """
    A class to represent a condition in a query.
    """

    def __init__(self, column1: str, operator: str, column2: str):
        self.column1: str = column1
        self.operator: str = operator   # enum of <, >, =, <=, >=, !=
        self.column2: str = column2

        if operator not in ['<', '>', '=', '<=', '>=', '!=']:
            raise ValueError("Invalid operator")

    def __str__(self):
        return f"{self.column1} {self.operator} {self.column2}"
