import re
from typing import List, Union, Dict

class ExpressionParser:
    def __init__(self):
        self.precedence = {
            '+': 1,
            '-': 1,
            '*': 2,
            '/': 2,
            '%': 2,
            '^': 3
        }
    
    def tokenize(self, expression: str) -> List[str]:
        """
        Tokenize the input expression into individual tokens.
        Handles numbers, string literals, attributes, and operators.
        """
        token_pattern = r'''
            (\d+(?:\.\d+)?)|
            ('[^']*')|
            ([a-zA-Z_.][a-zA-Z0-9_.]*)|
            ([+\-*/^%()])|
            (\s+)
        '''
        tokens = re.findall(token_pattern, expression, re.VERBOSE)
        
        parsed_tokens = [
            token for group in tokens 
            for token in group 
            if token and not token.isspace()
        ]
        
        return parsed_tokens
    
    def is_number(self, token: str) -> bool:
        """Check if a token is a numeric value."""
        try:
            float(token)
            return True
        except ValueError:
            return False
    
    def is_string_literal(self, token: str) -> bool:
        """Check if a token is a string literal."""
        return token.startswith("'") and token.endswith("'")
    
    def is_attribute(self, token: str) -> bool:
        """Check if a token is an attribute/identifier."""
        return re.match(r'^[a-zA-Z_.][a-zA-Z0-9_.]*$', token) is not None
    
    def parse_expression(self, expression: str, context: Dict[str, Union[int, float, str]] = None) -> List[str]:
        """
        Parse an infix expression using the Shunting Yard algorithm.
        
        :param expression: Input expression string
        :param context: Optional dictionary to resolve attribute values
        :return: Postfix (Reverse Polish Notation) expression
        """
        if context is None:
            context = {}
        
        tokens = self.tokenize(expression)
        
        output_queue = []
        operator_stack = []
        
        for token in tokens:
            if self.is_number(token):
                output_queue.append(token)
            
            elif self.is_string_literal(token):
                output_queue.append(token)
            
            elif self.is_attribute(token):
                if token in context:
                    output_queue.append(str(context[token]))
                else:
                    output_queue.append(token)
            
            elif token in self.precedence:
                while (operator_stack and 
                       operator_stack[-1] != '(' and 
                       (self.precedence.get(operator_stack[-1], 0) > self.precedence.get(token, 0) or 
                        (self.precedence.get(operator_stack[-1], 0) == self.precedence.get(token, 0)))):
                    output_queue.append(operator_stack.pop())
                operator_stack.append(token)
            
            elif token == '(':
                operator_stack.append(token)
            
            elif token == ')':
                while operator_stack and operator_stack[-1] != '(':
                    output_queue.append(operator_stack.pop())
                
                if operator_stack and operator_stack[-1] == '(':
                    operator_stack.pop()
                else:
                    raise ValueError("Mismatched parentheses")
        
        while operator_stack:
            if operator_stack[-1] == '(':
                raise ValueError("Mismatched parentheses")
            output_queue.append(operator_stack.pop())
        
        return output_queue
    
    def evaluate_postfix(self, postfix_tokens: List[str]) -> Union[int, float, str]:
        """
        Evaluate a postfix (Reverse Polish Notation) expression.
        
        :param postfix_tokens: List of tokens in postfix notation
        :return: Result of the expression
        """
        stack = []
        
        for token in postfix_tokens:
            if self.is_number(token):
                stack.append(float(token))
            
            elif self.is_string_literal(token):
                stack.append(token[1:-1])
            
            elif token in self.precedence:
                if len(stack) < 2:
                    raise ValueError("Invalid expression")
                
                right = stack.pop()
                left = stack.pop()
                
                if token == '+':
                    if isinstance(left, str) or isinstance(right, str):
                        stack.append(str(left) + str(right))
                    else:
                        stack.append(left + right)
                elif token == '-':
                    stack.append(left - right)
                elif token == '*':
                    stack.append(left * right)
                elif token == '/':
                    if right == 0:
                        raise ValueError("Division by zero")
                    stack.append(left / right)
                elif token == '%':
                    stack.append(left % right)
                elif token == '^':
                    stack.append(left ** right)
        
        if len(stack) != 1:
            raise ValueError("Invalid expression")
        
        return stack[0]
    
    def evaluate(self, expression: str, context: Dict[str, Union[int, float, str]] = None) -> Union[int, float, str]:
        """
        Evaluate an expression with optional context for attributes.
        
        :param expression: Input expression string
        :param context: Optional dictionary to resolve attribute values
        :return: Result of the expression
        """
        postfix_tokens = self.parse_expression(expression, context)
        return self.evaluate_postfix(postfix_tokens)