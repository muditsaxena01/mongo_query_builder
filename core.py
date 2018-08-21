from six import string_types
import re

re_type = type(re.compile('regex'))

OPERATORS = [
    'EQ', 'NEQ', 'GT', 'LT', 'GTE', 'LTE', 'CONTAINS', 'LACKS', 'STARTS', 'ENDS', 'REGEX', 'IN', 'NIN',
    'EXISTS', 'TYPE', 'SIZE', 'ELEM_MATCH', 'HAS', 'AND', 'OR', 'NOT', 'NOR'
]


LOGICAL_OPERATORS = {
    'AND': {
        'operator': '$and',
        'dataTypes': (list, tuple)
    },
    'OR': {
        'operator': '$or',
        'dataTypes': (list, tuple)
    },
    'NOT': {
        'operator': '$not',
        'dataTypes': (list, tuple)
    },
    'NOR': {
        'operator': '$nor',
        'dataTypes': (list, tuple)
    }
}


COMPARISON_OPERATORS = {
    'EQ': {
        'operator': None,
        'dataTypes': None
    },
    'NEQ': {
        'operator': '$ne',
        'dataTypes': None
    },
    'GT': {
        'operator': '$gt',
        'dataTypes': None
    },
    'LT': {
        'operator': '$lt',
        'dataTypes': None
    },
    'GTE': {
        'operator': '$gte',
        'dataTypes': None
    },
    'LTE': {
        'operator': '$lte',
        'dataTypes': None
    }
}


RANGE_COMPARISON_OPERATORS = {
    'IN': {
        'operator': '$in',
        'dataTypes': (list, tuple)
    },
    'NIN': {
        'operator': '$nin',
        'dataTypes': (list, tuple)
    }
}


STRING_OPERATORS = {
    'CONTAINS': {
        'operator': '$regex',
        'dataTypes': tuple(list(string_types) + [re_type])
    },
    'LACKS': {
        'operator': '$regex',
        'prefix': '^((?!',
        'suffix': ').)*$',
        'dataTypes': tuple(list(string_types) + [re_type])
    },
    'STARTS': {
        'operator': '$regex',
        'prefix': '^',
        'dataTypes': tuple(list(string_types) + [re_type])
    },
    'ENDS': {
        'operator': '$regex',
        'suffix': '$',
        'dataTypes': tuple(list(string_types) + [re_type])
    },
    'REGEX': {
        'operator': '$regex',
        'dataTypes': tuple(list(string_types) + [re_type])
    }
}

ELEMENT_OPERATORS = {
    'EXISTS': {
        'operator': '$exists',
        'dataTypes': (bool)
    },
    'TYPE': {
        'operator': '$type',
        'dataTypes': (string_types, int),
        'values': [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, -1, 127,
            'double', 'string', 'object', 'array', 'binData', 'undefined', 'objectId', 'bool', 'long',
            'date', 'null', 'regex', 'dbPointer', 'javascript', 'symbol', 'javascriptWithScope', 'int',
            'timestamp', 'decimal', 'minKey', 'maxKey', 'number'
        ]
    }
}

ARRAY_OPERATORS = {
    'SIZE': {
        'operator': '$size',
        'dataTypes': (int)
    },
    'ELEM_MATCH': {
        'operator': '$exists',
        'dataTypes': (dict)
    },
    'HAS': {
        'operator': '$all',
        'dataTypes': (list, tuple)
    }
}


class JsonQueryParser(object):

    def __init__(self, default=None):
        self.default = default

    def __validate_expression_datatype(self, operator, expression, acceptable_datatypes=None, field=None):

        field = f'for field {field}' if field else ''

        if not isinstance(expression, acceptable_datatypes):
            raise TypeError(f'Object of type {expression.__class__.__name__} '
                            f'is not a valid query expression for {operator} {field}')

    def __validate_expression(self, expression):

        if not isinstance(expression, dict):
            raise TypeError(f'Object of type {expression.__class__.__name__} '
                            f'is not a valid query expression')

        if 'operator' not in expression:
            raise ValueError('Query expression must have \'operator\'')

    def __build_logical_expression(self, operator, expressions):

        query_expressions = []

        found_operator = LOGICAL_OPERATORS.get(operator, {})

        mongo_operator = found_operator.get('operator')
        acceptable_datatypes = found_operator.get('dataTypes')

        if acceptable_datatypes:
            self.__validate_expression_datatype(operator, expressions, acceptable_datatypes)

        for expression in expressions:
            query_expressions.append(self.build_query(expression))

        return {mongo_operator: query_expressions}

    def __build_array_query_expression(self, operator, operand, field):

        query_expression = {}

        found_operator = ARRAY_OPERATORS.get(operator, {})

        mongo_operator = found_operator.get('operator')
        acceptable_datatypes = found_operator.get('dataTypes')

        if acceptable_datatypes:
            self.__validate_expression_datatype(operator, operand, acceptable_datatypes, field)

        if operator == 'ELEM_MATCH':
            query_expression = {mongo_operator: self.build_query(operand)}
        else:
            query_expression = {mongo_operator: operand}

        return query_expression

    def __build_general_comparison_expression(self, operator, operand, field):

        found_operator = COMPARISON_OPERATORS.get(operator, {}) or RANGE_COMPARISON_OPERATORS.get(operator, {})

        mongo_operator = found_operator.get('operator')
        acceptable_datatypes = found_operator.get('dataTypes')

        if acceptable_datatypes:
            self.__validate_expression_datatype(operator, operand, acceptable_datatypes, field)

        return {mongo_operator: operand} if mongo_operator else operand

    def __build_element_expression(self, operator, operand, field):

        found_operator = ELEMENT_OPERATORS.get(operator, {})

        mongo_operator = found_operator.get('operator')
        acceptable_datatypes = found_operator.get('dataTypes')
        acceptable_values = found_operator.get('values')

        if acceptable_datatypes:
            self.__validate_expression_datatype(operator, operand, acceptable_datatypes, field)

        if acceptable_values and operand not in acceptable_values:
            raise ValueError(f'Unknown operand value {operand} for operator {operator} in {field}')

        return {mongo_operator: operand}

    def __build_string_query_expression(self, operator, operand, field, options=None):

        expression = {}

        found_operator = STRING_OPERATORS.get(operator, {})

        mongo_operator = found_operator.get('operator')
        acceptable_datatypes = found_operator.get('dataTypes')

        if acceptable_datatypes:
            self.__validate_expression_datatype(operator, operand, acceptable_datatypes, field)

        mongo_operand = operand

        if isinstance(operand, string_types):

            if found_operator.get('prefix'):
                mongo_operand = found_operator['prefix'] + mongo_operand

            if found_operator.get('suffix'):
                mongo_operand = mongo_operand + found_operator['suffix']

            expression.update({mongo_operator: operand})

            if options and isinstance(options, string_types) and set(list(options)) == set(list('igsxm')):
                expression.update({'$options': options})
            else:
                unknown_options = ''.join(list(set(list(options)) - set(list('igsxm'))))
                raise ValueError(f'Unknown options {unknown_options} for Operator {operator} in field {field}')
        else:
            expression.update({mongo_operand: operand})

        return expression

    def build_query(self, expression):

        self.__validate_expression(expression)

        operator = expression.get('operator')
        operand = expression.get('value')
        expressions = expression.get('expressions')
        field = expression.get('field')
        options = expression.get('options')

        if expression['operator'] in LOGICAL_OPERATORS:
            return self.__build_logical_expression(operator, expressions)

        if expression['operator'] in COMPARISON_OPERATORS:
            return {
                field: self.__build_general_comparison_expression(operator, operand, field)
            }

        else:
            if not field and isinstance(field, string_types):
                raise ValueError('Invalid value for field (None)')

            if expression['operator'] in RANGE_COMPARISON_OPERATORS:
                return self.__build_general_comparison_expression(operator, operand, field)

            elif expression['operator'] in STRING_OPERATORS:
                return {
                    field: self.__build_string_query_expression(operator, operand, field, options)
                }

            elif expression['operator'] in ELEMENT_OPERATORS:
                return {
                    field: self.__build_element_expression(operator, operand, field)
                }

            elif expression['operator'] in ARRAY_OPERATORS:
                return {
                    field: self.__build_array_query_expression(operator, operand, field)
                }

            elif self.default:
                return self.default(expression)

            else:
                raise ValueError(f'Unknown operator {operator} for field {field}')
