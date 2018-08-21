
from core import JsonQueryParser


class MongoQueryBuilder(object):

    def __init__(self):
        self.parser = JsonQueryParser()

    def build(self, expressions):

        query_expressions = []

        if isinstance(expressions, list):

            for expression in expressions:
                query_expressions.append(self.parser.build_query(expression))

        else:
            query_expressions.append(self.parser.build_query(expressions))

        return {'$and': query_expressions}
