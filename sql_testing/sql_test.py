import sqlalchemy
import logging


# todo: Check if tables in database exist
# todo: Add cleanup for database via contextmanager

# sqlalchemy.engine.Engine

class TestSql:
    """Class to SQL testing

    Class takes provided SQL files and tests it by calling
        1) path_test_setup
        2) path_to_call
        3) path_expected

    """

    def __init__(self, path_test_setup, path_to_call, path_expected, engine=None):

        self.path_test_setup = path_test_setup
        self.path_to_call = path_to_call
        self.path_expected = path_expected

        # if engine is not defined get sqlite as base db
        if engine is None:
            self.engine = sqlalchemy.create_engine('sqlite://')
        else:
            self.engine = engine

    @staticmethod
    def read_sql_file(path, statement_separator=';'):
        """Read provided sql file"""
        with open(path, 'r') as file:
            file_content = file.read()

        return file_content.split(statement_separator)



#class SqlStatementProperties:
#    PRE_TABLE_STATEMENTS = ['select', 'join']
#    def __init__(self, statement):
#
#        self.statement = statement

