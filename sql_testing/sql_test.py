import sqlalchemy
import logging


# todo: Check if tables in database exist
# todo: Add cleanup for database via contextmanager

# sqlalchemy.engine.Engine

class TestSql:
    """Class for SQL testing

    Class takes provided SQL files and tests it. First the test setup file
    will be triggered. This file contains e.g. INSERT, CREATE, ... statements
    on which the main statement (in path_to_call) relies on. After this
    the sql statement which shall be tested will be triggered (in path_to_call).
    The last step compares the result of the second step with the expected output
    provided in path_expected.

    :param path_test_setup: path to test setup with sql statements
    :param path_to_call: path to statement which shall be tested
    :param path_expected: path to statement with expected output
    :param engine: sqlalchemy engine (if none provided local sqlite engine will be used)
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

