from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.orm import sessionmaker
import logging
import os
from contextlib import contextmanager


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
    :param target: target table/view where the result can be found
    :param engine: sqlalchemy engine (if none provided local sqlite engine will be used)
    """

    def __init__(self, path_test_setup, path_to_call, target, engine=None):

        self.path_test_setup = path_test_setup
        self.path_to_call = path_to_call
        self.target = target

        # check if defined files exist
        if not self.check_file_existence():
            raise FileNotFoundError('Input file does not exist')

        # if engine is not defined get sqlite as base db
        if engine is None:
            self.engine = create_engine('sqlite://')
        else:
            self.engine = engine

    def check_file_existence(self):
        """check if input files exist"""

        # get class items
        for var, path in self.__dict__.items():
            # check if "path_" is included in variable name
            if 'path_' in var and not os.path.isfile(path):
                return False

        return True

    @staticmethod
    def read_sql_file(path, statement_separator=';'):
        """Read provided sql file"""
        with open(path, 'r') as file:
            file_content = file.read()

        return file_content.split(statement_separator)

    @staticmethod
    def execute_multiple_statement(conn, statements):
        """Execute multiple sql statements"""
        # todo:  add try except for crappy sql statements?!
        for s in statements:
            conn.execute(s)

    @staticmethod
    def search_db_obj_by_name(engine, name):
        """search a db object like a view or table by name"""
        meta_data = MetaData(engine)
        meta_data.reflect(views=True)
        return meta_data.tables[name]

    @contextmanager
    def run(self):
        """run test"""

        statements_test_setup = self.read_sql_file(self.path_test_setup)
        statements_main_call = self.read_sql_file(self.path_to_call)

        with self.engine.begin() as conn:
            # execute multiple sql statements to setup testing
            self.execute_multiple_statement(conn, statements_test_setup)

            # execute main sql statement which shall be tested
            self.execute_multiple_statement(conn, statements_main_call)

            # get target table instance
            target_table_instance = self.search_db_obj_by_name(conn, self.target)

            # get all entries in table and yield the result
            yield conn.execute(target_table_instance.select()).all()



    # class SqlStatementProperties:
    #     PRE_TABLE_STATEMENTS = ['select', 'join']
    #
    #     def __init__(self, statement):
    #         self.statement = statement

