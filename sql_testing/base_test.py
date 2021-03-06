# import logging
import os
from contextlib import contextmanager

from sqlalchemy import MetaData, create_engine


class BaseTest:
    """Class for SQL testing

    Class takes provided SQL files and tests it. First the test setup file
    will be triggered. This file contains e.g. INSERT, CREATE, ... statements
    on which the main statement (in path_to_call) relies on. After this
    the sql statement which shall be tested will be triggered (in
    path_to_call). The last step compares the result of the second step with
    the expected output provided in path_expected.

    :param path_test_setup: path to test setup with sql statements
    :param path_to_call: path to statement which shall be tested
    :param target: target table/view where the result can be found
    :param engine: engine to use
    """

    def __init__(self, path_test_setup, path_to_call, target, engine=None):

        self.path_test_setup = path_test_setup
        self.path_to_call = path_to_call
        self.target = target

        # check if defined files exist
        self.check_file_existence()

        # get sqlite engine for non dialect tests
        if engine is None:
            self.engine = create_engine("sqlite://")

    def check_file_existence(self):
        """check if input files exist

        Function checks if files needed for setup and main sql exist.
        It therefore iterates through the class dictionary and searches
        for entries with 'path_' specification. If one of the files
        can not be found an error will be raised.

        :raise FileNotFoundError: if file can not be found
        """
        # get class items
        for var, path in self.__dict__.items():
            # check if "path_" is included in variable name
            if "path_" in var and not os.path.isfile(path):
                raise FileNotFoundError(f"File {path} does not exist")

    @staticmethod
    def read_sql_file(path, statement_separator=";", mapping_dict=None):
        """Read provided sql file"""
        with open(path, "r") as file:
            file_content = file.read()

        return file_content.split(statement_separator)

    def execute_files(self, path_to_file, conn):
        """Execute multiple sql statements"""

        # read sql file
        statements = self.read_sql_file(path_to_file)

        for statement in statements:
            if len(statement.replace(" ", "")) > 0:
                conn.execute(statement)

    def _get_db_obj_by_name(self, engine, name):
        """search a db object like a view or table by name"""
        meta_data = self._get_db_objects(engine)
        return meta_data.tables[name]

    @staticmethod
    def _get_db_objects(engine):
        """search db objects like views or tables"""
        meta_data = MetaData(engine)
        meta_data.reflect(views=True)
        return meta_data

    @contextmanager
    def run(self):
        """run test"""

        with self.engine.begin() as conn:
            # execute multiple sql statements to setup testing
            self.execute_files(conn=conn, path_to_file=self.path_test_setup)

            # execute main sql statement which shall be tested
            self.execute_files(conn=conn, path_to_file=self.path_to_call)

            # get target table instance
            target_table_instance = self._get_db_obj_by_name(conn, self.target)

            # get all entries in table and yield the result
            yield conn.execute(target_table_instance.select()).all()

    @staticmethod
    def compare_table_values(target, expected):
        """Compare target table output to expected output"""

        for exp, tar in zip(expected, target):
            assert exp == tar, (
                f"Expected (={exp}) and result (={tar}) is not " f"the same"
            )
