import logging
import random
import string
from contextlib import contextmanager
from pathlib import Path
from re import sub

import yaml
from sqlalchemy import insert
from sqlalchemy.schema import CreateTable

from .base_test import BaseTest
from .sql_statement_properties import SqlStatementProperties

# todo: Check if tables in database exist
# todo: Add cleanup for database via contextmanager
# engine = sqlalchemy.create_engine('postgresql://philipp:philipp@localhost:5432/postgres')


class DbSpecificTest(BaseTest):
    def __init__(self, engine, path_test_setup, path_to_call, target):
        self.engine = engine

        # call __init__ of base class to initialize all other parameters
        # and to check if files provided in path variables exist
        super().__init__(path_test_setup, path_to_call, target, engine)

        # create instances which include information about provided queries
        self.setup_properties = SqlStatementProperties(
            self.read_sql_file(self.path_test_setup)
        )
        self.call_properties = SqlStatementProperties(
            self.read_sql_file(self.path_to_call)
        )

    @staticmethod
    def _get_random_suffix(length=5):
        """Create a random db object suffix"""
        chars = string.ascii_lowercase + string.digits
        random_choice = "".join(random.choice(chars) for _ in range(length))
        return random_choice

    @staticmethod
    def _get_obj_mapping(suffix, obj_names):
        """get dictionary for mapping of db objects"""
        d = {obj: obj + "_" + suffix for obj in obj_names}
        return d

    def _get_all_query_objects(self):
        """get a list of all query objects"""
        tables = self.setup_properties.tables + self.call_properties.tables
        views = self.setup_properties.views + self.call_properties.views
        return list(set(tables + views))

    @staticmethod
    def _has_intersection(db_obj, query_obj):
        """check if two lists have an intersection"""
        for q_obj in query_obj:
            if q_obj in db_obj:
                return True

        return False

    @staticmethod
    def read_sql_file(path, statement_separator=";", mapping_dict=None):
        """Read provided sql file"""
        with open(path, "r") as file:
            file_content = file.read()

        # get rid of newlines
        file_content = file_content.replace("\n", " ").lower()

        if mapping_dict is not None:
            for original_obj, test_obj in mapping_dict.items():
                pattern = fr"\s{original_obj}\s"
                new_name = " " + test_obj + " "
                file_content = sub(pattern, new_name, file_content)

        return file_content.split(statement_separator)

    def _get_test_table_mapping_info(self):
        """Get mapping information and ensure test table name is not equal to existing tables"""
        # make sure to proceed only if db objects with random suffix do not accidentally
        # exist in db
        while True:
            # get a random suffix for tables
            suffix = self._get_random_suffix(5)

            # get all views and tables mentioned in query files
            query_objects = self._get_all_query_objects()

            # create mapping dictionary for db objects
            mapping_dict = self._get_obj_mapping(suffix, query_objects)

            # get database objects
            db_objects = self._get_db_objects(self.engine)

            # check that mapping_dict values are not already part of database
            if not self._has_intersection(db_objects.tables, mapping_dict.values()):
                return mapping_dict, suffix

    @contextmanager
    def run(self):

        # get mapping table names and related suffix
        mapping_dict, suffix = self._get_test_table_mapping_info()

        # create all objects with mapping name
        statements_test_setup = self.read_sql_file(
            self.path_test_setup, mapping_dict=mapping_dict
        )
        statements_main_call = self.read_sql_file(
            self.path_to_call, mapping_dict=mapping_dict
        )

        # establish a connection by using a context manager to ensure the connection will be closed
        # after usage.
        with self.engine.connect() as connection:
            # use connection to create a transaction object. This object is used for rollback at the end
            # as we don't want the test objects to pollute the database
            transaction = connection.begin()

            # execute multiple sql statements to setup testing
            self.execute_multiple_statement(connection, statements_test_setup)

            # execute main sql statement which shall be tested
            self.execute_multiple_statement(connection, statements_main_call)

            # get target table instance
            target_table_instance = self._get_db_obj_by_name(
                connection, self.target + "_" + suffix
            )

            # get all entries in table and yield the result
            yield connection.execute(target_table_instance.select()).all()

            # rollback transaction
            transaction.rollback()
