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
from .sql_statement_properties import SqlStatementProperties, YamlProperties


class DbSpecificTest(BaseTest):
    def __init__(self, engine, path_test_setup, path_to_call, target):
        self.engine = engine

        # call __init__ of base class to initialize all other parameters
        # and to check if files provided in path variables exist
        super().__init__(path_test_setup, path_to_call, target, engine)

        # create instances which include information about provided queries
        self.setup_properties = self._get_object_names_from_files(
            self.path_test_setup, is_call_file=False
        )
        self.call_properties = self._get_object_names_from_files(
            self.path_to_call, is_call_file=True
        )

    def _get_object_names_from_files(self, path, is_call_file=False):

        if path.split(".")[-1] == "sql":
            return SqlStatementProperties(self.read_sql_file(path))
        elif path.split(".")[-1] in ["yaml", "yml"]:
            if not is_call_file:
                return YamlProperties(self.read_yaml_file(path))
            NotImplementedError("Call file type should be sql")

        NotImplementedError("Setup is not implemented for this file type")

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

    def execute_files(self, path_to_file, conn, mapping_dict=None):
        """Execute multiple sql statements"""

        file_type = path_to_file.split(".")[-1]
        if file_type == "sql":
            # read sql file
            statements = self.read_sql_file(path_to_file, mapping_dict=mapping_dict)

            for statement in statements:
                if len(statement.replace(" ", "")) > 0:
                    conn.execute(statement)

        elif file_type == "yaml" or path_to_file.split(".")[-1] == "yml":
            config = self.read_yaml_file(path_to_file)
            self.create_tables_from_yaml(conn, config.get("tables"), mapping_dict)
        else:
            raise TypeError(f"Execution of file type {file_type} is not implemented.")

    def create_tables_from_yaml(self, conn, table_config, mapping_dict):
        """Create tables from yaml config"""

        string_to_store = ""
        for key, val in table_config.items():

            # get table object we want to copy
            table_obj = self._get_db_obj_by_name(conn, key)

            # copy an existing structure if flag exists is set to True
            if val.get("exists"):
                string_to_store += self._copy_existing_table(
                    conn, table_obj, mapping_dict, val.get("number_of_rows")
                )

            else:
                NotImplementedError(
                    "Sorry, but for now no table creation with yaml is implemented"
                )

        # store results in sql file
        self._store_stmt_in_sql_file(
            self.path_test_setup, string_to_store, mapping_dict
        )

    @staticmethod
    def _store_stmt_in_sql_file(path: str, statement: str, mapping_dict: dict):
        """Store a statement extracted from yaml file in sql"""
        # create path variable
        path = Path(path)

        # change suffix as we want to store in a .sql file
        path = path.parent / (path.stem + ".sql")

        # invert created suffix back to normal table name
        for key, val in mapping_dict.items():
            statement = statement.replace(val, key)

        with open(path, "w") as file:
            file.write(statement)

    @staticmethod
    def _copy_existing_table(conn, table_obj, mapping_dict, number_of_rows=-1):
        """Copy an existing table structure with an additional suffix"""

        # create statement string
        statement_str = ""

        # store rows of original table
        table_entries = conn.execute(table_obj.select()).all()

        if number_of_rows != -1:
            try:
                subset_entries = random.sample(table_entries, number_of_rows)
            except ValueError:
                logging.error(
                    "Number of rows specified in yaml file exceeds rows in table"
                )
                raise

        # change name
        table_obj.name = mapping_dict[table_obj.name]

        # update constraints as we have to make sure that constraints
        # are not named equally to already existing ones
        new_constraints = []
        for c in table_obj.constraints:
            constraint = c
            constraint.name = (
                constraint.name + "_" + list(mapping_dict.values())[0].split("_")[-1]
            )
            new_constraints.append(constraint)

        # create table object, store it and execute it
        # table_obj.create(conn)
        create_res = CreateTable(table_obj, bind=conn)
        conn.execute(create_res)

        # add result of create table statement to statement string
        statement_str += str(create_res) + ";\n"

        # add subset of entries
        for s in subset_entries:
            insert_res = insert(table_obj, values=s, bind=conn)

            # store insert statements in statement string
            statement_str += str(insert_res) % s + ";\n"

            conn.execute(table_obj.insert(s))

        return statement_str

    @staticmethod
    def read_yaml_file(path):
        """Read provided yaml file"""

        with open(path, "r") as stream:
            setup_config = yaml.safe_load(stream)

        return setup_config

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

        # establish a connection by using a context manager to ensure the connection will be closed
        # after usage.
        with self.engine.connect() as connection:
            # use connection to create a transaction object. This object is used for rollback at the end
            # as we don't want the test objects to pollute the database
            transaction = connection.begin()

            # execute firstly multiple sql statements to setup testing and secondly
            # the main sql statement
            for paths in [self.path_test_setup, self.path_to_call]:

                self.execute_files(
                    conn=connection,
                    path_to_file=paths,
                    mapping_dict=mapping_dict,
                )

            # get target table instance
            target_table_instance = self._get_db_obj_by_name(
                connection, self.target.lower() + "_" + suffix
            )

            # get all entries in table and yield the result
            yield connection.execute(target_table_instance.select()).all()

            # rollback transaction
            transaction.rollback()
