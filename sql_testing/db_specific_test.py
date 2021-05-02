from contextlib import contextmanager
from re import sub

from .base_test import BaseTest
from .sql_statement_properties import SqlStatementProperties
from .utils import get_random_suffix


class DbSpecificTest(BaseTest):
    def __init__(self, engine, path_test_setup, path_to_call, target):
        self.engine = engine

        # call __init__ of base class to initialize all other parameters
        # and to check if files provided in path variables exist
        super().__init__(path_test_setup, path_to_call, target, engine)

        # create instances which include information about provided queries
        self.setup_properties = self._get_object_names_from_files(self.path_test_setup)
        self.call_properties = self._get_object_names_from_files(self.path_to_call)

    def _get_object_names_from_files(self, path):

        if path.split(".")[-1] == "sql":
            return SqlStatementProperties(self.read_sql_file(path))
        NotImplementedError("File type should be sql")

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
        else:
            raise TypeError(f"Execution of file type {file_type} is not implemented.")

    def _get_test_table_mapping_info(self):
        """Get mapping information and ensure test table name is not equal to existing tables"""
        # make sure to proceed only if db objects with random suffix do not accidentally
        # exist in db
        while True:
            # get a random suffix for tables
            suffix = get_random_suffix(5)

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
