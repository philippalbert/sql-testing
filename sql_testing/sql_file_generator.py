import logging
import random
from pathlib import Path

import sqlalchemy
import yaml
from sqlalchemy import Column, MetaData, Table, insert
from sqlalchemy.schema import CreateTable

from .utils import get_random_list, get_random_suffix


class SqlFileGenerator:
    def __init__(self, engine, path_test_setup):
        self.engine = engine
        self.path_test_setup = path_test_setup
        # self.mapping_dict = {}  # self._get_test_table_mapping_info()
        self.string_to_store = ""

    @staticmethod
    def read_yaml_file(path):
        """Read provided yaml file"""

        with open(path, "r") as stream:
            setup_config = yaml.safe_load(stream)

        return setup_config

    def generate_sql_file(self):
        """Generate sql file via yaml file"""

        with self.engine.connect() as connection:
            # use connection to create a transaction object. This object is used for rollback at the end
            # as we don't want the test objects to pollute the database
            transaction = connection.begin()

            config = self.read_yaml_file(self.path_test_setup)
            self.create_tables_from_yaml(connection, config.get("tables"))

            transaction.rollback()

        self._store_stmt_in_sql_file(self.path_test_setup, self.string_to_store)

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
                return mapping_dict  # , suffix

    def create_tables_from_yaml(self, conn, table_config):
        """Create tables from yaml config"""

        for key, val in table_config.items():

            # copy an existing structure if flag exists is set to True
            if val.get("exists"):

                # get table object we want to copy
                table_obj = self._get_meta_data(conn).tables[key]

                self._copy_existing_table(conn, table_obj, val.get("number_of_rows"))

            else:
                self._create_new_table(conn, key, val, val.get("number_of_rows"))

        # store results in sql file
        self._store_stmt_in_sql_file(self.path_test_setup, self.string_to_store)

    @staticmethod
    def _get_meta_data(engine):
        """search db objects like views or tables"""
        meta_data = MetaData(engine)
        meta_data.reflect(views=True)
        return meta_data

    def _create_new_table(self, conn, table_name, table_config, n_rows):

        columns = []
        for key, val in table_config.get("column_names").items():

            col_type = val.get("type")
            col_obj = Column(key, getattr(sqlalchemy, col_type))
            columns.append(col_obj)

        # create table instance
        table_obj = Table(table_name, self._get_meta_data(conn), *columns)
        # prepare Create table with bind of current connection and execute it
        create_res = CreateTable(table_obj, bind=conn)
        conn.execute(create_res)
        # add result of create table statement to statement string
        self.string_to_store += str(create_res) + ";\n"

        val_dict = {}
        for key, val in table_config.get("column_names").items():
            val_dict[key] = get_random_list(val, n_rows)

        # get column order for insert statements
        column_order = [col[0] for col in table_obj.columns._collection]

        test_entries = self.rearrange_dict_for_insert(column_order, val_dict, n_rows)

        # add subset of entries
        for s in test_entries:
            insert_res = insert(table_obj, values=s, bind=conn)

            s_convert = [
                "'" + test + "'" if isinstance(test, str) else test for test in s
            ]

            # store insert statements in statement string
            self.string_to_store += (
                str(insert_res) % dict(zip(column_order, s_convert)) + ";\n"
            )

            # conn.execute(insert_res)
            conn.execute(table_obj.insert(s))

    @staticmethod
    def rearrange_dict_for_insert(column_order, insert_dict, n_rows):

        result = []
        for i in range(n_rows):
            result.append([insert_dict[col][i] for col in column_order])
        return tuple(result)

    @staticmethod
    def _store_stmt_in_sql_file(path: str, statement: str):
        """Store a statement extracted from yaml file in sql"""
        # create path variable
        path = Path(path)

        # change suffix as we want to store in a .sql file
        path = path.parent / (path.stem + ".sql")

        with open(path, "w") as file:
            file.write(statement)

    def _copy_existing_table(self, conn, table_obj, number_of_rows=-1):
        """Copy an existing table structure with an additional suffix"""

        # store rows of original table
        table_entries = conn.execute(table_obj.select()).all()

        # take all data or just a subset of the original table
        if number_of_rows != -1:
            try:
                test_entries = random.sample(table_entries, number_of_rows)
            except ValueError:
                logging.error(
                    "Number of rows specified in yaml file exceeds rows in table"
                )
                raise
        else:
            # in case of no subset take the whole data set, hence all entries
            # in table
            test_entries = table_entries

        self._change_table_constraints(table_obj)

        # create table object, store it and execute it
        create_res = CreateTable(table_obj, bind=conn)

        # add result of create table statement to statement string
        self.string_to_store += str(create_res) + ";\n"

        # get column order for insert statements
        column_order = [col[0] for col in table_obj.columns._collection]

        # add subset of entries
        for s in test_entries:
            insert_res = insert(table_obj, values=s, bind=conn)

            s_convert = [
                "'" + test + "'" if isinstance(test, str) else test for test in s
            ]

            # store insert statements in statement string
            self.string_to_store += (
                str(insert_res) % dict(zip(column_order, s_convert)) + ";\n"
            )

    @staticmethod
    def _change_table_constraints(table_obj):
        """Change name of table constraints"""
        new_constraints = []
        for c in table_obj.constraints:
            constraint = c
            constraint.name = constraint.name + "_" + get_random_suffix(8)
            new_constraints.append(constraint)
