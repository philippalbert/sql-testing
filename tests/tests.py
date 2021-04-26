import os
from tempfile import TemporaryDirectory
from unittest import TestCase

from sqlalchemy.exc import OperationalError

from sql_testing.base_test import BaseTest


class TestBaseTest(TestCase):
    def test_check_file_existence(self):
        """ensure file existence check works as expected

        1) check to ensure check works if both files exist
        2) check to ensure that error will be raised if file does not exist
        """

        with TemporaryDirectory() as temp_dir:
            file_1 = os.path.join(temp_dir, "dummy_file1.txt")
            file_2 = os.path.join(temp_dir, "dummy_file2.txt")

            # no error expected
            with open(file_1, "w+") as dummy_file1, open(file_2, "w+") as dummy_file2:
                dummy_file1.write("something")
                dummy_file2.write("something")
                BaseTest(file_1, file_2, "dummy_target")

            # error expected
            file_3 = os.path.join(temp_dir, "dummy_file3.txt")
            with self.assertRaises(FileNotFoundError):
                BaseTest(file_1, file_3, "dummy_target")

    def test_read_sql_file(self):
        """test that file reading works as expected"""

        with TemporaryDirectory() as temp_dir:
            file_1 = os.path.join(temp_dir, "dummy_file1.txt")
            file_2 = os.path.join(temp_dir, "dummy_file2.txt")

            query_string = "select * from some_table; select * from another_table"
            expected = query_string.split(";")

            # no error expected
            with open(file_1, "w+") as dummy_file1, open(file_2, "w+") as dummy_file2:
                dummy_file1.write(query_string)
                dummy_file2.write("something")

                base_test = BaseTest(file_1, file_2, "dummy_target")

            output = base_test.read_sql_file(file_1)

            self.assertEqual(output, expected)

    def test_run(self):
        """test run method"""
        # check for valid statements
        expected = [(53.5, "USA"), (47.5, "Germany")]
        base_test = BaseTest(
            path_test_setup="fixtures/run_base_test_setup.sql",
            path_to_call="fixtures/run_base_test_call.sql",
            target="MEAN_AGE_PER_COUNTRY",
        )

        with base_test.run() as result:
            base_test.compare_table_values(result, expected)

        # check for invalid sql statements
        base_test = BaseTest(
            path_test_setup="fixtures/run_base_test_setup_invalid.sql",
            path_to_call="fixtures/run_base_test_call.sql",
            target="MEAN_AGE_PER_COUNTRY",
        )

        with self.assertRaises(OperationalError):
            with base_test.run() as result:
                base_test.compare_table_values(result, expected)
