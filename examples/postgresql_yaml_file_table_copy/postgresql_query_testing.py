from pathlib import Path
from pprint import pprint

import sqlalchemy

from sql_testing.db_specific_test import DbSpecificTest
from sql_testing.sql_file_generator import SqlFileGenerator


def test_your_function():
    """Example on how to test a postgresql function"""

    # create an engine to interact with your specific database
    engine = sqlalchemy.create_engine(
        "postgresql://philipp:philipp@localhost:5432/postgres"
    )

    # get path to specific example folder
    path = Path(__file__).parent

    sql_file_generator = SqlFileGenerator(
        path_test_setup=str(path / Path("setup.yaml")), engine=engine
    )

    sql_file_generator.generate_sql_file()

    db_specific_test = DbSpecificTest(
        path_test_setup=str(path / Path("setup.sql")),
        path_to_call=str(path / Path("postgresql_example_call.sql")),
        target="famous_people",
        engine=engine,
    )

    with db_specific_test.run() as result:
        pprint(result)


if __name__ == "__main__":
    test_your_function()
