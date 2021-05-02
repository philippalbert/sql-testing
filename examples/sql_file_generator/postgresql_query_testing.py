from pathlib import Path

import sqlalchemy

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
        path_test_setup=str(path / Path("setup.yaml")),
        engine=engine,
    )

    sql_file_generator.generate_sql_file()


if __name__ == "__main__":
    test_your_function()
