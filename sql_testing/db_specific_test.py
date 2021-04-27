from .base_test import BaseTest

# todo: Check if tables in database exist
# todo: Add cleanup for database via contextmanager
# engine = sqlalchemy.create_engine('postgresql://philipp:philipp@localhost:5432/postgres')


class SqlStatementProperties:
    IDENTIFIER = {
        "PRE_TABLE_STATEMENTS": ["from", "join", "table", "into", "update"],
        "PRE_VIEW_STATEMENTS": ["view"],
    }

    def __init__(self, statement_list):
        self.statement_list = statement_list

    # @staticmethod
    # def _get_word_list(statement):

    @property
    def tables(self):
        """Get all tables specified in statements"""
        tables = []

        # iterate over list of statements
        for statement in self.statement_list:
            # add tables of all statements to one list
            tables.extend(self._search_property_name(statement, "PRE_TABLE_STATEMENTS"))

        # get rid of duplicates and return list
        return list(set(tables))

    @property
    def views(self):
        """Get all views specified in statements"""
        views = []

        # iterate over list of statements
        for statement in self.statement_list:
            # add views of all statements to one list
            views.extend(self._search_property_name(statement, "PRE_VIEW_STATEMENTS"))

        # get rid of duplicates and return list
        return list(set(views))

    def _search_property_name(self, statement, s_type="PRE_TABLE_STATEMENTS"):
        w_list = statement.split()

        # search for table identifiers and return following table name
        table_list = [
            w_list[index + 1]
            for index, word in enumerate(w_list)
            if word.lower() in self.IDENTIFIER[s_type]
        ]

        return table_list


class DbSpecificTest(BaseTest):
    def __init__(self, engine, path_test_setup, path_to_call, target):

        pass

    def run(self):

        pass
