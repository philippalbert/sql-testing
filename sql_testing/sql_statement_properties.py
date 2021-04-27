class SqlStatementProperties:
    IDENTIFIER = {
        "PRE_TABLE_STATEMENTS": ["from", "join", "table", "into", "update"],
        "PRE_VIEW_STATEMENTS": ["view"],
    }

    def __init__(self, statement_list):
        self.statement_list = statement_list

    @property
    def tables(self):
        """Get all tables specified in statements"""
        return self._search_statement_list(s_type="PRE_TABLE_STATEMENTS")

    @property
    def views(self):
        """Get all views specified in statements"""
        return self._search_statement_list(s_type="PRE_VIEW_STATEMENTS")

    def _search_statement_list(self, s_type="PRE_TABLE_STATEMENTS"):
        """Get all objects in statements defined by type"""
        objects = []

        # iterate over list of statements
        for statement in self.statement_list:
            # add objects of all statements to one list
            objects.extend(self._search_property_name(statement, s_type))

        # get rid of duplicates and return list
        return list(set(objects))

    def _search_property_name(self, statement, s_type="PRE_TABLE_STATEMENTS"):
        w_list = statement.split()

        # search for table identifiers and return following table name
        table_list = [
            w_list[index + 1]
            for index, word in enumerate(w_list)
            if word.lower() in self.IDENTIFIER[s_type]
        ]

        return table_list
