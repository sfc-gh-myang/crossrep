from unittest import TestCase
import scriptloader
import os
import re

class TestGet_statement_blocks(TestCase):
    def test_get_statement_blocks(self):
        filedict = scriptloader.list_scripts("/Users/mlee/crossrep/", "ddl2")
        failedStatements = []
        for file_obj in filedict:
            filename = os.path.join(file_obj['path'], file_obj['file'])
            print("---- Reading File: %s" % (filename))
            f = open(filename, "r")
            long_sql_text = f.read()
            f.close()
            sql_statements = scriptloader.get_statement_blocks(long_sql_text);
            failures = ''

            for statement in sql_statements:
                upper_statement = statement.upper().lstrip()
                if upper_statement.find('CREATE PROCEDURE') >= 0 or upper_statement.find('CREATE FUNCTION') >= 0:
                    continue
                # count number of create statements
                col = upper_statement.split("\nCREATE ")

                if len(col) > 1:
                    print("\n\n" + "------- File: %s \n Found %d CREATE in statement \n %s" % (filename, len(col), statement))
                    #failures += "\n\n" + "------- File: %s \n Found %d in statement \n %s" % (filename, len(res), statement)
        #print(failures)
        # self.fail(failures)

