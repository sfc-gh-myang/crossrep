import os
from unittest import TestCase
import scriptloader

class Test_ScriptLoader(TestCase):
    def test_get_statement_blocks(self):

        nothing_special = "nothing special;"
        statements = scriptloader.get_statement_blocks(nothing_special)
        if statements[0] != "nothing special":
            self.fail('Failed:  should be \n"nothing special" but got \n"' + statements[0])
        multiple_line_comment_block = "ha ha /* this is a test\n; ; */; abc --;"
        statements = scriptloader.get_statement_blocks(multiple_line_comment_block)
        if len(statements) != 2 or statements[0] != "ha ha /* this is a test\n; ; */" or statements[1] != ' abc --;':
            self.fail(
                'Failed: statement[0] should be \n"ha ha /* this is a test\n; ; */" but got \n"' + statements[0] + '"'
                + "\n" + 'statement[1] should be \n" abc --;" but got \n"' + statements[1] + '"')
        sp_bare = 'create PROCEDURE if not exists "SP_CYCLETIMEENGINE_STEPLEVELCT_TST"()' \
                  + "\nRETURNS VARCHAR(16777216)\nLANGUAGE JAVASCRIPT\nEXECUTE AS CALLER\nAS '" \
                  + "'\nsome code ; code 2 ; return result;\n';"
        sp_text = nothing_special + "\n" + sp_bare + "\n" + nothing_special + "\n" + sp_bare + "\n" + nothing_special
        statements = scriptloader.get_statement_blocks(sp_text)
        if statements[0] != "nothing special":
            self.fail('Failed:  statement[0] should be \n"nothing special" but got \n"' + statements[0])
        if statements[1] != '\n' + sp_bare[:-1]:
            self.fail("Failed: stored procedure not match, should be:\n\n" + sp_bare[:-1]
                      + "\nbut got:\n" + statements[1])





