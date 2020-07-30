from unittest import TestCase
import scriptloader

class TestReplace_semicolon_in_block(TestCase):
    def test_get_statement_blocks(self):
        code_block_with_comments = "abc /*/*\n;\n--*/cdc"
        code_block_converted = "abc /*/*\nsemicolon\n--*/cdc"
        returned_text = scriptloader.replace_semicolon_in_block("/*", "*/", code_block_with_comments)
        if returned_text != code_block_converted:
            self.fail("Failed:result does not match, should be:\n\n" + code_block_converted
                      + "\nbut got:\n" + returned_text)

        code_block_with_stored_proc = '''create PROCEDURE if not exists "SP_CYCLETIMEENGINE_STEPLEVELCT_ASM"()
    RETURNS VARCHAR(16777216)
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
    AS '
        /****************************************************************************************************************************************************************
         * Cycle Time Engine for TEST Area. Writes into the "ENG_WW_SPE_DM"."MFGOPS"."CycleTimeEngine_StepLevelCT_TST" table.
         * Author: NDHAKAL/PRANGANAGARA
         * Created Date: 04/24/2020;
         * Version: 1.0.0
         * Modified By;
         * Modified Date;
         * Modified For;
         ****************************************************************************************************************************************************************/
            var
            result = executor.jsonResult(finalStatus);
            return result;

';'''
        code_block_with_stored_proc_converted = '''create PROCEDURE if not exists "SP_CYCLETIMEENGINE_STEPLEVELCT_ASM"()
    RETURNS VARCHAR(16777216)
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
    AS '
        /****************************************************************************************************************************************************************
         * Cycle Time Engine for TEST Area. Writes into the "ENG_WW_SPE_DM"."MFGOPS"."CycleTimeEngine_StepLevelCT_TST" table.
         * Author: NDHAKAL/PRANGANAGARA
         * Created Date: 04/24/2020semicolon
         * Version: 1.0.0
         * Modified Bysemicolon
         * Modified Datesemicolon
         * Modified Forsemicolon
         ****************************************************************************************************************************************************************/
            var
            result = executor.jsonResult(finalStatus)semicolon
            return resultsemicolon

';'''
        returned_text = scriptloader.replace_semicolon_in_block("CREATE PROCEDURE", "\n';", code_block_with_stored_proc)
        if returned_text != code_block_with_stored_proc_converted:
            self.fail("Failed: result does not match, should be:\n\n" + code_block_with_stored_proc_converted
                      + "\nbut got:\n" + returned_text)
        returned_text = scriptloader.replace_semicolon_in_block("CREATE PROCEDURE", "\n';",
                                                                    code_block_with_stored_proc + "\n" + code_block_with_stored_proc)
        if returned_text != code_block_with_stored_proc_converted + "\n" + code_block_with_stored_proc_converted:
            self.fail("Failed: result does not match, should be:\n\n" + code_block_with_stored_proc_converted + "\n" + code_block_with_stored_proc_converted
                      + "\nbut got:\n" + returned_text)


        create_function_code_block = '''create FUNCTION if not exists "MAM_ATTR_VALUE_DATE_FORMAT"(ATTR_ID VARCHAR, ATTR_VALUE VARCHAR)
RETURNS VARCHAR(255)
LANGUAGE SQL
IMMUTABLE
AS '
     CASE
        WHEN  ATTR_ID LIKE ''%DATE%'' AND ATTR_ID NOT LIKE ''%VALIDATE%'' AND REGEXP_LIKE(ATTR_VALUE,''[\\\\d+]{1,4}[-/][\\\\d+]{1,2}[-/][\\\\d+]{2,4}[ T]{0,1}[0-9:]{0,8}'') = 1 
            THEN 
            // comment with ;
            CAST(
                CASE
                    WHEN LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1)) = 4
                    THEN STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1)||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',2), 2,''0'')||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3), 2,''0'')
                    ||
                    CASE
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 5 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)||'':00''
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 8 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)
                    ELSE '' 00:00:00''
                    END
                    WHEN LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1)) < 3 AND LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3)) = 4
                    THEN STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3)||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1), 2,''0'')||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',2), 2,''0'')
                    ||
                    CASE
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 5 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)||'':00''
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 8 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)
                    ELSE '' 00:00:00''
                    END
                    WHEN LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1)) < 3 AND LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3)) < 3
                    THEN ''20''||STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3)||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1), 2,''0'')||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',2), 2,''0'') 
                    ||
                    CASE
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 5 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)||'':00''
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 8 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)
                    ELSE '' 00:00:00''
                    END
				ELSE ATTR_VALUE
                END
            AS VARCHAR(255))
        WHEN  ATTR_ID LIKE ''%DATE%'' AND ATTR_ID NOT LIKE ''%VALIDATE%'' AND REGEXP_LIKE(ATTR_VALUE,''[\\\\d+]{8}'') = 1 
            THEN CAST(SUBSTR(ATTR_VALUE,1,4)||''-''||SUBSTR(ATTR_VALUE,5,2)||''-''||SUBSTR(ATTR_VALUE,7,2)||'' 00:00:00'' AS VARCHAR(255))
        ELSE CAST (ATTR_VALUE AS VARCHAR(255))
    END
';\nabc'''
        create_function_code_block_converted = '''create FUNCTION if not exists "MAM_ATTR_VALUE_DATE_FORMAT"(ATTR_ID VARCHAR, ATTR_VALUE VARCHAR)
RETURNS VARCHAR(255)
LANGUAGE SQL
IMMUTABLE
AS '
     CASE
        WHEN  ATTR_ID LIKE ''%DATE%'' AND ATTR_ID NOT LIKE ''%VALIDATE%'' AND REGEXP_LIKE(ATTR_VALUE,''[\\\\d+]{1,4}[-/][\\\\d+]{1,2}[-/][\\\\d+]{2,4}[ T]{0,1}[0-9:]{0,8}'') = 1 
            THEN 
            // comment with semicolon
            CAST(
                CASE
                    WHEN LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1)) = 4
                    THEN STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1)||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',2), 2,''0'')||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3), 2,''0'')
                    ||
                    CASE
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 5 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)||'':00''
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 8 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)
                    ELSE '' 00:00:00''
                    END
                    WHEN LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1)) < 3 AND LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3)) = 4
                    THEN STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3)||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1), 2,''0'')||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',2), 2,''0'')
                    ||
                    CASE
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 5 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)||'':00''
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 8 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)
                    ELSE '' 00:00:00''
                    END
                    WHEN LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1)) < 3 AND LENGTH(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3)) < 3
                    THEN ''20''||STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',3)||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',1), 2,''0'')||''-''||LPAD(STRTOK(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',1),''-'',2), 2,''0'') 
                    ||
                    CASE
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 5 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)||'':00''
                        WHEN LENGTH(STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)) = 8 THEN '' ''||STRTOK(REPLACE(REPLACE(ATTR_VALUE,''T'','' ''),''/'',''-''),'' '',2)
                    ELSE '' 00:00:00''
                    END
				ELSE ATTR_VALUE
                END
            AS VARCHAR(255))
        WHEN  ATTR_ID LIKE ''%DATE%'' AND ATTR_ID NOT LIKE ''%VALIDATE%'' AND REGEXP_LIKE(ATTR_VALUE,''[\\\\d+]{8}'') = 1 
            THEN CAST(SUBSTR(ATTR_VALUE,1,4)||''-''||SUBSTR(ATTR_VALUE,5,2)||''-''||SUBSTR(ATTR_VALUE,7,2)||'' 00:00:00'' AS VARCHAR(255))
        ELSE CAST (ATTR_VALUE AS VARCHAR(255))
    END
';\nabc'''
        returned_text = scriptloader.replace_semicolon_in_block("CREATE function", "\n';", create_function_code_block)
        assert returned_text == create_function_code_block_converted
        # if returned_text != create_function_code_block_converted:
        #    self.fail("Failed: result does not match, should be:\n\n" + create_function_code_block_converted
        #              + "\nbut got:\n" + returned_text)
        pass

