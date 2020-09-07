from unittest import TestCase
#import scriptloader
import crossrep

class TestReplace_semicolon_in_block(TestCase):
    def test_get_statement_blocks(self):
        code_block_with_comments = "abc /*/*\n;\n--*/cdc"
        code_block_converted = "abc /*/*\nsemicolon\n--*/cdc"
        returned_text = scriptloader.search_for_code_block("/*", "*/", code_block_with_comments)
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
        returned_text = scriptloader.search_for_code_block("CREATE PROCEDURE", "\n';", code_block_with_stored_proc)
        if returned_text != code_block_with_stored_proc_converted:
            self.fail("Failed: result does not match, should be:\n\n" + code_block_with_stored_proc_converted
                      + "\nbut got:\n" + returned_text)
        returned_text = scriptloader.search_for_code_block("CREATE PROCEDURE", "\n';",
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
    ENDCREATE PROCEDURE IF NOT EXISTS "REMOVE_STAGE_FILE_TEST"(START_LETTER VARCHAR, END_LETTER VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  
    FUNCTION INSERT_DELETE_TIMESTAMP() {
	TRY {
		VAR FILE_UPDATE_SQL = `UPDATE PROD_MFG_LOAD.UTIL.DELETE_CONTROL 
								SET STAGE_DELETED_ENDTIME = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) 
								WHERE STAGE_FILE IN (${STAGE_IN_LIST});`

		VAR FILE_CMD = SNOWFLAKE.CREATESTATEMENT( {SQLTEXT: FILE_UPDATE_SQL } );
		VAR FILE_RESULT = FILE_CMD.EXECUTE();
	}
	CATCH (ERR)  {
		RESULT =  "FAIL, CODE: " + ERR.CODE + "\\R\\N  STATE: " + ERR.STATE;
		RESULT += "\\R\\N  MESSAGE: " + ERR.MESSAGE;
		RESULT += "\\R\\N STACK TRACE:\\R\\N" + ERR.STACKTRACETXT; 
    RETURN(RESULT);
	}  

  }
  
  /*DESCRIPTION
    THIS STORED PROCEDURE WILL BE USED TO REMOVE STAGE FILES BY READING THE PROD_MFG_LOAD.UTIL.DELETE_CONTROL TABLE AND EXECUTING DELETE SQL. 
	SNOWFLAKE DOES NOT HAVE A BULK DELETE OPTION.  
  */
   
  //VARIABLES USED IN PROXY
  VAR RESULT = ""; //INSTANTIATE TRY RESULTS VARIABLE
  VAR A = 1;
  VAR STAGE_IN_LIST = "";
  
  
  TRY {
	VAR STAGE_SELECT_SQL = `SELECT DATA_SET, STAGE_FILE 
						FROM PROD_MFG_LOAD.UTIL.DELETE_CONTROL
						WHERE STAGE_DELETED_ENDTIME IS NULL
						AND DATA_SET > ''${START_LETTER}''
                        AND DATA_SET <= ''${END_LETTER}''
						ORDER BY 1,2;`
						
	VAR STAGE_CMD = SNOWFLAKE.CREATESTATEMENT( {SQLTEXT: STAGE_SELECT_SQL } );
    VAR STAGE_RESULT = STAGE_CMD.EXECUTE();
	    //IF NO JOBS ARE READY THEN EXIT THE SCRIPT.  IF JOBS EXIST THEN CREATE A PREDICATE TO BE USED IN ALL UPDATE STATEMENTS
    
	IF ( STAGE_RESULT.NEXT() == FALSE)
    {
        RESULT = `SKIPPED, 0, 0,, NO STAGE FILES READY FOR DELETION`;
        RETURN(RESULT)
    }
    ELSE	  
    {
        VAR DATA_SET_VALUE = STAGE_RESULT.GETCOLUMNVALUE(1);
        VAR STAGE_FILE_VALUE = STAGE_RESULT.GETCOLUMNVALUE(2);
        DO {
			IF (A==1){
				STAGE_IN_LIST += `''`+ STAGE_FILE_VALUE + `''\\R\\N `;  //BEGIN BUILDING THE IN CLAUSE FOR THE DATETIME STAMP INSERT
			}
			ELSE {
				STAGE_IN_LIST += `,''`+ STAGE_FILE_VALUE + `''\\R\\N `;
			}
			
			//DELETE THE STAGE FILE
			VAR STAGE_DELETE_SQL = `REMOVE @PROD_MFG_LOAD.${DATA_SET_VALUE}.STAGE/${STAGE_FILE_VALUE};`
			VAR DELETE_CMD = SNOWFLAKE.CREATESTATEMENT( {SQLTEXT: STAGE_DELETE_SQL } );
            RETURN DELETE_CMD;
			//VAR DELETE_RESULT = DELETE_CMD.EXECUTE();	
			
			//SNOWFLAKE HAS A MAXIMUM IN CLAUSE OF ~16,0000 PER SQL STATEMENT.  THE IF CONDITION BELOW PREVENTS EXCEEDING THE MAX LIMIT BY CALLING THE FUNCTION TO INSERT TIMESTAMP
			//DROPPED TO 5,000 SO A COMMIT IS DONE EVERY 15 MINUTES.
            IF (A > 5000 && STAGE_FILE_VALUE != ``)
            {  
				//CALL INSERT FUNCTION TO MARK TIMESTAMP
                INSERT_DELETE_TIMESTAMP(); 
				STAGE_IN_LIST = ""; //RESET THE STAGE FILE LIST
				A = 0;
            }
            STAGE_RESULT.NEXT();
            DATA_SET_VALUE = STAGE_RESULT.GETCOLUMNVALUE(1);
            STAGE_FILE_VALUE = STAGE_RESULT.GETCOLUMNVALUE(2);
            A++;
        } WHILE(STAGE_FILE_VALUE != NULL)
			
		//FINAL DATE INSERT FOR REMAINING RECORDS
		INSERT_DELETE_TIMESTAMP(); 
    }
  }
  CATCH (ERR)  {
    RESULT =  "FAIL, CODE: " + ERR.CODE + "\\R\\N  STATE: " + ERR.STATE;
    RESULT += "\\R\\N  MESSAGE: " + ERR.MESSAGE;
    RESULT += "\\R\\N STACK TRACE:\\R\\N" + ERR.STACKTRACETXT; 
    RETURN(RESULT);
  }

  RESULT = `SUCCESS, STAGE FILES HAVE BEEN REMOVED FROM S3;`
         
  RETURN RESULT
  '
';\nabc'''
        returned_text = scriptloader.search_for_code_block("CREATE function", "\n';", create_function_code_block)
        assert returned_text == create_function_code_block_converted
        # if returned_text != create_function_code_block_converted:
        #    self.fail("Failed: result does not match, should be:\n\n" + create_function_code_block_converted
        #              + "\nbut got:\n" + returned_text)


        double_quote_sp = '''CREATE FUNCTION, CREATE EXTERNAL TABLE"
   + ", CREATE MATERIALIZED VIEW, CREATE PROCEDURE, CREATE TEMPORARY TABLE, CREATE PIPE"
   + " ON" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += sqlTextsemicolon

  sqlText = "GRANT ALL ON ALL TABLES IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL EXTERNAL TABLES IN" + schema + to_rolesemicolon
    if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL MATERIALIZED VIEWS IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL VIEWS IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL STAGES IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL FILE FORMATS IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL STREAMS IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL TASKS IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL SEQUENCES IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL FUNCTIONS IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  sqlText = "GRANT ALL ON ALL PROCEDURES IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
  else
    all_statements += "\\n" + sqlTextsemicolon
  // all on all existing temp tables does not make sense
  //query_text = "\\nGRANT ALL ON ALL TEMPORARY TABLES IN" + schema + to_rolesemicolon

  sqlText = "\\nGRANT ALL ON ALL PIPES IN" + schema + to_rolesemicolon
  if (TO_RUN == true)
  {
    snowflake.createStatement({sqlText: sqlText}).execute()semicolon
    all_statements = "Queries executed successfully"semicolon
  }
  else
    all_statements += sqlTextsemicolon
  return all_statementssemicolon'''
        pass

