import os
from datetime import datetime
import snowflake.connector
import glob
from os import listdir
from os.path import isfile, join
import fnmatch

def list_scripts(migHome, objectType = ''):
    list = []
    '''rootfolder = migHome + "scripts/"
    for folder in listdir(rootfolder):
        print(rootfolder + folder)
        if not (isfile(rootfolder + folder)) :
            for subfolder in listdir(rootfolder + folder):
                subfolder_path = rootfolder + folder + "/" + subfolder + "/";
                if not (isfile(subfolder_path)):
                    for file in listdir(subfolder_path):
                        fullpath = subfolder_path + file
                        if isfile(fullpath) and fnmatch.fnmatch(fullpath, '*.sql'):
                            list.append(fullpath)
    '''
    root_folder = migHome + "scripts/"
    if len(objectType) > 0:
        root_folder += objectType + "/"
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".sql") and file.startswith("01_dbDDL_"):
                # sort_on = "key2"
                dict = {}
                dict['file'] = file
                dict['path'] = root
                list.append(dict)
    return sorted(list, key = lambda i: i['file'])


def upload_scripts(mode, filedict, cursor, failed_statements):
    # open file
    filename = os.path.join(filedict['path'], filedict['file'])
    print("--------------------------------------------------")
    print("Opening script file: " + filename )
    f = open(filename, "r")
    if f.mode == "r":
        contents = f.read()
        print("Script file read, size = " + str(len(contents)))
        batch_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # cursor.execute("CALL mei_db_crossrep.mei_tgt_crossrep.parse_sql_script(%s, %s, %s)", (batch_id, filename, contents))
        build_ddl_statememts(mode, batch_id, contents, filename, cursor, failed_statements)
        f.close()

def replace_semicolon_in_block(start_keyword, end_keyword, new_sql_text):
    comment_end_index = 0
    new_sql_text2 = ""
    comment_start_index = new_sql_text.lower().find(start_keyword.lower(), comment_end_index)
    while comment_start_index >= 0:
        if comment_start_index > comment_end_index:
            # save the left side of the start index
            new_sql_text2 += new_sql_text[comment_end_index:comment_start_index]
        comment_end_index = new_sql_text.lower().find(end_keyword.lower(), comment_start_index + len(start_keyword))
        if comment_end_index > 0:
            # replace all the ; in between start_keyword and end_keyword with "semicolon"
            new_sql_text2 += new_sql_text[comment_start_index: comment_end_index].replace(";", "semicolon")
            new_sql_text2 += end_keyword
            comment_end_index += len(end_keyword)
            comment_start_index = new_sql_text.lower().find(start_keyword.lower(), comment_end_index)
        else:
            print("Incomplete comment block, can't find */ before the end of line:")
            print(new_sql_text[comment_start_index:])
            comment_start_index = -1

    # no start_keyword is found
    # if comment_end_index == 0:
    #     new_sql_text2 = new_sql_text
    #else:
    # adding the remaining text
    new_sql_text2 += new_sql_text[comment_end_index:]

    return new_sql_text2

# limitation: if a line has --  then ; ..  It will be wrongly broken down
# if there is a --, followed by /*, it might lead to the wrong results
def get_statement_blocks(long_sql_text):
    long_sql_text = long_sql_text.replace("if not exists IF NOT EXISTS", "IF NOT EXISTS")
    # replacing "/*" or "*/" after the line with a symbol
    stmts = long_sql_text.split("\n")
    stmts2 = []

    for stmt in stmts:
        end_index = stmt.find("--")
        if end_index >= 0:
            # neutralize the /* */ that came after the --
            # stmt = stmt[:end_index] + stmt[end_index:].replace("/*", "slash*").replace("*/", "*slash").replace(";", "semicolon")
            stmt = stmt[:end_index] + stmt[end_index:].replace(";", "semicolon")
        # put it back into the long string
        stmts2.append(stmt)

    new_sql_text = '\n'.join(stmts2)

    new_sql_text = replace_semicolon_in_block("/*", "*/", new_sql_text)
    new_sql_text = replace_semicolon_in_block("create procedure", "';\n", new_sql_text)
    new_sql_text = replace_semicolon_in_block("create function", "';\n", new_sql_text)

    statements = new_sql_text.split(";")
    for i in range(len(statements)):
        statements[i] = statements[i].replace("semicolon", ";")# .replace("slash*", "/*").replace("*slash", "*/"))
    return statements


def build_ddl_statememts(mode, batch_id, long_sql_text, file_path, cursor, failed_statements):
    #spliting the file by ';'

    sql_statements = get_statement_blocks(long_sql_text);

    retry_list = []
    cur_database = os.path.basename(file_path).replace('01_dbDDL_', '').replace('.sql', '')
    cur_schema = ""
    old_database = ""
    old_schema = ""
    # table_keyword = "create TABLE if not exists "
    # schema_keyword = "create schema if not exists "
    # db_keyword = "create database if not exists "
    use_db_keyword = "use database "
    use_schema_keyword = "use schema "
    sp_keyword = "create PROCEDURE if not exists "
    #view_keyword = "create view if not exists "
    mview_keyword = "create materialized view if not exists "
    fn_keyword = "create FUNCTION if not exists "
    file_format_keyword = "create FILE FORMAT if not exists "
    #seq_keyword = "create sequence if not exists "
    create_secure_view_keyword = "CREATE SECURE VIEW "
    grant_keyword = "grant "
    create_keyword = "create "
    create_or_replace_keyword = "create or replace "
    create_or_replace_transient_keyword = "create or replace transient "
    alter_keyword = "alter "
    # use_keyword = "use "
    if_not_exists_keyword = " if not exists"

    total_statement = 0
    i = 0
    partial_commment_block = ""
    completed_comment_block = ""
    while i < len(sql_statements) - 1:
        statement = sql_statements[i].lstrip()
        if len(statement) == 0:
            i = i + 1
            continue

        statement_type = 'unknown'

        if statement.startswith(sp_keyword) or statement.startswith(fn_keyword):
            if statement.startswith(sp_keyword):
                statement_type = sp_keyword.upper()
            else:
                statement_type = fn_keyword.upper()
            statement = statement + "\n"
            while i + 1 < len(sql_statements):
                i = i + 1
                if sql_statements[i].lstrip().lower().startswith(create_keyword):
                    i = i - 1
                    break
                else:
                    statement = statement + sql_statements[i] + ";"
        else:
            # removing double spacing
            statement = statement.replace("  ", " ")

            if statement.lower().startswith(use_db_keyword):
                statement_type = use_db_keyword.upper()
                cur_database = statement[len(use_db_keyword)::]
            elif statement.lower().startswith(use_schema_keyword):
                statement_type = use_schema_keyword.upper()
                cur_schema = statement[len(use_schema_keyword)::]
            elif statement.upper().startswith(create_secure_view_keyword):
                statement_type = create_secure_view_keyword
            elif statement.lower().startswith(create_or_replace_transient_keyword):
                end_index = statement.lower().find(" ", len(create_or_replace_transient_keyword) + 1)
                if end_index >= 0:
                    remaining_str = statement[len(create_or_replace_transient_keyword):]
                    if remaining_str.upper().startswith('DATABASE '):
                        cur_database = remaining_str[len('DATABASE '):]
                    elif remaining_str.upper().startswith('SCHEMA '):
                        cur_schema = remaining_str[len('SCHEMA '):]
                    statement_type = (create_or_replace_transient_keyword + statement[len(create_or_replace_transient_keyword):end_index]).upper()

            elif statement.lower().startswith(create_or_replace_keyword):
                end_index = statement.lower().find(" ", len(create_or_replace_keyword) + 1)
                if end_index >= 0:
                    statement_type = (create_or_replace_keyword + statement[len(create_or_replace_keyword):end_index]).upper()
            elif statement.lower().startswith(grant_keyword):
                end_index = statement.lower().find(" ", len(grant_keyword) + 1)
                if end_index >= 0:
                    statement_type = (grant_keyword + statement[len(grant_keyword)+1:end_index]).upper()
            elif statement.lower().find(create_secure_view_keyword) > 0:
                statement_type = "CREATE SECURE VIEW";
            elif statement.lower().startswith(create_keyword):
                # find te keywords between "create " and " if not exists"
                end_index = statement.lower().find(if_not_exists_keyword, len(create_keyword) + 1)
                if end_index >= 0:
                    statement_type = (create_keyword + statement[len(create_keyword):end_index]).upper()
            elif statement.lower().startswith(alter_keyword):
                # find te keywords between "create " and " if not exists"
                end_index = statement.lower().find(" ", 6)
                if end_index >= 0:
                    statement_type = (create_keyword + statement[5:end_index]).upper()
            else:
                #'GRANT  READ ON STAGE ABI_WH.HIGH_END_SRCE_S.HE_FLOAD_STAGE TO ROLE ABI_MOBILIZE_BTEQ_DEV'
                print(statement)

        try:
            if len(completed_comment_block) > 0:
                statement = completed_comment_block + statement
                completed_comment_block = ""
            if mode == 'DR_TEST':
                if statement_type == 'unknown':
                    failed_statements.append(
                        {"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
                         "statement": statement, "file_path": file_path, "error": "Bad Statement"})
                else:
                    print("------------------------ Statement found (" + str(total_statement+1) + ") ----------------------------")
                    print(statement)
            elif mode == 'DR':
                # if cur_database != old_database:
                #    cursor.execute("USE DATABASE " + cur_database)
                #    # reset old schema so that it will execute USE SCHEMA
                #    old_database = cur_database
                #    old_schema = ''
                #if cur_schema != old_schema:
                #    cursor.execute("USE SCHEMA " + cur_schema)
                #    old_schema = cur_schema

                print("------------------------ Running Statement (" + str(total_statement+1) + ") ----------------------------")
                print(statement)
                cursor.execute(statement)
        except snowflake.connector.errors.ProgrammingError as e:
            failed_statements.append({"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
                              "statement": statement, "file_path": file_path})
            pass
        # stmt = snowflake.createStatement(
        # {
        #    sqlText: "INSERT INTO source_scripts (BATCH_ID,SCRIPT_FILE_NAME_PATH, SQL_TEXT, STATEMENT_TYPE, USE_DATABASE_NAME, USE_SCHEMA_NAME) VALUES (?, ?, ?, ?, ?, ?);",
        #    binds: [BATCH_ID, FILE_PATH, statement, statement_type, cur_database, cur_schema]
        # }
        # result_set1 = stmt.execute();
        total_statement = total_statement + 1
        i = i + 1


def retry_failed_statements(cursor, failed_statements):
    next_retry = []
    retry_list = failed_statements
    cur_schema = ''
    cur_database = ''
    loop_no = 1
    while len(retry_list) > 0:
        print("Retry failed statement loop #" + str(loop_no))
        loop_no += 1
        for item in retry_list:
            if item["cur_database"] != cur_database:
                cur_database = item["cur_database"]
                cursor.execute('USE DATABASE "' + cur_database + '"')
                # reset old schema so that it will execute USE SCHEMA
                cur_schema = ""
            if item["cur_schema"] != cur_schema:
                cur_schema = item["cur_schema"]
                cursor.execute('USE SCHEMA "' + cur_schema + '"')
            try:
                print("Retry statement --->" + item["statement"])
                cursor.execute(item["statement"])
                print("Retry succeeded -!")
            except snowflake.connector.errors.ProgrammingError as e:
                item["error"] = str(e)
                next_retry.append(item)
                pass
        if len(retry_list) == len(next_retry):
            # this means no more statement can be run successfully
            return next_retry
        else:
            retry_list = next_retry
            next_retry = []
    return next_retry
