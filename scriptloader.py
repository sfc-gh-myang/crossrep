import os
import re
import sys
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
    root_folder = os.path.join(migHome , "scripts")
    if len(objectType) > 0:
        root_folder = os.path.join(root_folder, objectType)
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
    contents = f.read()
    print("Script file read, size = " + str(len(contents)))
    batch_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # cursor.execute("CALL mei_db_crossrep.mei_tgt_crossrep.parse_sql_script(%s, %s, %s)" % (batch_id, filename, contents))
    build_ddl_statememts(mode, batch_id, contents, filename, cursor, failed_statements)
    f.close()

def search_for_code_block(start_keyword, end_keyword, new_sql_text, check_for_create_after=False):
    comment_end_index = 0
    new_sql_text2 = ""



    comment_start_index = new_sql_text.lower().find(start_keyword.lower(), comment_end_index)
    while comment_start_index >= 0:
        # print('searching for [%s], currently at:[%d]\r' % (start_keyword, comment_start_index))
        # sys.stdout.flush()
        if comment_start_index > comment_end_index:
            # save the left side of the start index
            new_sql_text2 += new_sql_text[comment_end_index:comment_start_index]
        comment_end_index = new_sql_text.lower().find(end_keyword.lower(), comment_start_index + len(start_keyword))
        # check for false ending
        while check_for_create_after == 1 and comment_end_index > 0:
            remaining_text = new_sql_text.lower()[comment_end_index + len(end_keyword)::].lstrip()
            if remaining_text.startswith('create') or len(remaining_text) == 0:
                break;
                # find the next index
            comment_end_index = new_sql_text.lower().find(end_keyword.lower(), comment_end_index + len(end_keyword))

        text_block = new_sql_text[comment_start_index + len(start_keyword): comment_end_index]
        if comment_end_index > 0:
            # replace all the ; in between start_keyword and end_keyword with "semicolon"

            text_block = safeguard_text_block(text_block,
                                              (start_keyword.lower().startswith(
                                                  "create procedure") or start_keyword.lower().startswith(
                                                  "create function")))
            new_sql_text2 += start_keyword + text_block
            new_sql_text2 += end_keyword
            comment_end_index += len(end_keyword)
            comment_start_index = new_sql_text.lower().find(start_keyword.lower(), comment_end_index)

        else:
            print("Incomplete comment block, can't find [%s] before the end of line:", end_keyword)
            print(new_sql_text[comment_start_index:])
            new_sql_text2 += start_keyword + safeguard_text_block(text_block, (start_keyword.lower().startswith(
                                               "create procedure") or start_keyword.lower().startswith(
                                               "create function")))
            comment_start_index = -1

    # no start_keyword is found
    # if comment_end_index == 0:
    #     new_sql_text2 = new_sql_text
    #else:
    # adding the remaining text
    new_sql_text2 += new_sql_text[comment_end_index:]

    return new_sql_text2


def safeguard_text_block(text_block, is_in_javascript):
    create_str = re.compile("create ", re.IGNORECASE)
    text_block = text_block.replace(";", "semicolon")
    text_block = create_str.sub("CCRREEAATTEE ", text_block)
    if (is_in_javascript):
        text_block = neutralize_line_after_comments(text_block, "//")
        text_block = replace_semicolon_in_block(text_block, "`")
        text_block = replace_semicolon_in_block(text_block, '"')
    return text_block


def neutralize_line_after_comments(long_sql_text, comment_indicator):
    stmts = long_sql_text.split("\n")
    stmts2 = []

    for stmt in stmts:
        end_index = stmt.find(comment_indicator)
        if end_index >= 0:
            # neutralize the /* */ that came after the --
            # stmt = stmt[:end_index] + stmt[end_index:].replace("/*", "slash*").replace("*/", "*slash").replace(";", "semicolon")
            stmt = stmt[:end_index] + stmt[end_index:].replace(";", "semicolon").replace("/*", 'slash*').replace("*/",
                                                                                                                 "*slash").replace('CCRREEAATTEE ', 'CREATE ')
        # put it back into the long string
        stmts2.append(stmt)

    if len(stmts2) > 0:
        return '\n'.join(stmts2)
    else:
        return long_sql_text


# limitation: if a line has --  then ; ..  It will be wrongly broken down
# if there is a --, followed by /*, it might lead to the wrong results
def get_statement_blocks(long_sql_text):
    long_sql_text = long_sql_text.replace("if not exists IF NOT EXISTS", "IF NOT EXISTS")
    # replacing "/*" or "*/" after the line with a symbol
    new_sql_text = neutralize_line_after_comments(long_sql_text, "--")

    new_sql_text = search_for_code_block("create procedure", "';\n", new_sql_text, True)
    new_sql_text = search_for_code_block("create function", "';\n", new_sql_text, True)
    new_sql_text = search_for_code_block("/*", "*/", new_sql_text)
    new_sql_text = replace_semicolon_in_block(new_sql_text, "'")

    statements = new_sql_text.split(";")
    for i in range(len(statements)):
        statements[i] = statements[i].replace("semicolon", ";").replace('CCRREEAATTEE ', 'CREATE ')# .replace("slash*", "/*").replace("*slash", "*/"))
    return statements


def replace_semicolon_in_block(new_sql_text, block_keyword):
    single_quote_tokens = new_sql_text.split(block_keyword)
    start_index = 0
    if len(new_sql_text) > 0 and new_sql_text.startswith(block_keyword) == False:
        # all the even number of tokens are in single quotes
        start_index = 1
    for x in range(start_index, len(single_quote_tokens), 2):
        single_quote_tokens[x] = single_quote_tokens[x].replace(';', "semicolon")
    result = block_keyword.join(single_quote_tokens)
    if result != new_sql_text:
        print("replaced semicolon in block %s" % new_sql_text)
    return result


'''
def add_quotes_around_columns(statement):
    object_type = "table"
    statement.lower().find("create ' + object_type + ' if not exists")
    if name_start_index < 0:
        object_type = "view"
        name_start_index = statement.lower().find("create ' + object_type + ' if not exists")
        if name_start_index < 0:
            return statement

    new_statement = statement[:name_start_index]
'''




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
    use_db_keyword = "USE DATABASE "
    use_schema_keyword = "USE SCHEMA "
    sp_keyword = "CREATE PROCEDURE IF NOT EXISTS "
    #view_keyword = "create view if not exists "
    mview_keyword = "CREATE MATERIALIZED VIEW IF NOT EXISTS "
    fn_keyword = "CREATE FUNCTION IF NOT EXISTS "
    file_format_keyword = "CREATE FILE FORMAT IF NOT EXISTS "
    #seq_keyword = "create sequence if not exists "
    create_secure_view_keyword = "CREATE SECURE VIEW "
    grant_keyword = "GRANT "
    create_keyword = "CREATE "
    create_or_replace_keyword = "CREATE OR REPLACE "
    create_or_replace_transient_keyword = "CREATE OR REPLACE TRANSIENT "
    alter_keyword = "ALTER "
    # use_keyword = "use "
    if_not_exists_keyword = " IF NOT EXISTS"

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
                if sql_statements[i].lstrip().upper().startswith(create_keyword):
                    i = i - 1
                    break
                else:
                    statement = statement + sql_statements[i] + ";"
        else:
            # removing all the double spacing
            stmt_wo_double_spacing = statement.upper()
            original_stmt_wo_double_spacing = statement
            while stmt_wo_double_spacing.find("  ") >= 0:
                stmt_wo_double_spacing = stmt_wo_double_spacing.replace("  ", " ")

            if stmt_wo_double_spacing.startswith(use_db_keyword):
                statement_type = use_db_keyword
                cur_database = original_stmt_wo_double_spacing[len(use_db_keyword)::]
            elif stmt_wo_double_spacing.startswith(use_schema_keyword):
                statement_type = use_schema_keyword
                cur_schema = original_stmt_wo_double_spacing[len(use_schema_keyword)::]
            elif stmt_wo_double_spacing.startswith(create_secure_view_keyword):
                statement_type = create_secure_view_keyword
            elif stmt_wo_double_spacing.startswith(create_or_replace_transient_keyword):
                end_index = stmt_wo_double_spacing.find(" ", len(create_or_replace_transient_keyword) + 1)
                if end_index >= 0:
                    remaining_str = original_stmt_wo_double_spacing[len(create_or_replace_transient_keyword):]
                    if remaining_str.startswith('DATABASE '):
                        cur_database = remaining_str[len('DATABASE '):]
                    elif remaining_str.startswith('SCHEMA '):
                        cur_schema = remaining_str[len('SCHEMA '):]
                        statement_type = (create_or_replace_transient_keyword + stmt_wo_double_spacing[len(
                            create_or_replace_transient_keyword):end_index]).upper()

            elif stmt_wo_double_spacing.startswith(create_or_replace_keyword):
                end_index = stmt_wo_double_spacing.find(" ", len(create_or_replace_keyword) + 1)
                if end_index >= 0:
                    statement_type = (create_or_replace_keyword + stmt_wo_double_spacing[len(create_or_replace_keyword):end_index]).upper()
            elif stmt_wo_double_spacing.startswith(grant_keyword):
                end_index = stmt_wo_double_spacing.find(" ", len(grant_keyword) + 1)
                if end_index >= 0:
                    statement_type = (grant_keyword + stmt_wo_double_spacing[len(grant_keyword)+1:end_index]).upper()
            elif stmt_wo_double_spacing.find(create_secure_view_keyword) > 0:
                statement_type = "CREATE SECURE VIEW";
            elif stmt_wo_double_spacing.startswith(create_keyword):
                # find te keywords between "create " and " if not exists"
                end_index = stmt_wo_double_spacing.find(if_not_exists_keyword, len(create_keyword) + 1)
                if end_index >= 0:
                    statement_type = (create_keyword + stmt_wo_double_spacing[len(create_keyword):end_index]).upper()
            elif stmt_wo_double_spacing.startswith(alter_keyword):
                # find te keywords between "create " and " if not exists"
                end_index = stmt_wo_double_spacing.find(" ", 6)
                if end_index >= 0:
                    statement_type = create_keyword + stmt_wo_double_spacing[5:end_index]
            else:
                #'GRANT  READ ON STAGE ABI_WH.HIGH_END_SRCE_S.HE_FLOAD_STAGE TO ROLE ABI_MOBILIZE_BTEQ_DEV'
                print(statement)

        try:
            ''' if len(completed_comment_block) > 0:
                statement = completed_comment_block + statement
                completed_comment_block = ""'''
            # if statement_type.find(" TABLE") >= 0 or statement_type.find(" VIEW") >= 0:
            #    statement = add_quotes_around_columns(statement)
            if mode == 'DR_TEST':
                if statement_type == 'unknown':
                    failed_statements.append(
                        {"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
                         "statement": statement, "file_path": file_path, "error": "Bad Statement"})
                else:
                    print('------------------------ Running Statement (%d of %d) ------------------' %
                          (total_statement + 1, len(sql_statements)))
                    print(statement)
            elif mode == 'DR':
                # do the tables first
                if statement_type.find(" VIEW") > 0 or statement_type.find(" PIPE") > 0 or statement_type.find(" TASK") > 0:
                    failed_statements.append(
                        {"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
                         "statement": statement, "file_path": file_path, "success": False})
                else:
                    print('------------------------ Running Statement (%d of %d) ------------------' %
                          (total_statement + 1, len(sql_statements)))
                    print(statement)
                    cursor.execute(statement)
        except snowflake.connector.errors.ProgrammingError as e:
            failed_statements.append({"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
                              "statement": statement, "file_path": file_path, "success": False})
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

    retry_sequence = ['SEQUENCE', 'FILE FORMAT', 'TABLE', 'VIEW', 'STAGE', 'PIPE', 'PROCEDURE', 'FUNCTION', 'STREAM', 'TASK', '']
    while len(retry_list) > 0:
        loop_no += 1
        print("Retry failed statement loop #" + str(loop_no))
        for selected_type in retry_sequence:
            for item in retry_list:
                if "cur_database" not in item.keys() or "cur_schema" not in item.keys() or "statement" not in item.keys() or "statement_type" not in item.keys():
                    # ignore the bad items
                    continue
                statement_type = item["statement_type"].upper()

                if (selected_type == '' or statement_type.find(selected_type) > 0) and not item["success"]:
                    item["current_loop"] = loop_no
                    try:

                        if item["cur_database"] != cur_database:
                            cur_database = item["cur_database"]
                            cursor.execute('USE DATABASE "' + cur_database + '"')
                            # reset old schema so that it will execute USE SCHEMA
                            cur_schema = ""
                        if item["cur_schema"] != cur_schema:
                            cur_schema = item["cur_schema"]
                            cursor.execute('USE SCHEMA ' + cur_schema)

                        print("-----Retry statement --->\n" + item["statement"])
                        cursor.execute(item["statement"])
                        print("-----Retry succeeded -!")
                        item["success"] = True
                    except snowflake.connector.errors.ProgrammingError as e:
                        item["error"] = str(e)
                        if item not in next_retry:
                            next_retry.append(item)
                        pass
        if len(retry_list) == len(next_retry):
            # this means no more statement can be run successfully
            break
        else:
            retry_list = next_retry
            next_retry = []
    return retry_list
