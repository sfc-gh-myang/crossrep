import os
import re
import sys
from datetime import datetime
import snowflake.connector
import glob
from os import listdir
from os.path import isfile, join
import fnmatch

USE_CREATE_IF_NOT_EXISTS = 1
USE_CREATE_OR_REPLACE = 2

def list_scripts(migHome, objectType=''):
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
    root_folder = os.path.join(migHome, "scripts")
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
    return sorted(list, key=lambda i: i['file'])



def search_for_code_block(start_keyword, end_keyword, new_sql_text, check_for_create_after=False):
    comment_end_index = 0
    new_sql_text2 = ""
    sp_or_func = 0
    if start_keyword.lower().startswith(
            "create procedure") or start_keyword.lower().startswith(
        "create function") or start_keyword.lower().startswith(
        "create or replace procedure") or start_keyword.lower().startswith("create or replace function"):
        sp_or_func = 1

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
            remaining_text = new_sql_text.lower()[comment_end_index + len(end_keyword)::]
            if remaining_text.lstrip().startswith('create') or len(remaining_text) == 0:
                break;
                # find the next index
            comment_end_index = new_sql_text.lower().find(end_keyword.lower(), comment_end_index + len(end_keyword))

        if comment_end_index > 0:
            text_block = new_sql_text[comment_start_index + len(start_keyword): comment_end_index]
            # replace all the ; in between start_keyword and end_keyword with "semicolon"
            text_block = safeguard_text_block(text_block,
                                              sp_or_func)
            new_sql_text2 += start_keyword + text_block
            if sp_or_func == 1:
                new_sql_text2 += '\n'
            new_sql_text2 += end_keyword
            comment_end_index += len(end_keyword)
            comment_start_index = new_sql_text.lower().find(start_keyword.lower(), comment_end_index)

        else:
            # print("Incomplete comment block, can't find [%s] before the end of line:" % end_keyword)
            # print(new_sql_text[comment_start_index:])
            text_block = new_sql_text[comment_start_index + len(start_keyword):]
            new_sql_text2 += start_keyword + safeguard_text_block(text_block[:-1], sp_or_func)
            comment_start_index = -1

    # no start_keyword is found
    # if comment_end_index == 0:
    #     new_sql_text2 = new_sql_text
    # else:
    # adding the remaining text
    new_sql_text2 += new_sql_text[comment_end_index:]

    return new_sql_text2


def safeguard_text_block(text_block, is_in_javascript):
    if (is_in_javascript):
        text_block = neutralize_line_after_comments(text_block, "//")
        text_block = replace_semicolon_in_block(text_block, "`")
        text_block = replace_semicolon_in_block(text_block, '"')
        text_block = replace_semicolon_in_block(text_block, "''")
    else:
        text_block = replace_semicolon_in_block(text_block, "$$")
    text_block = text_block.replace(";", "semicolon")
    create_str = re.compile("create ", re.IGNORECASE)
    text_block = create_str.sub("CCRREEAATTEE ", text_block)
    return text_block


def neutralize_line_after_comments(long_sql_text, comment_indicator):
    stmts = long_sql_text.split("\n")
    stmts2 = []
    create_str = re.compile("create ", re.IGNORECASE)
    for stmt in stmts:
        end_index = stmt.find(comment_indicator)
        if end_index >= 0:
            # neutralize the /* */ that came after the --
            # stmt = stmt[:end_index] + stmt[end_index:].replace("/*", "slash*").replace("*/", "*slash").replace(";", "semicolon")
            text_block = stmt[end_index:].replace(";", "semicolon").replace("/*", 'slash*').replace("*/",
                                                                                                    "*slash").replace(
                "'", "singlequote")
            text_block = create_str.sub("CCRREEAATTEE ", text_block)
            stmt = stmt[:end_index] + text_block
        # put it back into the long string
        stmts2.append(stmt)

    if len(stmts2) > 0:
        return '\n'.join(stmts2)
    else:
        return long_sql_text


def use_create_object_if_not_exists(ddl):
    if ddl.lower().find("replace") >= 0:
        ddl2 = re.sub(r'create\s+(or\s+replace\s+)?(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|temporary table|stage|pipe|stream|task|masking policy)\s+'
                     , r'create \2 if not exists ', ddl, flags=re.MULTILINE | re.IGNORECASE)
        if ddl2 == ddl:
            ddl2 = re.sub(r'create\s+(or\s+replace\s+)?(transient\s+)?(database|schema)+\s+(\S+)(\s+)?'
                 # , r'create \2\3 if not exists \4;\nuse \3 \4', ddl, flags=re.MULTILINE | re.IGNORECASE)
                 , r'create \2\3 if not exists \4', ddl, flags=re.MULTILINE | re.IGNORECASE)
    else:
        ddl2 = ddl
    return ddl2


def use_create_or_replace_object(ddl):
    if ddl.lower().find("exists") >= 0:
        ddl2 = re.sub(r'create\s+(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|temporary table|stage|pipe|stream|task|masking policy)\s+if\s+not\s+exists\s+'
                     , r'create or replace \1 ', ddl, flags=re.MULTILINE | re.IGNORECASE)
        if ddl2 == ddl:
            ddl2 = re.sub(r'create\s+(transient\s+)?(database|schema)\s+if\s+not\s+exists\s+(\S+)(\s+)?'
                 # , r'create or replace \1\2 \3;\nuse \2 \3', ddl, flags = re.MULTILINE | re.IGNORECASE)
                 , r'create or replace \1\2 \3', ddl, flags=re.MULTILINE | re.IGNORECASE)
    else:
        ddl2 = ddl
    return ddl2


# limitation: if a line has --  then ; ..  It will be wrongly broken down
# if there is a --, followed by /*, it might lead to the wrong results
def get_statement_blocks(long_sql_text):
    long_sql_text = long_sql_text.replace("if not exists IF NOT EXISTS", "IF NOT EXISTS")

    new_sql_text = search_for_code_block("create procedure", "';\n", long_sql_text, True)
    new_sql_text = search_for_code_block("create or replace procedure", "';\n", new_sql_text, True)

    new_sql_text = search_for_code_block("create function", "';\n", new_sql_text, True)
    new_sql_text = search_for_code_block("create or replace function", "';\n", new_sql_text, True)

    # replacing "/*" or "*/" after the line with a symbol

    # ^create\s * or\s * replace\s * view
    new_sql_text = neutralize_line_after_comments(new_sql_text, "--")
    new_sql_text = search_for_code_block("/*", "*/", new_sql_text)
    new_sql_text = replace_semicolon_in_block(new_sql_text, "'")

    statements = new_sql_text.split(";")
    for i in range(len(statements)):
        statement = statements[i].replace("semicolon", ";").replace("singlequote", "'").replace('CCRREEAATTEE ',
                                                                                                'CREATE ').replace(
            "slash*", "/*").replace("*slash", "*/")
        # create <object> if not exists
    return statements


def replace_semicolon_in_block(new_sql_text, block_keyword):
    splited_tokens = new_sql_text.split(block_keyword)
    start_index = 0
    if len(new_sql_text) > 0 and new_sql_text.startswith(block_keyword) == False:
        # all the even number of tokens are in single quotes
        start_index = 1
    for x in range(start_index, len(splited_tokens), 2):
        splited_tokens[x] = splited_tokens[x].replace(';', "semicolon")
    result = block_keyword.join(splited_tokens)
    # if result != new_sql_text:
    #    print("replaced semicolon in block %s" % new_sql_text)
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

def try_handle_statement(mode
                         , statement
                         , statement_type
                         , cur_database
                         , cur_schema
                         , verbose
                         , failed_statements
                         , file_path
                         , total_statement_count
                         , running_total
                         , cursor):

    try:
        if mode == 'DR_TEST':
            if statement_type == '':
                failed_statements.append(
                    {"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
                     "statement": statement, "file_path": file_path, "error": "Bad Statement"})
            else:
                print('------------------------ Running Statement (%d of %d) ------------------' %
                      (running_total + 1, total_statement_count))
                print(statement)
        elif mode == 'DR':
            # do the tables first
            if len(statement_type) == 0 or statement_type.find(" VIEW") > 0 or statement_type.find(" PIPE") > 0 or statement_type.find(
                    " TASK") > 0:
                failed_statements.append(
                    {"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
                     "statement": statement, "file_path": file_path, "success": False})
            else:
                if verbose:
                    print('------------------------ Running Statement (%d of %d) ------------------' %
                          (running_total + 1, total_statement_count))
                    print(statement)
                cursor.execute(statement)
    except snowflake.connector.errors.ProgrammingError as e:
        failed_statements.append(
            {"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
             "statement": statement, "file_path": file_path, "success": False})
        pass




def match_use_statements(statement, current_db_schema):
    statement_type = ""
    m = re.match(r"use\s+(database|schema)+\s+(\S+)\s*", statement, flags=re.MULTILINE | re.IGNORECASE)
    # ms = re.match(r"use\s+(transient\s+)?schema\s+(\S+)", statement, flags=re.MULTILINE | re.IGNORECASE)
    if m:
        statement_type = str(m.groups()[0]).upper()
        current_db_schema[statement_type] == m.groups()[1]
        statement_type = "USE " + statement_type
    return statement_type

def match_create_db_or_schema(statement, option, current_db_schema):
    statement_type = ''
    object_name = ''
    if option == USE_CREATE_IF_NOT_EXISTS:
        m = re.match(r'create\s+(transient\s+)?(database|schema)\s+if\s+not\s+exists\s+(\S+)(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = str(m.groups()[1]).upper()
            object_name = m.groups()[2]

    else:
        m = re.match(r'create\s+(or\s+replace\s+)?(transient\s+)?(database|schema)\s+(\S+)(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = m.groups()[2].upper()
            object_name = m.groups()[3]
    if object_name != '':
        current_db_schema[statement_type] = object_name
        statement_type = "CREATE " + statement_type
    return statement_type

def match_create_db_object(statement, option):
    statement_type = ''
    object_name = ''
    if option == USE_CREATE_IF_NOT_EXISTS:
        m = re.match(
            r'create\s+(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|temporary table|stage|pipe|stream|task|masking policy)(\s+if\s+not\s+exists\s+)?'
            , statement
            ,flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = str(m.groups()[0]).upper()
    else:
        m = re.match(
            r'create\s+(or\s+replace\s+)?(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|temporary table|stage|pipe|stream|task|masking policy)\s+'
            , statement
            , flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = str(m.groups()[1]).upper()

    if statement_type != '':
        statement_type = "CREATE " + statement_type
    return statement_type


def build_ddl_statements(mode, batch_id, long_sql_text, file_path, cursor, failed_statements, verbose=False, option=USE_CREATE_IF_NOT_EXISTS):

    sql_statements = get_statement_blocks(long_sql_text)
    current_db_schema = {}
    current_db_schema["SCHEMA"] = ""
    current_db_schema["DATABASE"] = os.path.basename(file_path).replace('01_dbDDL_', '').replace('.sql', '')

    total_statement = 0
    i = 0
    partial_commment_block = ""
    completed_comment_block = ""
    while i < len(sql_statements) - 1:
        statement = sql_statements[i].lstrip()

        # Convert everything to use the same convention
        if option == USE_CREATE_OR_REPLACE:
            statement = use_create_or_replace_object(statement)
        else:
            # create or replace <object>
            statement = use_create_object_if_not_exists(statement)
        if len(statement) == 0:
            i = i + 1
            continue

        statement_type = match_use_statements(statement, current_db_schema)
        if len(statement_type) == 0:
            statement_type = match_create_db_or_schema(statement, option, current_db_schema)
        if len(statement_type) == 0:
            statement_type = match_create_db_object(statement, option)
        if len(statement_type) == 0:
            if verbose:
                print("****  Cannot parse statememt, will run as is:")
                print(statement)

        if len(statement_type) == 0 and verbose:
            print("---- Statement not recognized ---- ")
        try_handle_statement(mode, statement
                         , statement_type
                         , current_db_schema["DATABASE"]
                         , current_db_schema["SCHEMA"]
                         , verbose
                         , failed_statements
                         , file_path
                         , len(sql_statements)
                         , total_statement
                         , cursor)
        if statement_type == 'CREATE DATABASE':
            sql_text = "USE DATABASE " + current_db_schema["DATABASE"]
            if mode == 'DR_TEST':
                print(sql_text)
            elif mode == 'DR':
                cursor.execute(sql_text)
        elif statement_type == 'CREATE SCHEMA':
            sql_text = "USE SCHEMA " + current_db_schema["SCHEMA"]
            if mode == 'DR_TEST':
                print(sql_text)
            elif mode == 'DR':
                cursor.execute(sql_text)
        total_statement = total_statement + 1
        i = i + 1


def build_ddl_statememts_old(mode, batch_id, long_sql_text, file_path, cursor, failed_statements, verbose=False):
    # spliting the file by ';'

    sql_statements = get_statement_blocks(long_sql_text)

    retry_list = []
    cur_database = os.path.basename(file_path).replace('01_dbDDL_', '').replace('.sql', '')
    cur_schema = ""
    old_database = ""
    old_schema = ""
    # table_keyword = "create TABLE if not exists "
    schema_keyword = "create schema if not exists "
    # db_keyword = "create database if not exists "
    use_db_keyword = "USE DATABASE "
    use_schema_keyword = "USE SCHEMA "
    sp_keyword = "CREATE PROCEDURE IF NOT EXISTS "
    # view_keyword = "create view if not exists "
    mview_keyword = "CREATE MATERIALIZED VIEW IF NOT EXISTS "
    fn_keyword = "CREATE FUNCTION IF NOT EXISTS "
    file_format_keyword = "CREATE FILE FORMAT IF NOT EXISTS "
    # seq_keyword = "create sequence if not exists "
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
            stmt_upper = statement.upper()
            stmt_original = statement
            while stmt_upper.find("  ") >= 0:
                stmt_upper = stmt_upper.replace("  ", " ")

            if stmt_upper.startswith(use_db_keyword):
                statement_type = use_db_keyword
                cur_database = stmt_original[len(use_db_keyword)::]
            elif stmt_upper.startswith(use_schema_keyword):
                statement_type = use_schema_keyword
                cur_schema = stmt_original[len(use_schema_keyword)::]
            #elif stmt_upper.startswith(schema_keyword):
            #    statement_type = schema_keyword
            #    cur_schema = stmt_original[len(schema_keyword)::]
            elif stmt_upper.startswith(create_secure_view_keyword):
                statement_type = create_secure_view_keyword
            elif stmt_upper.startswith(create_or_replace_transient_keyword):
                end_index = stmt_upper.find(" ", len(create_or_replace_transient_keyword) + 1)
                if end_index >= 0:
                    remaining_str = stmt_original[len(create_or_replace_transient_keyword):]
                    if remaining_str.startswith('DATABASE '):
                        cur_database = remaining_str[len('DATABASE '):]
                    elif remaining_str.startswith('SCHEMA '):
                        cur_schema = remaining_str[len('SCHEMA '):]
                        statement_type = (create_or_replace_transient_keyword + stmt_upper[len(
                            create_or_replace_transient_keyword):end_index]).upper()

            elif stmt_upper.startswith(create_or_replace_keyword):
                end_index = stmt_upper.find(" ", len(create_or_replace_keyword) + 1)
                if end_index >= 0:
                    statement_type = (create_or_replace_keyword + stmt_upper[
                                                                  len(create_or_replace_keyword):end_index]).upper()
            elif stmt_upper.startswith(grant_keyword):
                end_index = stmt_upper.find(" ", len(grant_keyword) + 1)
                if end_index >= 0:
                    statement_type = (grant_keyword + stmt_upper[len(grant_keyword) + 1:end_index]).upper()
            elif stmt_upper.find(create_secure_view_keyword) > 0:
                statement_type = "CREATE SECURE VIEW";
            elif stmt_upper.startswith(create_keyword):
                # find te keywords between "create " and " if not exists"
                end_index = stmt_upper.find(if_not_exists_keyword, len(create_keyword) + 1)
                if end_index >= 0:
                    statement_type = (create_keyword + stmt_upper[len(create_keyword):end_index]).upper()
            elif stmt_upper.startswith(alter_keyword):
                # find te keywords between "create " and " if not exists"
                end_index = stmt_upper.find(" ", 6)
                if end_index >= 0:
                    statement_type = create_keyword + stmt_upper[5:end_index]
            else:
                # 'GRANT  READ ON STAGE ABI_WH.HIGH_END_SRCE_S.HE_FLOAD_STAGE TO ROLE ABI_MOBILIZE_BTEQ_DEV'
                if verbose:
                    print(statement)

        try:
            ''' if len(completed_comment_block) > 0:
                statement = completed_comment_block + statement
                completed_comment_block = ""'''
            # if statement_type.find(" TABLE") >= 0 or statement_type.find(" VIEW") >= 0:
            #    statement = add_quotes_around_columns(statement)
            if mode == 'DR_TEST':
                    print('------------------------ Running Statement (%d of %d) ------------------' %
                          (total_statement + 1, len(sql_statements)))
                    print(statement)
            elif mode == 'DR':
                # do the tables first
                if statement_type.find(" VIEW") > 0 or statement_type.find(" PIPE") > 0 or statement_type.find(
                        " TASK") > 0:
                    failed_statements.append(
                        {"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
                         "statement": statement, "file_path": file_path, "success": False})
                else:
                    if verbose:
                        print('------------------------ Running Statement (%d of %d) ------------------' %
                              (total_statement + 1, len(sql_statements)))
                        print(statement)
                    cursor.execute(statement)
        except snowflake.connector.errors.ProgrammingError as e:
            failed_statements.append(
                {"statement_type": statement_type, "cur_database": cur_database, "cur_schema": cur_schema,
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


def retry_failed_statements(cursor, failed_statements, verbose=False, mode='DR'):
    next_retry = []
    retry_list = failed_statements
    cur_schema = ''
    cur_database = ''
    loop_no = 1

    retry_sequence = ['SEQUENCE', 'FILE FORMAT', 'TABLE', 'VIEW', 'STAGE', 'PIPE', 'PROCEDURE', 'FUNCTION', 'STREAM',
                      'TASK', '']
    while len(retry_list) > 0:
        loop_no += 1
        if verbose or mode == 'DR_TEST':
            print("Retry statement loop #" + str(loop_no))
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
                        if verbose:
                            print("-----Retry statement --->\n" + item["statement"])
                        else:
                            print("-- retry: %s ..." % item["statement"][:60])
                        cursor.execute(item["statement"])
                        if verbose:
                            print("----- succeeded -!")
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


def upload_scripts(mode, filedict, cursor, failed_statements, verbose, option=USE_CREATE_IF_NOT_EXISTS):
    # open file
    filename = os.path.join(filedict['path'], filedict['file'])
    print("--------------------------------------------------")
    print("Opening script file: " + filename)
    f = open(filename, "r")
    contents = f.read()
    print("Script file read, size = " + str(len(contents)))
    batch_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # cursor.execute("CALL mei_db_crossrep.mei_tgt_crossrep.parse_sql_script(%s, %s, %s)" % (batch_id, filename, contents))
    build_ddl_statements(mode, batch_id, contents, filename, cursor, failed_statements, verbose, USE_CREATE_IF_NOT_EXISTS)
    f.close()
