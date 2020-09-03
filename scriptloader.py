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


def search_for_procedure_or_function(new_sql_text):
    statement_start_index = -1
    statement_end_index = 0
    new_sql_text2 = ""

    #(? <=...)  Matches if the current position in the string is preceded by a match for ... that ends at the current position.
    m = re.search(r'^create\s+(or\s+replace\s+)?(external\s+)?(function|procedure)\s+(\S+)(\s+)?|^create\s+(external\s+)?(function|procedure)\s+if\s+not\s+exists\s+(\S+)(\s+)?', new_sql_text,
                     flags=re.MULTILINE | re.IGNORECASE)
    if m:
        # get the start position of the statement
        statement_start_index = m.regs[0][0]
        statement_phrase = new_sql_text[m.regs[0][0]: m.regs[0][1]]
    while statement_start_index >= 0:
        if statement_start_index > statement_end_index:
            # save the left side of the start index
            new_sql_text2 += new_sql_text[statement_end_index:statement_start_index]
        statement_start_index += len(statement_phrase)

        m2 = re.search("'\s*;\s*(?=^\s*(create|\Z))", new_sql_text[statement_start_index:],
                       flags=re.MULTILINE | re.IGNORECASE)
        if m2 != None:
            statement_end_index = statement_start_index + m2.regs[0][1]  #this is the end of statement, relative index from the statement_start_index
            statement_close_phrase = new_sql_text[statement_start_index:][m2.regs[0][0]: m2.regs[0][1]]
        else:
            statement_end_index = -1
        # if we find a good end index
        if statement_end_index > 0:

            # replace all the ; in between start_keyword and end_keyword with "semicolon"

            text_block = new_sql_text[statement_start_index:statement_end_index - len(statement_close_phrase)]
            text_block = safeguard_text_block(text_block, True)
            new_sql_text2 += statement_phrase + text_block + statement_close_phrase
            m = re.search(
                r'^create\s+(or\s+replace\s+)?(external\s+)?(function|procedure)\s+(\S+)(\s+)?|^create\s+(external\s+)?(function|procedure)\s+if\s+not\s+exists\s+(\S+)(\s+)?',
                new_sql_text[statement_end_index:],
                flags=re.MULTILINE | re.IGNORECASE)
            if m:
                # get the start position of the statement
                statement_start_index = statement_end_index + m.regs[0][0]
                statement_phrase = new_sql_text[statement_start_index: statement_start_index + m.regs[0][1] - m.regs[0][0]]
            else:
                statement_start_index = -1
        else:
            # print("Incomplete comment block, can't find [%s] before the end of line:" % end_keyword)
            # print(new_sql_text[comment_start_index:])
            text_block = new_sql_text[statement_start_index: ]
            text_block = safeguard_text_block(text_block, True)
            new_sql_text2 += statement_phrase + text_block
            statement_start_index = -1
    if statement_end_index > 0:
        new_sql_text2 += new_sql_text[statement_end_index:]

    return new_sql_text2


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
    new_sql_text = search_for_procedure_or_function(long_sql_text)
    # replacing "/*" or "*/" after the line with a symbol

    # ^create\s * or\s * replace\s * view
    new_sql_text = neutralize_line_after_comments(new_sql_text, "--")
    new_sql_text = search_for_code_block("/*", "*/", new_sql_text)
    new_sql_text = replace_semicolon_in_block(new_sql_text, "'")

    statements = new_sql_text.split(";")
    for i in range(len(statements)):
        statements[i] = statements[i].replace("semicolon", ";").replace("singlequote", "'").replace('CCRREEAATTEE ',
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
    return result


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
        # check if object name has double-quotes - doing it as a separate check to not break existing logic
        m = re.match(r'create\s+(transient\s+)?(database|schema)\s+if\s+not\s+exists\s+(\".+?\")(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if not m: 
            m = re.match(r'create\s+(transient\s+)?(database|schema)\s+if\s+not\s+exists\s+(\S+)(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = str(m.groups()[1]).upper()
            object_name = m.groups()[2]

    else:
        # check if object name has double-quotes - doing it as a separate check to not break existing logic
        m = re.match(r'create\s+(or\s+replace\s+)?(transient\s+)?(database|schema)\s+(\".+?\")(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if not m:
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
            sql_text = 'USE DATABASE ' + current_db_schema["DATABASE"]
            if mode == 'DR_TEST':
                print(sql_text)
            elif mode == 'DR':
                cursor.execute(sql_text)
        elif statement_type == 'CREATE SCHEMA': 
            sql_text = 'USE SCHEMA ' + current_db_schema["SCHEMA"]
            if mode == 'DR_TEST':
                print(sql_text)
            elif mode == 'DR':
                cursor.execute(sql_text)
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
    build_ddl_statements(mode, batch_id, contents, filename, cursor, failed_statements, verbose, option)
    f.close()

