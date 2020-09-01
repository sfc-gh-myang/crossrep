from unittest import TestCase
import scriptloader
import os
import re


def format_ddl_with_create_if_not_exists(ddl):
    '''
    CREATE SCHEMA , CREATE SCHEMA
    CREATE TABLE , CREATE TABLE
    CREATE EXTERNAL TABLE
    CREATE VIEW
    CREATE MATERIALIZED VIEW
    CREATE MASKING POLICY
    CREATE SEQUENCE
    CREATE FILE FORMAT
    CREATE STAGE
    CREATE PIPE
    CREATE STREAM
    CREATE TASK
    CREATE FUNCTION
    CREATE EXTERNAL FUNCTION
    CREATE PROCEDURE
    '''
    ddl = re.sub(r'create\s+(or\s+replace\s+)?(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|stage|pipe|stream|task|masking policy)\s+'
                 , r'create \2 if not exists ', ddl, flags=re.MULTILINE | re.IGNORECASE)
    # ddl = re.sub(r'create\s+(or\s+replace)\s+(transient database|transient schema|database|schema)\s+(\S+)',
    #             r'create \2 if not exists \3\nuse \2 \3', ddl, flags=re.MULTILINE | re.IGNORECASE)

    #ddl = re.sub(r'create\s+(or\s+replace\s+)?(database|schema)\s+(\S+)(\s+)?'
    #             , r'create \2 if not exists \3;\nuse \2 \3', ddl, flags=re.MULTILINE | re.IGNORECASE)
    ddl = re.sub(r'create\s+(or\s+replace\s+)?(transient\s+)?(database|schema)\s+(\S+)(\s+)?'
                 , r'create \2\3 if not exists \4;\nuse \3 \4', ddl, flags=re.MULTILINE | re.IGNORECASE)
    return ddl
    #
    # create VIEW if not exists S2.VIEW1 COPY GRANTS AS SELECT * FROM S1.TAB1;
    # remove COPY GRANTS on create view - not needed for newly created objects/grants
    # ddl=re.sub(r'create\s+(view if not exists)\s+(\S)+\s+(COPY GRANTS)\s+',r'create view if not exists \2 ',ddl,flags=re.MULTILINE|re.IGNORECASE)

def format_ddl_with_create_or_replace(ddl):
    ddl = re.sub(r'create\s+(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|stage|pipe|stream|task|masking policy)\s+if\s+not\s+exists\s+'
                 , r'create or replace \1 ', ddl, flags=re.MULTILINE | re.IGNORECASE)
    # ddl = re.sub(r'create\s+(database|schema)\s+if\s+not\s+exists\s+(\S+)(\s+)?;'

    ddl = re.sub(r'create\s+(transient\s+)?(database|schema)\s+if\s+not\s+exists\s+(\S+)(\s+)?'
                 , r'create or replace \1\2 \3;\nuse \2 \3', ddl, flags = re.MULTILINE | re.IGNORECASE)
    # ddl = re.sub(r'create\s+(database|schema)\s+if\s+not\s+exists\s+(\S+)(\s+)?'
    #             , r'create or replace \1 \2;\nuse \1 \2', ddl, flags=re.MULTILINE | re.IGNORECASE)
    return ddl


class TestGet_statement_blocks(TestCase):
    def test_get_statement_blocks(self):
        '''
        ddl = re.sub(r'create\s+(or\s+replace\s+)?schema\s+(\S+)\s+(with\s+managed\s+access)',
                     r'create schema if not exists \2 with managed access;\n use \2 \3',
                     'CREATE OR replace schema abc  \n with managed access',
                     flags=re.MULTILINE | re.IGNORECASE)


        ddl = format_ddl_with_create_if_not_exists(r'\ncreate or  repLACE external table    "aBc" \n AS something')
        sql = format_ddl_with_create_or_replace(ddl)

        ddl = format_ddl_with_create_if_not_exists(r'\ncreate or  repLACE external function abc \n AS something')
        sql = format_ddl_with_create_or_replace(ddl)
        assert(sql == '\\ncreate or replace external function abc \\n AS something')


        sql = format_ddl_with_create_if_not_exists(r'Create or Replace database database_1')
        assert(sql == 'create database if not exists database_1;\nuse database database_1')
        sql = format_ddl_with_create_if_not_exists(r'Create or Replace transient database database_1')
        assert(sql == 'create transient database if not exists database_1;\nuse database database_1')

        sql = format_ddl_with_create_if_not_exists(r'Create or Replace schema schemaabc')
        assert(sql == 'create schema if not exists schemaabc;\nuse schema schemaabc')
        sql = format_ddl_with_create_if_not_exists(r'Create or Replace transient schema schemaabc')
        assert(sql == 'create transient schema if not exists schemaabc;\nuse schema schemaabc')

        sql = format_ddl_with_create_if_not_exists("create or replace schema   mschema  with managed access;") # doesn't work
        # assert(sql == 'create schema if not exists mschema with managed access;\nuse schema database_1')
        # this does not work so far
        #sql = format_ddl_with_create_if_not_exists("create or replace TRANSIENT schema   mschema  with managed access;")

        sql = format_ddl_with_create_or_replace(r'create schema if not exists schema1')
        assert (sql == 'create or replace schema schema1;\nuse schema schema1')

        sql = format_ddl_with_create_or_replace("create TRANSIENT database if not exists db1")
        assert (sql == 'create or replace TRANSIENT database db1;\nuse database db1')
        sql = format_ddl_with_create_or_replace('create transient schema if not exists "schema_1"')
        assert (sql == 'create or replace transient schema "schema_1";\nuse schema "schema_1"')

        sql = format_ddl_with_create_or_replace("create schema if not exists mschema with managed access")
        #assert(sql == 'create or replace schema mschema with managed access')

        sql = format_ddl_with_create_or_replace("create schema  if not  exists mschema  with managed access")
        #assert (sql ==  'create or replace schema mschema with managed access')
        sql = format_ddl_with_create_or_replace("create TRANSIENT schema if not exists mschema with managed access")
        sql = format_ddl_with_create_or_replace("create transient schema if not exists mschema with managed access")

        statement_type = 'unknown'
        m = re.match(r"use\s+(database|schema)+\s+(\S+)\s*", 'Use database "db1"', flags=re.MULTILINE | re.IGNORECASE)
        #ms = re.match(r"use\s+(transient\s+)?schema\s+(\S+)", statement, flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = m.groups(0)[0]
            statement_type = statement_type.upper()
            if statement_type == "DATABASE":
                cur_database = m.groups(0)[1]
            else:
                cur_schema = m.groups(0)[1]

        m = re.match(r'create\s+(or\s+replace\s+)?(transient\s+)?(database|schema)\s+(if\s+not\s+exists\s)?(\S+)(\s+)?'
                     , r'create database if not exists abc', flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type =  m.groups()[2]
            statement_type = statement_type.upper()
            if statement_type == "DATABASE":
                cur_database = m.groups()[4]
            else:
                cur_schema = m.groups()[4]
            statement_type = 'CREATE ' + m.groups(0).upper + statement_type

        '''
        '''
        # pattern = r"\n*create\s*or\s*replace\s*(?P<object1>)\s*(?P<object2>)\s*(?P<object3>\w+)"
        pattern = pattern = r"\n*create\s+or\s+replace\s*(external)\s+(?P<object_type>\w+)\s+(?P<object_name>\w+)"


        m = re.match(pattern, sql, re.IGNORECASE)
        object_type = m.group("object_type")
        for text in m.groups:
            print(text)

        if object_type != None:
            object_type = m.group("object_type")
            decorator = "external"
            object_name = m.group("object_name")
            new_sql = "create " + decorator + " " + object_type + " if not exists " + object_name
        else:
            object_type = m.group("object1")
            decorator = ""
            object_name = m.group("object2")
            new_sql = "create " + object_type + " if not exists " + object_name
        

        sql = r"create or  repLACE procedure    abc  AS somethingelse \ncreate or replace table "
        m = re.match(pattern, sql, re.IGNORECASE)
        external = sql[m.regs[1][0]:m.regs[1][2]]
        object_type = m.group("object_type")
        new_sql = "create " + external + " " + object_type + " if not exists " + sql[m.regs[2][1]:]
        '''



        filedict = scriptloader.list_scripts("/Users/mlee/crossrep/", "ddl")
        failedStatements = []
        for file_obj in filedict:
            filename = os.path.join(file_obj['path'], file_obj['file'])
            print("\n---- Reading File: %s" % (filename))
            f = open(filename, "r")
            long_sql_text = f.read()
            f.close()

            sql_statements = scriptloader.get_statement_blocks(long_sql_text);
            failures = ''

            for statement in sql_statements:
                if statement.find("semicolon") >= 0:
                    assert()
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

