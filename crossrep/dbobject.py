#!/usr/bin/env python3
import crossrep, re, string 
"""
Created on March 27 2019
Updated on July 2nd 2020
only generate object: warehouse, network policy, resource monitor
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'
# *********************************************************************************************************************
# 
# This module contains the Snowflake-specific logic of creating database level objects :
#   The following user tables are created to store database level objects information.
#  - all_databases, all_schemas, all_stages, all_pipes, TABLE_CONSTRAINTS, ALL_SEQ_DEFAULT
#  - their DDLs that can be generated , then to be executed in target system 
#    scripts/ddl/31_create_stages_DDL.sql   => DDL to create stages
#    scripts/ddl/32_create_pipes_DDL.sql    => DDL to create pipes 
#    scripts/ddl/01_dbDDL_<DatabaseName>.sql => DDL to create all databases and objects within those databases, this is for manual replicaiton only
#           with snowflake replication, most of supported objects with the replicated database will be created as well.
#
tb_db = 'ALL_DATABASES'
tb_sc = 'ALL_SCHEMAS'
tb_constr = 'TABLE_CONSTRAINTS'
tb_seq = 'ALL_SEQ_DEFAULT'
tb_fgrant = 'ALL_FGRANTS'
tb_stage = 'ALL_STAGES'
tb_pipe = 'ALL_PIPES'
# *********************************************************************************************************************

### create table for storing all databases information excluding inbound share databases
### update the table if it's not new: update existing records, insert new records, delete the dropped ones (small table not doing merge any more)
# tb_db: table name for all databases
# tb_share: table name for all shares
# cursor: cursor connects to your snowflake account where it creates tables to store metadata  
def crDatabase (cursor):
    tbname = crossrep.tb_db
    if crossrep.verbose == True:
        print('Start creating database table ...')
    if crossrep.mode == 'CUSTOMER':
        query = ("create or replace  table " + tbname +" as select distinct DATABASE_NAME , DATABASE_OWNER from  snowflake.account_usage.databases " + 
        " where deleted is null and DATABASE_NAME not in (select distinct db_name from "+ crossrep.tb_share +" where db_name is not null and kind = 'INBOUND') " )
        cursor.execute(query )
    elif crossrep.mode == 'SNOWFLAKE':
        tb_temp = "TEMP_" + tbname
        cursor.execute("begin")
        cursor.execute("show databases in account " + crossrep.acctpref )
        # split(object_name,'.')[0] dbname, split(object_name,'.')[1] scname
        cursor.execute("create or replace temp table " + tb_temp +" as select distinct \"name\" DATABASE_NAME, \"owner\" DATABASE_OWNER from table(result_scan(last_query_id())) where \"origin\" = '' " +
        " and DATABASE_NAME not in (select distinct db_name from "+ crossrep.tb_share + " where db_name is not null and kind = 'INBOUND') " )

        cursor.execute("commit")
        # check whether table exists
        checkquery = ("select count(*) from information_schema.tables where table_catalog = '" + crossrep.default_db + "' and table_schema = '" + 
            crossrep.default_sc + "' and table_name = '" + tbname + "' and table_owner is not null")
        if crossrep.verbose == True:
            print('query checking database table existing: ' + checkquery) 
        cursor.execute(checkquery)
        rec = cursor.fetchall()
        for r in rec:
            if r[0] == 0:
                # create table if not existing
                print('create new database table')
                cursor.execute("create table " + tbname+" as select distinct DATABASE_NAME, DATABASE_OWNER from " + tb_temp ) 
            else:
                print ('update database table')
                # insert/update the delta of new users information 
                mquery = ( "merge into "+tbname+" tgt using (" +
                " select DATABASE_NAME, DATABASE_OWNER from " + tb_temp +
                "    minus " +
                " select DATABASE_NAME, DATABASE_OWNER from "+tbname +
                "  ) as src on tgt.DATABASE_NAME = src.DATABASE_NAME" +
                " when matched then update set tgt.DATABASE_OWNER = src.DATABASE_OWNER" +
                " when not matched then insert ( DATABASE_NAME, DATABASE_OWNER )" +
                "    values ( src.DATABASE_NAME, src.DATABASE_OWNER )" )
                if crossrep.verbose == True:
                    print(mquery)
                cursor.execute(mquery)

                # delete the one dropped
                dquery = ( "delete from "+tbname+" tgt using (" +
                " select DATABASE_NAME  from " + tbname +
                "    minus " +
                " select DATABASE_NAME from "+tb_temp +
                " ) as src where tgt.DATABASE_NAME = src.DATABASE_NAME" )
                if crossrep.verbose == True:
                    print(dquery)
                cursor.execute(dquery)
                cursor.execute("commit")
        cursor.execute("drop table if exists " + tb_temp )
        cursor.execute("commit")

    # split(object_name,'.')[0] dbname, split(object_name,'.')[1] scname

    if crossrep.verbose == True:
        print('Finish creating database table ...')

### generating a database's DDL for the database and all objects in the database, only needed for manual replication
# file_pref: file name prefix including folder path for storing DDLs for database level objects, one file for each database object DDLs
# of: output file
# dbname : database name
# tb_db: table name for all databases
# cursor: cursor connects to your snowflake account where it creates tables to store metadata  
#def genDatabaseDDL ( dbname, file_pref, cursor):
def genDatabaseDDL ( dbname, of, cursor):
    if crossrep.verbose == True:
        print('Start generating database object DDLs for ' + dbname + '...')

    dbname = crossrep.quoteID(dbname)
    dquery = "select get_ddl('database', '" + crossrep.acctpref_qualifier+dbname + "')"

    try:
        cursor.execute(dquery)
        #cursor.execute("select get_ddl('database', '" + crossrep.acctpref_qualifier+dbname + "')")
        row = cursor.fetchall()
        for record in row:
            ddl = record[0]
            #ddl=re.sub(r'\"([^:]+):.+',r'\1',ddl)
            #print('before ===> ' + ddl)
            #ddl=re.sub(r'create\s+(or\s+replace\s+)?(database|schema|table|view|materialized view|file format|function|sequence|procedure|materliazed view|external table|stage|pipe|stream|task)\s+',r'create \2 if not exists ',ddl,flags=re.MULTILINE|re.IGNORECASE)
            # new get_ddl will output with "if not exists", no need to replace any more, comment out line 120,121 (next 2 lines) and 124
            #ddl=re.sub(r'create\s+(or\s+replace\s+)?(table|view|materialized view|file format|function|sequence|procedure|external table|stage|pipe|stream|task)\s+',r'create \2 if not exists ',ddl,flags=re.MULTILINE|re.IGNORECASE)
            #ddl=re.sub(r'create\s+(or\s+replace\s+)?(database|schema)\s+(\S+)(\s+)?;',r'create \2 if not exists \3;\n use \2 \3;',ddl,flags=re.MULTILINE|re.IGNORECASE)
            # create VIEW if not exists S2.VIEW1 COPY GRANTS AS SELECT * FROM S1.TAB1;
            # remove COPY GRANTS on create view - not needed for newly created objects/grants
            #ddl=re.sub(r'create\s+(view if not exists)\s+(\S)+\s+(COPY GRANTS)\s+',r'create view if not exists \2 ',ddl,flags=re.MULTILINE|re.IGNORECASE)
            #print('after ===> ' + ddl)
            # FIELD_OPTIONALLY_ENCLOSED_BY = ''' ==> FIELD_OPTIONALLY_ENCLOSED_BY = ''''
            #ddl=re.sub(r'FIELD_OPTIONALLY_ENCLOSED_BY = \'\'\'',r'FIELD_OPTIONALLY_ENCLOSED_BY = "\'"',ddl,flags=re.MULTILINE|re.IGNORECASE)
            ddl=re.sub(r"FIELD_OPTIONALLY_ENCLOSED_BY = '''",r"FIELD_OPTIONALLY_ENCLOSED_BY = ''''",ddl,flags=re.MULTILINE|re.IGNORECASE)
            
            #of = open(file_pref + dbname+".sql","w") 
            of.write(ddl)
            #of.close()
    except Exception as err:
        print('An error occured in generating DDLs, skipped database "' + crossrep.acctpref_qualifier + dbname + '" :' + str(err))
        print('Query:\t' + dquery)

    if crossrep.verbose == True:
        print('Finish creating database DDL script file ...')

### drop all databases using all_databases table
# tbshare: table name for all shares
# ofile: output file for dropping database statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def dropAllDBs( ofile, cursor):
    if crossrep.verbose == True:
        print('Start generating drop database stmt ...')
    
    #ofile.write('use role accountadmin;\n')    
    query = (" select DATABASE_NAME from  "+ crossrep.tb_db )
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = crossrep.addQuotes(r[0])
        gsql1 = "drop database if exists " + name +"  ;\n "
        ofile.write(gsql1)

    if crossrep.verbose == True:
        print('Finish generating drop database stmt ...')

### create table for all database and its schemas excluding inbound share databases , only for mode = 'SNOWFLAKE'
# tb_sc: table name for all schemas
# tb_db : table name for all databases
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crSchema ( cursor):
    tbname = crossrep.tb_sc
    if crossrep.verbose == True:
        print('Start creating or updating schema table ...')
    isCreated = False 
    # check whether table exists
    checkquery = ("select count(*) from information_schema.tables where table_catalog = '" + crossrep.default_db + "' and table_schema = '" + crossrep.default_sc + "' and table_name = '" 
    + tbname + "' and table_owner is not null")
    
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            # create table if not existing
            if crossrep.verbose == True:
                print('create new schema table')
        else:
            if crossrep.verbose == True:
                print ('update schema table')
            isCreated = True

    tb_temp = "TEMP_" + tbname

    cursor.execute("select distinct DATABASE_NAME from "+ crossrep.tb_db + " order by DATABASE_NAME")
    rec = cursor.fetchall()
    for r in rec:
        dbname = crossrep.quoteID (r[0])
        #squery = "SHOW SCHEMAS IN DATABASE " + crossrep.source_acct + ".\""+dbname+"\""
        squery = "SHOW SCHEMAS IN DATABASE " + crossrep.acctpref_qualifier +dbname 

        cursor.execute("begin")
        cquery = ("create or replace temp table "+ tb_temp+" as select '"+ dbname+"'::string CATALOG_NAME, \"name\" SCHEMA_NAME,  \"owner\" SCHEMA_OWNER from table(result_scan(last_query_id())) " +
        " where SCHEMA_OWNER != '' and SCHEMA_NAME != '' ")
        if crossrep.verbose == True:
                print (squery)
                print (cquery)
        cursor.execute(squery )
        cursor.execute(cquery)
        cursor.execute("commit")
        
        if isCreated == False:
            if crossrep.verbose == True:
                print ('create schema table')
            cursor.execute("create or replace table "+ tbname+" as select CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER from  " + tb_temp )
            isCreated = True 
        else:
            if crossrep.verbose == True:
                print ('update schema table')
            # insert/update the delta of new users information 
            mquery = ( "merge into "+tbname+" tgt using (" +
            " select CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER from " + tb_temp + 
            "    minus " +
            " select CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER from "+tbname +
            "   ) as src on tgt.SCHEMA_NAME = src.SCHEMA_NAME AND tgt.CATALOG_NAME = src.CATALOG_NAME " +
            " when not matched then insert ( CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER)" +
            "    values ( src.CATALOG_NAME, src.SCHEMA_NAME, src.SCHEMA_OWNER )" )
            if crossrep.verbose == True:
                print(mquery)
            cursor.execute(mquery)

            # delete the one dropped
            dquery = ( "delete from "+tbname+" tgt using (" +
            " select CATALOG_NAME , SCHEMA_NAME  from " + tbname + " where CATALOG_NAME = '"+ dbname+"'"
            "    minus " +
            " select CATALOG_NAME, SCHEMA_NAME from "+tb_temp +
            " ) as src where tgt.SCHEMA_NAME = src.SCHEMA_NAME AND tgt.CATALOG_NAME = src.CATALOG_NAME" )
            if crossrep.verbose == True:
                print(dquery)
            cursor.execute(dquery)
            cursor.execute("commit")
    cursor.execute("drop table if exists " + tb_temp )
    cursor.execute("commit")
    if crossrep.verbose == True:
        print('Finish creating schema table ...')

### create table for all stages , only needed for mode = 'SNOWFLAKE'
# tb_stage : table name for all stages
# tb_sc: table name for schemas
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crStages(cursor):
    if crossrep.verbose == True:
        print('creating stage table ...')
    tbname = crossrep.tb_stage 
    created = False
    query = ("select CATALOG_NAME, SCHEMA_NAME from "+ crossrep.tb_sc + " order by CATALOG_NAME, SCHEMA_NAME" )
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        dbname = crossrep.addQuotes(r[0])
        scname = crossrep.addQuotes(r[1])
        cursor.execute("begin")
        squery = "SHOW STAGES IN SCHEMA "+ crossrep.acctpref_qualifier + dbname + "." + scname
        if crossrep.verbose == True:
            print(squery)
        cursor.execute(squery)
        if created == False:
            if crossrep.verbose == True:
                print('creating stage table')
            cursor.execute("create or replace table "+ tbname+" as select distinct $2 stage_name,  $3 stage_catalog, $4 stage_schema, $5 stage_url, $8 stage_owner, $9 comment, $10 stage_region, $11 stage_type, $12 cloud " + 
            " from table(result_scan(last_query_id()))")
            created = True
        else :
            if crossrep.verbose == True:
                print('inserting into stage table')
            cursor.execute("insert into "+ tbname+" select distinct $2 stage_name,  $3 stage_catalog, $4 stage_schema, $5 stage_url, $8 stage_owner, $9 comment, $10 stage_region, $11 stage_type, $12 cloud " + 
            " from table(result_scan(last_query_id()))")
        cursor.execute("commit")
    if crossrep.verbose == True:
        print('Finish creating stage table ...')

### generating all stages' DDL using all_stages table information
# tbstage: table name for all stages information
# ofile: output file for all stage DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genAllStageDDL( ofile, cursor):
    #query = ("select distinct stage_catalog, stage_schema, stage_name, stage_url, region, type, comment, owner from "+tbstage +
    #" where owner is not null  order by stage_catalog, stage_schema, stage_name ")
    if crossrep.mode == 'SNOWFLAKE':
        query = ("select distinct stage_catalog, stage_schema, stage_name, stage_url, stage_region, stage_type, comment, stage_owner from " + crossrep.tb_stage +
            " where stage_owner is not null order by stage_catalog, stage_schema, stage_name ") 
    elif crossrep.mode == 'CUSTOMER':    
        query = ("select distinct stage_catalog, stage_schema, stage_name, stage_url, stage_region, stage_type, comment, stage_owner from snowflake.account_usage.stages " +
        " where stage_owner is not null and deleted is null order by stage_catalog, stage_schema, stage_name ")

    if crossrep.verbose == True:
        print("stageQuery:" + query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        stage_catalog = r[0]
        stage_schema = r[1]  
        stage_name = r[2]
        stage_url = r[3]
        region = r[4] 
        type = r[5] 
        comment = r[6]
        owner = r[7]

        if not stage_url:
            stage_url = ''
        if not region:
            region = ''
        if not type:
            type = ''
        if not comment:
            comment = ''

        if crossrep.verbose==True:
            print("stage_catalog:"+stage_catalog)
            print("stage_schema:"+stage_schema)
            print("stage_name:"+stage_name)
            print("stage_url:"+stage_url)
            print("region:"+region)
            print("type:"+type)
            print("comment:"+comment)
            print("owner:"+owner)
        if stage_name.isdigit() == True:
            continue
        crSQL = 'CREATE STAGE IF NOT EXISTS  "' + stage_catalog + '"."' + stage_schema + '"."' + stage_name + '"'
        if crossrep.isBlank (stage_url ) == False :
            crSQL = crSQL + " URL =  '" + stage_url + "' comment='" + comment + "'"
            if crossrep.verbose==True:
                print(crSQL)
        ofile.write(crSQL+';\n')
        #ofile.write('grant ownership on stage "' + stage_catalog + '"."' + stage_schema + '"."' + stage_name + '" to role ' + owner+';\n')

### generating DDL for all stages of a specific database using all_stages table information 
# dbname : database name for the stages belong to
# tb_stage: table name for all stages information
# ofile: output file for all stage DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genStageDDLByDB(dbname, ofile, cursor):
    if crossrep.mode == 'SNOWFLAKE':
        query = ("select distinct stage_catalog, stage_schema, stage_name, stage_url, stage_region, stage_type, comment, stage_owner from " + crossrep.tb_stage +
            " where stage_owner is not null and stage_catalog = '"+dbname + "' order by stage_catalog, stage_schema, stage_name ") 
    elif crossrep.mode == 'CUSTOMER':    
        query = ("select distinct stage_catalog, stage_schema, stage_name, stage_url, stage_region, stage_type, comment, stage_owner from snowflake.account_usage.stages " +
        " where stage_owner is not null and deleted is null and stage_catalog = '"+dbname + "' order by stage_catalog, stage_schema, stage_name ")

    if crossrep.verbose == True:
        print("stageQuery:" + query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        stage_catalog = r[0]
        stage_schema = r[1]  
        stage_name = r[2]
        stage_url = r[3]
        region = r[4] 
        type = r[5] 
        comment = r[6]
        if not stage_url:
            stage_url = ''
        if not region:
            region = ''
        if not type:
            type = ''
        if not comment:
            comment = ''

        if stage_name.isdigit() == True:
            continue
        crSQL = 'CREATE STAGE IF NOT EXISTS  "' + stage_catalog + '"."' + stage_schema + '"."' + stage_name + '"'
        if crossrep.isBlank (stage_url ) == False :
            crSQL = crSQL + " URL =  '" + stage_url + "' comment='" + comment + "'"
            if crossrep.verbose==True:
                print(crSQL)
        ofile.write(crSQL+';\n')
        #ofile.write('grant ownership on stage "' + stage_catalog + '"."' + stage_schema + '"."' + stage_name + '" to role ' + owner+';\n')

### generating DDL for all stages of a specific database using all_stages table information 
# dblist : database list for the stages belong to
# tbstage: table name for all stages information
# ofile: output file for all stage DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genStageDDLByDBList(dblist, ofile, cursor):
    inlist = crossrep.genInList(dblist)
    if inlist != '':
        inPred = "and stage_catalog " + inlist
    else :
        inPred = ''

    if crossrep.mode == 'SNOWFLAKE':
        query = ("select distinct stage_catalog, stage_schema, stage_name, stage_url, stage_region, stage_type, comment, stage_owner from " + crossrep.tb_stage +
            " where stage_owner is not null "+inPred + " order by stage_catalog, stage_schema, stage_name ") 
    elif crossrep.mode == 'CUSTOMER':    
        query = ("select distinct stage_catalog, stage_schema, stage_name, stage_url, stage_region, stage_type, comment, stage_owner from snowflake.account_usage.stages " +
        " where stage_owner is not null and deleted is null "+inPred + " order by stage_catalog, stage_schema, stage_name ")

    if crossrep.verbose == True:
        print("stageQuery:" + query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        stage_catalog = r[0]
        stage_schema = r[1]  
        stage_name = r[2]
        stage_url = r[3]
        region = r[4] 
        type = r[5] 
        comment = r[6]

        if not stage_url:
            stage_url = ''
        if not region:
            region = ''
        if not type:
            type = ''
        if not comment:
            comment = ''

        if stage_name.isdigit() == True:
            continue
        crSQL = 'CREATE STAGE IF NOT EXISTS  "' + stage_catalog + '"."' + stage_schema + '"."' + stage_name + '"'
        if crossrep.isBlank (stage_url ) == False :
            crSQL = crSQL + " URL =  '" + stage_url + "' comment='" + comment + "'"
            if crossrep.verbose==True:
                print(crSQL)
        ofile.write(crSQL+';\n')
        #ofile.write('grant ownership on stage "' + stage_catalog + '"."' + stage_schema + '"."' + stage_name + '" to role ' + owner+';\n')

### get all pipes information from SHOW PIPES command using PROD information schema
# tb_pipe: table name for all pipes
# tb_sc: table name for all schemas
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crPipes(cursor):
    if crossrep.verbose == True:
        print('creating pipe table ...')
    tbname = crossrep.tb_pipe 
    if crossrep.mode == 'CUSTOMER':
        cursor.execute("begin")
        cursor.execute("SHOW PIPES IN ACCOUNT " + crossrep.acctpref)
        cursor.execute("create or replace table "+ tbname+" as select distinct \"name\" name,  \"database_name\" database_name, \"schema_name\" schema_name, \"definition\" definition , \"owner\" owner, \"notification_channel\" notification_channel , \"comment\" comment " + 
        " from table(result_scan(last_query_id())) where owner is not null order by database_name, schema_name, name")
        cursor.execute("commit")
    elif crossrep.mode == 'SNOWFLAKE':
        created = False
        query = ("select CATALOG_NAME, SCHEMA_NAME from "+ crossrep.tb_sc + " order by CATALOG_NAME, SCHEMA_NAME" )
        if crossrep.verbose == True:
            print(query)
        cursor.execute(query)
        rec = cursor.fetchall()
        for r in rec:
            dbname = crossrep.addQuotes(r[0])
            scname = crossrep.addQuotes(r[1])
            cursor.execute("begin")
            squery = "SHOW PIPES IN SCHEMA "+crossrep.acctpref_qualifier + dbname+"."+scname
            if crossrep.verbose == True:
                print(squery)
            cursor.execute(squery)
            if created == False:
                if crossrep.verbose == True:
                    print('creating pipe table')
                cursor.execute("create or replace table "+ tbname+" as select distinct \"name\" name,  \"database_name\" database_name, \"schema_name\" schema_name, \"definition\" definition , \"owner\" owner, \"notification_channel\" notification_channel , \"comment\" comment " + 
                " from table(result_scan(last_query_id()))")
                created = True
            else :
                if crossrep.verbose == True:
                    print('inserting into pipe table')
                cursor.execute("insert into "+ tbname+" select distinct \"name\" name,  \"database_name\" database_name, \"schema_name\" schema_name, \"definition\" definition , \"owner\" owner, \"notification_channel\" notification_channel , \"comment\" comment " + 
                " from table(result_scan(last_query_id()))")
            cursor.execute("commit")
    if crossrep.verbose == True:
        print('Finsh creating pipe table ...')
 
### create pipe if not exists mypipe as copy into mytable from @mystage;
### generating DDL for all pipes using all_pipes table information 
# tb_pipe: table name for all pipes information
# ofile: output file for all pipe DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genAllPipeDDL(ofile, cursor):
    query = ("select distinct database_name, schema_name, name, definition, comment, owner from "+crossrep.tb_pipe +
        "  order by database_name, schema_name, name ")
    if crossrep.verbose == True:
        print("Pipe Query:" + query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        dbname = r[0]
        scname = r[1]  
        pipename = r[2]
        definition = r[3]
        comment = r[4]
        if not comment:
            comment = ''        
        owner = r[5]
        if pipename.isdigit() == True:
            continue
        crSQL = 'CREATE PIPE IF NOT EXISTS  "' + dbname + '"."' + scname + '"."' + pipename + '" '  
        if crossrep.isBlank (comment ) == False :
            crSQL = crSQL + " COMMENT =  '" + comment + "' " 
        crSQL = crSQL + ' AS ' + definition 
        ofile.write(crSQL+';\n')
        if crossrep.verbose==True:
            print(crSQL)
        #ofile.write('grant ownership on pipe "' + dbname + '"."' + scname + '"."' + pipename + '" to role ' + owner+';\n')

### create pipe if not exists mypipe as copy into mytable from @mystage;
### generating DDL for all pipes of a specific database using all_pipes table information 
# dbname : database name for the pipes belong to
# tbpipe: table name for all pipes information
# ofile: output file for all pipe DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genPipeDDLByDB(dbname, ofile, cursor):
    query = ("select distinct database_name, schema_name, name, definition, comment, owner from "+crossrep.tb_pipe +
        " where owner is not null  and database_name = '" + dbname+ "' order by database_name, schema_name, name ")
    print("Pipe Query:" + query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        dbname = r[0]
        scname = r[1]  
        pipename = r[2]
        definition = r[3]
        comment = r[4]
        if not comment:
            comment = ''        
        owner = r[5]
        if pipename.isdigit() == True:
            continue
        crSQL = 'CREATE PIPE IF NOT EXISTS  "' + dbname + '"."' + scname + '"."' + pipename + '" '  
        if crossrep.isBlank (comment ) == False :
            crSQL = crSQL + " COMMENT =  '" + comment + "' " 
        crSQL = crSQL + ' AS ' + definition 
        ofile.write(crSQL+';\n')
        if crossrep.verbose==True:
            print(crSQL)
        #ofile.write('grant ownership on pipe "' + dbname + '"."' + scname + '"."' + pipename + '" to role ' + owner+';\n')

### create pipe if not exists mypipe as copy into mytable from @mystage;
### generating DDL for all pipes of a specific database using all_pipes table information 
# dblist : database list for the pipes belong to
# tbpipe: table name for all pipes information
# ofile: output file for all pipe DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genPipeDDLByDBList(dblist, ofile, cursor):
    inlist = crossrep.genInList(dblist)
    if inlist != '':
        inPred = "and database_name " + inlist
    else :
        inPred = ''

    query = ("select distinct database_name, schema_name, name, definition, comment, owner from "+crossrep.tb_pipe +
        " where owner is not null  " + inPred+ " order by database_name, schema_name, name ")
    print("Pipe Query:" + query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        dbname = r[0]
        scname = r[1]  
        pipename = r[2]
        definition = r[3]
        comment = r[4]
        if not comment:
            comment = ''        
        owner = r[5]
        if pipename.isdigit() == True:
            continue
        crSQL = 'CREATE PIPE IF NOT EXISTS  "' + dbname + '"."' + scname + '"."' + pipename + '" '  
        if crossrep.isBlank (comment ) == False :
            crSQL = crSQL + " COMMENT =  '" + comment + "' " 
        crSQL = crSQL + ' AS ' + definition 
        ofile.write(crSQL+';\n')
        if crossrep.verbose==True:
            print(crSQL)

### create table for all table constraints only for 'SNOWFLAKE' mode
# tb_constr: table name for all constraints
# tb_db: table name for all databases
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crTable_Constraints (cursor):
    if crossrep.verbose == True:
        print('Start creating constraint table '+ crossrep.tb_constr + ' ...')
    tbname = crossrep.tb_constr 
    created = False
    query = ("select distinct database_name from "+ crossrep.tb_db + " order by database_name" )
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        dbname = crossrep.quoteID(r[0])
        if crossrep.verbose == True:
            print('dbname:'+dbname)
        if created == False:
            # SNOW-84847
            cursor.execute("create or replace table "+ tbname+" as select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_TYPE from " + crossrep.acctpref_qualifier+
                 dbname +".INFORMATION_SCHEMA.TABLE_CONSTRAINTS")
            created = True
        else :
            cursor.execute("insert into "+ tbname+" select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_TYPE from  " + crossrep.acctpref_qualifier+
                 dbname +".INFORMATION_SCHEMA.TABLE_CONSTRAINTS")
        cursor.execute("commit")
    if crossrep.verbose == True:
        print('Finish creating constraint table ...')

### report foreign key that refers to database outside of its table's database 
# tbddl_file: output file name for table DDL
# dropfk_file: output file name for dropping cross-db foreign keys 
# addfk_file: output file name for adding cross-db foreign keys back
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def repFKeys (tbddl_file, dropfk_file, addfk_file, cursor):
    if crossrep.verbose == True:
        print('Start evaluating foreign keys ... ')
    
    if crossrep.mode == 'CUSTOMER':
        query =( "select DISTINCT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from snowflake.account_usage.TABLE_CONSTRAINTS"  +  
            " WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND DELETED IS NULL order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME " )
    elif crossrep.mode == 'SNOWFLAKE':
        query =( "select DISTINCT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from "  +  crossrep.tb_constr +
            " WHERE CONSTRAINT_TYPE = 'FOREIGN KEY'  order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME " )

    tbseq = 1
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        dbname = r[0]
        scname = r[1]
        tbname = r[2]
        dquery = "select get_ddl('table','\""+ dbname +"\".\"" +scname +"\".\"" + tbname+"\"')"
        if crossrep.verbose == True:
            print('get ddl: ' + dquery)
        try:
            cursor.execute(dquery)
            record = cursor.fetchall()
            for row in record:
                tbddl = row[0]
                if crossrep.verbose == True:
                    print (row[0])
                if FKCrossDB(tbddl, dbname, scname, tbname, dropfk_file, addfk_file,tbseq,cursor) == True:
                    tbddl_file.write( '-- table sequence '+str(tbseq)+'; In Schema:  '  + dbname +"." +scname +'\n')
                    tbddl_file.write( tbddl + '\n\n')
                    tbseq += 1
        except Exception as err:
            print('An error occured during reporting foreign keys, skipped table "' + dbname + '"."' + scname + '"."' + tbname + '" :' + str(err))
            print('Query:\t' + dquery)
    if crossrep.verbose == True:
        print('Finish evaluating foreign keys ... ')

### checking whether the table has cross-db FK (refers to table outside of its table's DB), return boolean true or false  
# tbddl: table DDL definition string
# tabledb: database name for the table
# tablesc: schema name for the table
# tablename:  table name
# dropfk_file: output file name for dropping cross-db foreign keys 
# addfk_file: output file name for adding cross-db foreign keys back
# tbseq: sequence number to record how many such cross-db FKs
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def FKCrossDB(tbddl, tabledb, tablesc, tablename, dropfk_file, addfk_file,tbseq,cursor):
    isCrossDB = False
    usedRole = False
    #print(tbddl)
    ## constraint D_PATIENT_FK2 foreign key (SOURCE_SYSTEM_ID) references PDX_DEV.EDW.D_SOURCE_SYSTEM(SOURCE_SYSTEM_ID)
    wlist = re.findall(r'((constraint\s+(\S+)\s+)?foreign\s+key\s*\(\s*(.+?)\)\s*references\s+(\S+?)\s*\((\S+?)\))', tbddl,re.MULTILINE|re.IGNORECASE)
    #print(wlist)
    for fk in wlist:
        #print('fk: ' + fk[0])
        #print('ref qualifier: ' + fk[4])
        dname = fk[4].split('.')[0]
        if(dname.startswith("\"")):
            dname = dname.strip('"')
        else:
            dname = dname.upper()
        #print('ref db: ' + dname + '; table db: ' + tabledb)
        if dname != tabledb:
            #print('crossing db')
            dropfk_file.write( '-- table sequence '+str(tbseq)+'\n')
            addfk_file.write( '-- table sequence '+str(tbseq)+'\n')
            if usedRole == False:
                #print('finding owner role')
                role = findOwnerRole(tabledb, tablesc, tablename, cursor)
                dropfk_file.write('-- use role ' + role+';\n')
                addfk_file.write('-- use role ' + role+';\n')
                usedRole = True

            if crossrep.isBlank(fk[1]) == True:
                dropfk_file.write('alter table '+tabledb + '.' +tablesc + '.' + tablename + ' drop foreign key ('+ fk[3] +');\n')
            else:
                dropfk_file.write('alter table '+tabledb + '.' +tablesc + '.' + tablename + ' drop constraint '+ fk[2] +';\n')

            addfk_file.write('alter table '+tabledb + '.' +tablesc + '.' + tablename + ' add '+fk[0] +';\n')
            #print('referring to db:' + dname)
            if isCrossDB == False:
                isCrossDB = True
    return isCrossDB

### get all table information refering to a sequence as default from information schema
### can't use account_usage.columns due to SNOW-76758
# tb_seq: table name for all default sequence information
# tb_db: table name for all databases
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crTable_DefaultSequence( cursor):
    if crossrep.verbose == True:
        print('Start creating default sequence table ...')
    tbname = crossrep.tb_seq 

    isCreated = False
    query = (" select distinct DATABASE_NAME from  "+ crossrep.tb_db )

    #query = ("select name from "+ tbdatabase + " order by name" )
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        dbname = r[0]
        if crossrep.mode == 'CUSTOMER':
            infosc_prefix = ''
        elif crossrep.mode == 'SNOWFLAKE':
            infosc_prefix = crossrep.acctpref_qualifier + "\"" + dbname +"\"."

        if isCreated == False:
            cquery = ("create or replace table "+ tbname + " as select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT from " + 
            infosc_prefix  +"INFORMATION_SCHEMA.columns where TABLE_CATALOG = '" + dbname + 
            "' and COLUMN_DEFAULT like '%NEXTVAL%' and DATA_TYPE = 'NUMBER'" )
            '''
            cquery = ("create or replace table "+ tbname + " as select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT from " + 
            crossrep.acctpref_qualifier + "\"" + dbname +"\".INFORMATION_SCHEMA.columns "+
                " where COLUMN_DEFAULT like '%NEXTVAL%' and DATA_TYPE = 'NUMBER'" )
            '''
            if crossrep.verbose == True:
                print(cquery)
            cursor.execute(cquery)
            cursor.execute("commit")
            isCreated = True
        else:
            iquery = ("insert into "+ tbname + " select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT from " + 
            infosc_prefix  +"INFORMATION_SCHEMA.columns where TABLE_CATALOG = '" + dbname + 
            "' and COLUMN_DEFAULT like '%NEXTVAL%' and DATA_TYPE = 'NUMBER'" )
            '''
            iquery = ("insert into "+ tbname + " select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT from " + 
            crossrep.acctpref_qualifier + "\"" + dbname +"\".INFORMATION_SCHEMA.columns "+
            " where COLUMN_DEFAULT like '%NEXTVAL%' and DATA_TYPE = 'NUMBER'" )
            '''
            if crossrep.verbose == True:
                print(iquery)
            try:
                cursor.execute(iquery)
                cursor.execute("commit")
            except Exception as err:
                print('An error occurred during creating default sequence table:' + str(err))
                print('query: \n' + iquery)
    if crossrep.verbose == True:
        print('Finish creating default sequence table ...')

### get all tables refering to a sequence as default using PROD information schema
### can either drop the cross-db sequence default or alter it to refer to its own db
### it's not supported to add a sequence default so adding it back after dropping is not a option
# tb_seq: table to storing sequence number for all tables with cross-db sequence default
# dfile: output file for dropping cross-db sequence default
# afile: output file for altering cross-db sequence default to refer to its database sequence default
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def repAllSeqTable( dfile, afile,cursor):
    if crossrep.verbose == True:
        print('Start reporting cross-db referenced default sequence ...')
    '''
    # can't use account_usage.columns due to SNOW-76758
    cquery = (" select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT from snowflake.account_usage.columns "+
    " where COLUMN_DEFAULT like '%NEXTVAL%' "+
    "  and DATA_TYPE = 'NUMBER'" )
    '''
    gquery = "select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT from " + crossrep.tb_seq 
    try:
        cursor.execute(gquery)
        rec = cursor.fetchall()
        for r in rec:
            dbname = r[0]
            scname = r[1]
            tbname = r[2]
            colname = r[3]
            defval = r[4]
            #print('table=> '+dbname + '.' +scname + '.' + tbname + '; column '+ colname + '; default '+ defval)
            # PLAYPEN4J.PUBLIC.SEQ.NEXTVAL
            if re.findall(r'(\S+\.NEXTVAL)', defval,re.MULTILINE|re.IGNORECASE):
                seqdb = defval.split('.')[0].strip('"')
                if seqdb != dbname.strip('"'):
                    defval = defval.replace(seqdb, dbname, 1)
                    dfile.write('alter table "'+dbname + '"."' +scname + '"."' + tbname + '" alter column '+ colname +' drop default' +';\n')
                    afile.write('alter table "'+dbname + '"."' +scname + '"."' + tbname + '" alter column '+ colname + ' set default '+ defval +';\n')
        cursor.execute("commit")
    except Exception as err:
        print('An error occurred during reporting cross-db reference for default sequence:' + str(err))
        print('\n' + iquery)
    if crossrep.verbose == True:
        print('Finish reporting cross-db referenced default sequence ...')

### reporting on external tables
# tbschema: table name for all schemas
# drop_file: output file for dropping all external tables
# ddl_file: output file for DDLs of valid external tables
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def repExTable ( drop_file, ddl_file,cursor):
    if crossrep.verbose == True:
        print('Start reporting external tables ...')
    #query = ("select dbname, name from "+ tbschema + " order by dbname, name" )
    if crossrep.mode == 'CUSTOMER':
        query = (" select CATALOG_NAME, SCHEMA_NAME from snowflake.account_usage.SCHEMATA where SCHEMA_OWNER is not null and DELETED is null order by CATALOG_NAME, SCHEMA_NAME ")
    elif crossrep.mode == 'SNOWFLAKE':
        query = (" select CATALOG_NAME, SCHEMA_NAME from " + crossrep.tb_sc + " where SCHEMA_OWNER is not null order by CATALOG_NAME, SCHEMA_NAME ")

    if crossrep.verbose == True:
        print(query)
        print('account prefix:' + crossrep.acctpref_qualifier)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        dbname = crossrep.quoteID(r[0])
        scname = crossrep.quoteID(r[1])
        if crossrep.verbose == True:
            print("dbname: " + dbname + "; scname: "+scname )
        try:
            cursor.execute("begin")
            cursor.execute("show external tables in schema "+crossrep.acctpref_qualifier + dbname+"."+scname)
            #cursor.execute("show external tables in account " )
            #cursor.execute('create or replace temp table temp_exttbs as select "database_name" dbname, "schema_name" scname, "name" name,"owner" owner,"invalid" invalid from table(result_scan(last_query_id())) ')
            cursor.execute('create or replace temp table temp_exttbs as select "name" name,"owner" owner,"invalid" invalid from table(result_scan(last_query_id())) ')
            cursor.execute("commit")
            ext_query = "select name from temp_exttbs " 
            cursor.execute(ext_query)
            record = cursor.fetchall()
            for row in record: 
                #dbname = row[0]
                #scname = row[1]
                tname = row[0]
                if crossrep.verbose == True:
                    print("tbname:"+tname)
                drop_file.write('drop external table if exists "'+ dbname + '"."'+ scname+'"."'+tname + '" ;\n') 
                #rep_file.write('"'+ dbname + '"."'+ scname+'"."'+tname + '"\n')
            
            val_query = ("select name, owner from temp_exttbs where invalid = 'false'" )
            cursor.execute(val_query)
            record = cursor.fetchall()

            for row in record: 
                tname = row[0]
                owner = row[1]
                if crossrep.verbose == True:
                    print("dbname: " + dbname + "; scname: "+scname + "; tbname: "+tname)
                ddl_file.write('--"'+ dbname + '"."'+ scname+'"."'+tname + '"\n')
                dquery = "select get_ddl('table','"+ dbname +"." +scname +"." + tname+"')"
                if crossrep.verbose == True:
                    print(dquery)
                cursor.execute("select get_ddl('table','"+ dbname +"." +scname +"." + tname+"')")
                rec = cursor.fetchall()
                for r in rec:
                    tbddl = r[0]
                    ddl_file.write(tbddl + '\n\n')
                    #ddl_file.write('grant ownership on table ' + '"'+ dbname + '"."'+ scname+'"."'+tname + '" to role ' + owner+ ';\n\n')
            
            cursor.execute('drop table if exists temp_exttbs ')
            cursor.execute("commit")
        except Exception as err:
            print('An error occurred in reporting external tables:' + str(err))
            print("Skipped external tables in schema "+crossrep.acctpref_qualifier + dbname+"."+scname)
    if crossrep.verbose == True:
        print('Finish reporting external tables ... ')

### handling invalid and cross-db referrenced materialized views
# tbschema: table name for all schemas
# drop_file: output file for dropping invalid and cross-db referrenced materialized views
# ddl_file: output file for DDLs of cross-db referrenced materialized views
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def repMVs ( drop_file, ddl_file,cursor):
    if crossrep.verbose == True:
        print('Start reporting cross-db referenced materized views ... ')
    #query = ("select dbname, name from "+ tbschema + " order by dbname, name" )
    if crossrep.mode == 'CUSTOMER':
        query = (" select CATALOG_NAME, SCHEMA_NAME from snowflake.account_usage.SCHEMATA where SCHEMA_OWNER is not null and DELETED is null order by CATALOG_NAME, SCHEMA_NAME ")
    elif crossrep.mode == 'SNOWFLAKE':
        query = (" select CATALOG_NAME, SCHEMA_NAME from " + crossrep.tb_sc + " where SCHEMA_OWNER is not null order by CATALOG_NAME, SCHEMA_NAME ")
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        try:
            dbname = crossrep.quoteID(r[0])
            scname = crossrep.quoteID(r[1])
            if crossrep.verbose == True:
                print("dbname:"+dbname+"; scname:"+scname)
            cursor.execute("begin")
            cursor.execute("show materialized views in schema "+crossrep.acctpref_qualifier + dbname+"."+scname)
            #cursor.execute("show materialized views in account " )
            cursor.execute('create or replace temp table temp_mvs as select "database_name" dbname, "schema_name" scname, "name" name,"source_database_name" sdbname,"owner" owner,"invalid" invalid, "text" ddl from table(result_scan(last_query_id())) ')
            cursor.execute("commit")
            inv_query = "select name from temp_mvs where invalid = 'true' " 
            cursor.execute(inv_query)
            record = cursor.fetchall()

            #rep_file.write('--invalid MV list ... ')
            for row in record: 
                vname = crossrep.quoteID(row[0])
                drop_query = "drop materialized view '"+ dbname + "'.'"+ scname+"'.'"+vname + "' ;\n "
                drop_file.write(drop_query) 
                if crossrep.verbose == True:
                    print(drop_query)
                #rep_file.write('"'+ dbname + '"."'+ scname+'"."'+vname + '"\n')
            
            crossdb_query = ("select name, ddl, owner from temp_mvs where invalid = 'false' and sdbname != dbname" )
            cursor.execute(crossdb_query)
            record = cursor.fetchall()
            #rep_file.write('--cross-db MV list ... ')
            for row in record: 
                vname = crossrep.quoteID(row[0])
                ddl = row[1]
                owner = row[2]
                #rep_file.write('"'+ dbname + '"."'+ scname+'"."'+vname + '"\n')
                drop_file.write('drop materialized view "'+ dbname + '"."'+ scname+'"."'+vname + '" ; \n') 
                ddl_file.write('--"'+ dbname + '"."'+ scname+'"."'+vname + '"\n')
                ddl_file.write(ddl + '\n\n')
                #ddl_file.write('grant ownership on view ' + '"'+ dbname + '"."'+ scname+'"."'+vname + '" to role ' + owner+ ';\n\n')

            cursor.execute('drop table if exists temp_mvs ')
            cursor.execute("commit")
        except Exception as err:
            print('An error occurred in reporting cross-db referenced MVs :' + str(err))
            print("Skipped MVs in schema "+crossrep.acctpref_qualifier + dbname+"."+scname)

    if crossrep.verbose == True:
        print('Finish reporting cross-db referenced materized views ... ')

