#!/usr/bin/env python3
import crossrep, re
"""
Created on April 28 2019
Updated on July 2nd 2020
reporting on customer account 
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'
# *********************************************************************************************************************
# 
# This module contains the Snowflake-specific logic of reporting account level unsupported objects :
# 
#  - evaluating foreign keys and sequences from snowhouse
#
# *********************************************************************************************************************

# foreign keys 
def repSHFKeys (dbname, tbddl_file, dropfk_file, addfk_file, cursor):
    if crossrep.verbose == True:
        print('Start evaluating foreign keys ... ')

    query =( "select DISTINCT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from  " + dbname +".INFORMATION_SCHEMA.TABLE_CONSTRAINTS" 
        " WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME " )
    if crossrep.verbose == True:
        print(query)
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
    if crossrep.verbose == True:
        print('Finish evaluating foreign keys ... ')



# get all tables refering to a sequence as default using PROD information schema
def repSHSeqTable( dbname, dfile, afile,cursor):
    query = "select distinct SCHEMA_NAME from " + dbname +".INFORMATION_SCHEMA.SCHEMATA where CATALOG_NAME = '" + dbname +"' and SCHEMA_NAME not in ('INFORMATION_SCHEMA','ACCOUNT_USAGE')" 
    #query = "select distinct SCHEMA_NAME from " + dbname +".INFORMATION_SCHEMA.SCHEMATA where CATALOG_NAME = '" + dbname +"' and SCHEMA_NAME = 'LOOKER' " 
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    record = cursor.fetchall()
    for row in record:
        scname = row[0]

        #gquery = "select distinct TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT from temp.public.temp_snowhouse_report where TABLE_SCHEMA = '" +scname + "'"
        gquery = ("create or replace temp table temp.public.temp_snowhouse_report as select distinct TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT from " + dbname +".INFORMATION_SCHEMA.columns "+
                " where TABLE_CATALOG = '" + dbname +"' and TABLE_SCHEMA = '"+ scname+"'"
                " and COLUMN_DEFAULT like '%NEXTVAL%' "+
                "  and DATA_TYPE = 'NUMBER'" )

        if crossrep.verbose == True:
            print(gquery)
        cursor.execute(gquery)

        query = (" select distinct TABLE_NAME,COLUMN_NAME,COLUMN_DEFAULT  from temp.public.temp_snowhouse_report ")
        if crossrep.verbose == True:
            print(query)
        cursor.execute(query)

        rec = cursor.fetchall()
        for r in rec:
            tbname = r[0]
            colname = r[1]
            defval = r[2]
             
            if crossrep.verbose == True:
                print('table db: '+dbname + '; seq db: '+ seqdb)
            
            if re.findall(r'(\S+\.NEXTVAL)', defval,re.MULTILINE|re.IGNORECASE):
                seqdb = defval.split('.')[0].strip('"')
                if seqdb != dbname.strip('"'):
                    dfile.write('alter table "'+dbname + '"."' +scname + '"."' + tbname + '" alter column '+ colname +' drop default' +';\n')
                    afile.write('alter table "'+dbname + '"."' +scname + '"."' + tbname + '" alter column '+ colname + ' set default '+ defval +';\n')
        cursor.execute("drop table if exists temp.public.temp_snowhouse_report")
    cursor.execute("commit")

