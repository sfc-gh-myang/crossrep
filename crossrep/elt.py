#!/usr/bin/env python3
import crossrep, re, string 
import threading
"""
Created on July 10 2019
Updated on July 2nd 2020
Loading, unloading data from tables
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'
# *********************************************************************************************************************
# 
# This module contains the Snowflake-specific logic of creating database level objects :
#   The following user tables are created to store database level objects information.
#  - all_databases, all_schemas, all_stages, all_pipes, TABLE_CONSTRAINTS, ALL_SEQ_DEFAULT
#  - their DDLs that can be generated , then to be executed in target system 
# 
# Important Note: This is for manual replication, meaning you are unloading your snowflake data from one snowflake account, 
#  then loading it into another snowflake account using this ELT scripts. With snowflake repliction engine, 
#  you don't need this script.
# 
# *********************************************************************************************************************

### unloading data from tables by database, one file for all unloading jobs of tables for each database. 
### update the table if it's not new: update existing records, insert new records, delete the dropped ones
# ufile_pref: file prefix including folder path for unloading jobs on database level objects, one file for each database object DDLs
# ufile_pref: file prefix including folder path for loading jobs on database level objects, one file for each database object DDLs
# tb_share: table name for all shares
# stgname: stage name that's going to hold data, create sub-folder in the stage with path of database name/schema name/table name
# ffname: file format name used for unloading
# cursor: cursor connects to your snowflake account where it creates tables to store metadata  
def ELTByDatabase ( ufile_pref, lfile_pref, stgname, ffname, dbname, cursor):
    if crossrep.verbose == True:
        print('Start generating unloading data copy statement ...')
        print(ufile_pref + '; '+ lfile_pref + '; '+ stgname + '; '+ ffname + '; '+ dbname)

    dbname = crossrep.quoteID(dbname)
    query = (" select distinct TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,FLOOR(BYTES/1000000000,1) as GB from  " + crossrep.acctpref_qualifier + dbname+
        ".information_schema.tables where table_owner is not null and row_count > 0 and row_count is not null and TABLE_TYPE = 'BASE TABLE' order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME ")
    if crossrep.verbose == True:
       print(query)
    # split(object_name,'.')[0] dbname, split(object_name,'.')[1] scname
    cursor.execute(query )
    rec = cursor.fetchall()
    ### spliting the output into 4 different files based on table data size:
    ### SMALL : < 2GB
    ### LARGE : [2GB, 15GB)
    ### XLARGE : [15GB, 30GB)
    ### XXLARGE : > 30GB
    count_small = 0
    cns = 1
    count_large = 0
    cnl = 1
    count_xlarge = 0
    cnxl = 1
    count_2xlarge = 0
    cn2xl = 1
    for r in rec:
        dbname = crossrep.quoteID(r[0])
        scname = crossrep.quoteID(r[1])
        tbname = crossrep.quoteID(r[2])
        tbsize_gb = r[3]

        stage_folder = '@' + crossrep.default_db +'.'+crossrep.default_sc +'.'+ stgname + '/'+dbname+'/'+scname+'/'+tbname+'/'
    
        # 18 (small wh) or 8 or 4 (2xl) jobs in each batch unloading/loading file
        if tbsize_gb < 2:
            count_small += 1
            if  count_small >= 17:
                count_small = 1
                cns += 1
            ufile = ufile_pref + dbname+"_small_"+ str(cns)+ ".sql"
            lfile = lfile_pref + dbname+"_small_"+ str(cns)+ ".sql"
        elif tbsize_gb >=2 and tbsize_gb < 15:
            count_large += 1
            if  count_large >= 9:
                count_large = 1
                cnl += 1
            ufile = ufile_pref + dbname+"_large_"+ str(cnl)+ ".sql"
            lfile = lfile_pref + dbname+"_large_"+ str(cnl)+ ".sql"            
        elif tbsize_gb >=15 and tbsize_gb < 30:
            count_xlarge += 1
            if  count_xlarge >= 5:
                count_xlarge = 1
                cnxl += 1
            ufile = ufile_pref + dbname+"_xlarge_"+ str(cnxl)+ ".sql"
            lfile = lfile_pref + dbname+"_xlarge_"+ str(cnxl)+ ".sql"            
        else:
            # tbsize_gb >= 30:
            count_2xlarge += 1
            if  count_2xlarge >= 5:
                count_2xlarge = 1
                cn2xl += 1
            ufile = ufile_pref + dbname+"_2xlarge_"+ str(cn2xl)+ ".sql"
            lfile = lfile_pref + dbname+"_2xlarge_"+ str(cn2xl)+ ".sql"            

        uf = open(ufile,"a+") 
        lf = open(lfile,"a+") 
        if crossrep.verbose == True:
            print("unloading file:" + ufile )
            print("loading file:" + lfile )
        unload_query = ('copy into ' + stage_folder + ' from ' + dbname+'.'+scname+'.'+tbname + 
        ' FILE_FORMAT = (FORMAT_NAME=\''+crossrep.default_db +'.'+crossrep.default_sc +'.'+ffname+'\') MAX_FILE_SIZE=160000000 HEADER=TRUE;\n')
        uf.write(unload_query)
        uf.close()
        
        load_query = ('copy into ' + dbname+'.'+scname+'.'+tbname + ' from ' + stage_folder + 
        ' FILE_FORMAT = (FORMAT_NAME=\''+crossrep.default_db +'.'+crossrep.default_sc +'.'+ffname+'\') ;\n')
        lf.write(load_query)
        lf.close()

    if crossrep.verbose == True:
        print('Finish generating unloading data copy statement ... ')

### multi-threading to speed up the comparison process on row count and hash_agg output 
def valCountHash(dblist, ofile):   
    # getting connections to both source and target accounts
    if crossrep.mode == 'CUSTOMER':
        source_ctx = crossrep.getConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_pwd, crossrep.default_wh, crossrep.default_rl)
        src_cursor = source_ctx.cursor()
        sdb = crossrep.getEnv('SRC_CUST_DATABASE')
        ssc = crossrep.getEnv('SRC_CUST_SCHEMA')
        src_cursor.execute("CREATE DATABASE IF NOT EXISTS "+sdb)
        src_cursor.execute("USE DATABASE "+sdb)
        src_cursor.execute("CREATE SCHEMA IF NOT EXISTS "+ssc)
        src_cursor.execute("USE SCHEMA "+ssc)

    else:
        print('only validating in CUSTOMER mode')
        return  

    tusr = crossrep.getEnv('TGT_CUST_USER')
    tpwd = crossrep.getEnv('TGT_CUST_PWD')
    tacct = crossrep.getEnv('TGT_CUST_ACCOUNT')
    tpwd = crossrep.getEnv('TGT_CUST_PWD')
    trl = crossrep.getEnv('TGT_CUST_ROLE')
    twh = crossrep.getEnv('TGT_CUST_WAREHOUSE')
    tdb = crossrep.getEnv('TGT_CUST_DATABASE')
    tsc = crossrep.getEnv('TGT_CUST_SCHEMA')

    tgt_ctx = crossrep.getConnection(tacct, tusr, tpwd, twh, trl)
    tgt_cursor = tgt_ctx.cursor()
    tgt_cursor.execute("CREATE DATABASE IF NOT EXISTS "+tdb)
    tgt_cursor.execute("USE DATABASE "+tdb)
    tgt_cursor.execute("CREATE SCHEMA IF NOT EXISTS "+tsc)
    tgt_cursor.execute("USE SCHEMA "+tsc)

    dblist = ','.join("'"+obj+"'" for obj in dblist )
    inPredicate = " and TABLE_CATALOG in (" + dblist + " )" 
         
    query = ("select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ROW_COUNT from snowflake.account_usage.tables  " +
        " where table_owner is not null and deleted is null  " + inPredicate + " and TABLE_TYPE = 'BASE TABLE' order by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME ")

    print("Query:" + query)
    src_cursor.execute(query)
    rec = src_cursor.fetchall()
    for r in rec:
        dbname = r[0]
        scname = r[1]  
        tbname = r[2]
        #validateByTable(dbname, scname, tbname, ofile, source_ctx, tgt_ctx)
        # Process each table validation in parallelized batches...
        t1 = threading.Thread(target=validateByTable, args=(dbname, scname, tbname, ofile, source_ctx, tgt_ctx))
        t1.setDaemon(True)
        t1.start()

    
    main_thread = threading.currentThread()
    for t in threading.enumerate():
        if t is main_thread:
            continue
        if crossrep.verbose:
            print('Completed %s', t.getName())
        t.join()

    src_cursor.close()
    source_ctx.close()
    tgt_ctx.close()
    if crossrep.verbose == True:
        print ('Finishing validating ELT data ' )

def validateByTable(dbname, scname, tbname, ofile, source_ctx, tgt_ctx):  
    source_cursor = source_ctx.cursor()
    swh = crossrep.getEnv('SRC_CUST_WAREHOUSE')
    source_cursor.execute("USE WAREHOUSE "+swh)

    '''
    source_cursor.execute("USE DATABASE "+crossrep.default_db)
    source_cursor.execute("USE SCHEMA "+crossrep.default_sc)
    '''
    target_cursor = tgt_ctx.cursor()
    twh = crossrep.getEnv('TGT_CUST_WAREHOUSE')
    target_cursor.execute("USE WAREHOUSE "+twh)

    squery = ("select ROW_COUNT from " + dbname+ ".information_schema.tables  " +
        " where TABLE_CATALOG='" + dbname + "' and TABLE_SCHEMA='" + scname+"' and TABLE_NAME='" + tbname + "' and table_owner is not null " )
    target_cursor.execute(squery)
    rows = target_cursor.fetchall()
    for r in rows:
        tgt_rc = r[0]

    source_cursor.execute(squery)
    rowset = source_cursor.fetchall()
    for row in rowset:
        src_rc = row[0]
        if tgt_rc != src_rc:
            ofile.write(" Table "+dbname+"."+scname+"."+tbname + ": source count => " + str(src_rc) + "; target count => " + str(tgt_rc) +"\n")

    hquery = ("select hash_agg(*) from \"" +dbname+"\".\""+scname+"\".\""+tbname + "\"")
    print(hquery)
    target_cursor.execute(hquery)
    record = target_cursor.fetchall()
    for row in record:
        thash = row[0]
    
    source_cursor.execute(hquery)
    rowset = source_cursor.fetchall()
    for onerow in rowset:
        shash = onerow[0]
        if thash != shash:
            ofile.write(" Table "+dbname+"."+scname+"."+tbname + ": source hash => " + str(shash) + "; target hasg => " + str(thash) +"\n")
    
    source_cursor.close()
    target_cursor.close()
