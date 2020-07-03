#!/usr/bin/env python3
"""
Created on Oct 1 2018
Updated on July 2nd 2020
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'

import os, re, string, subprocess, sys, getpass, random
import snowflake.connector
import crossrep

# *********************************************************************************************************************
# 
# This module contains the Snowflake-specific logic of creating replication commands.
#   The following SQL script files are generated :
#  - all_shares, all_warehouses, all_resmonitors, all_networkpolicies
#  - their DDLs that can be generated , then to be executed in target system are as follows:
#   scripts/rep/b1_alter_replica_dbs.sql        => alter database commands to enable replicatioin to target accounts
#   scripts/rep/b1_alter_failover_dbs.sql       => alter database commands to enable failover to target accounts
#   scripts/rep/b2_create_standby_global.sql    => create replica databases for target account
#   scripts/rep/b3_refresh_all_global.sql       => create refresh statement to refresh replicated database for target account
#   scripts/rep/b4_monitor_last_refresh.sql     => create monitor query to monitor replication progress 
#   scripts/rep/b5_switchover_to_standby.sql    => create failover query to be executed on target to switch over replica database as primary databse.
#   
#   The following metadata table will be created in local table to store metadata with global variable:
#    tb_gldb = 'ALL_GLOBAL_DBS'
#
# *********************************************************************************************************************

# generated alter database statement for replication/failover enablement 
def alterAllDBs(ofile, pfile,ralist,falist, cursor):
    # check whether table exists
    query = ("select distinct DATABASE_NAME from " + crossrep.tb_db )
    if crossrep.verbose == True:
        print(query)
    try:
        cursor.execute(query)
        rec = cursor.fetchall()
        for r in rec:
            if crossrep.verbose == True:
                print ('db name: '+ r[0])
            if crossrep.isBlank(r[0]) == False:
                ofile.write("alter database \"" + r[0] + "\" enable replication to accounts " + ralist +";\n")    
                pfile.write("alter database \"" + r[0] + "\" enable failover to accounts " + falist +";\n")     
    except Exception as err:
        print('An error occurred during alter database to enable replication:' + str(err))
        print('\n' + query)

# create a table to store all global database information
def crGlobalDBs( cursor):
    tbname = crossrep.tb_gldb
    if crossrep.verbose == True:
        print(tbname)
    if crossrep.mode == 'SNOWFLAKE':
        acctprefix = "in account " + crossrep.acctpref 
    elif crossrep.mode == 'CUSTOMER':
        acctprefix = ''
    try:
        cursor.execute("begin")
        cursor.execute("show replication databases " + acctprefix)
        cursor.execute("create or replace table " + tbname+" as select \"name\"  name,  \"snowflake_region\" snowflake_region,\"account_name\" account_name ,\"primary\" primary, \"is_primary\" is_primary, \"replication_allowed_to_accounts\" replication_allowed_to_accounts, \"failover_allowed_to_accounts\" failover_allowed_to_accounts  from table(result_scan(last_query_id()))") 
        cursor.execute("commit")
    except Exception as err:
        print('An error occurred during create replication database :' + str(err))

# create and link global databases to replication group so they becomes stand-by databases 
def linkGlobalDBsRepGroup( opt, ofile2, ofile3, ofile4, ofile5,ofile6,cursor):

    if opt == 'all' or crossrep.isBlank(opt):
        inPred = ''
    else:
        dfile = crossrep.getEnv('MIGRATION_HOME') + opt
        link_dblist = crossrep.readFile(dfile)
        inPred = crossrep.genInPredicate('name', link_dblist)

    query = ("select distinct name, snowflake_region, account_name from " + crossrep.tb_gldb + " where is_primary = 'true' " + inPred + " order by name ")
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        rep_region = r[1]
        acct_name = r[2]
        #print ('db name: '+ r[0])
        #cquery = " CREATE DATABASE \"" + name + "\" REPLICATION GROUP '" + rep_group + "'"

        cquery = ( " CREATE DATABASE \"" + name + "\" as replica of " + rep_region + "."+ acct_name+ ".\"" + name + "\"" +
        " auto_refresh_materialized_views_on_secondary = true;\n")        
        if crossrep.verbose == True:
            print (cquery)
        
        try:
            ofile2.write(cquery + ";\n")     

            aquery = " ALTER  DATABASE \"" + name + "\" REFRESH "
            if crossrep.verbose == True:
                print (cquery)
            ofile3.write(aquery + ";\n")     

            # Generate SQL to monitor Replication Progress
            mquery = ("with top_monitor as ( "+
                " select '"+ name +"' as dbname, f.value:phaseName::string as Phase, f.value:resultName::string as Result, to_timestamp_ltz(f.value:startTimeUTC::numeric,3) as startTime,  "+
                " NVL(to_timestamp_ltz(f.value:endTimeUTC::numeric,3),CURRENT_TIMESTAMP()) as endTime "+
                " from lateral flatten(input=> parse_json(system$database_refresh_progress('"+ name +"'))) f) "+
                ", detail_monitor as ( "+
                "    select '"+ name +"' as dbname, f.value:phaseName::string as Phase "+
                ", (d.value/1024/1024/1024/1024) tb_bytes "+
                " from lateral flatten(input=> parse_json(system$database_refresh_progress('"+ name +"'))) f "+
                ", lateral flatten(input=> parse_json(f.value:details)) d " +
                " where phase = 'Copying Primary Data' ) " +
                " select tm.dbname, timediff(seconds, min(tm.startTime), max(tm.endTime))/60/60 as exec_hours ,max(dm.tb_bytes) total_copy_tbbytes , min(dm.tb_bytes) completed_copy_tbbytes, 100*completed_copy_tbbytes/total_copy_tbbytes percent "+ 
                " from top_monitor tm left join detail_monitor dm " +
                " on tm.dbname = dm.dbname group by tm.dbname " )
            if crossrep.verbose == True:
                print(mquery)

            ofile4.write(mquery + ";\n")    
            
            squery = " ALTER  DATABASE \"" + name + "\" PRIMARY "
            ofile5.write(squery + ";\n")   

            sfquery = ("with top_monitor as ( "+
                " select '"+ name +"' as dbname, f.value:phaseName::string as Phase, f.value:resultName::string as Result, to_timestamp_ltz(f.value:startTimeUTC::numeric,3) as startTime,  "+
                " NVL(to_timestamp_ltz(f.value:endTimeUTC::numeric,3),CURRENT_TIMESTAMP()) as endTime "+
                " from lateral flatten(input=> parse_json(system$database_refresh_progress('"+ crossrep.acctpref_qualifier+name +"'))) f) "+
                ", detail_monitor as ( "+
                "    select '"+ name +"' as dbname, f.value:phaseName::string as Phase "+
                ", (d.value/1024/1024/1024/1024) tb_bytes "+
                " from lateral flatten(input=> parse_json(system$database_refresh_progress('"+ crossrep.acctpref_qualifier+name +"'))) f "+
                ", lateral flatten(input=> parse_json(f.value:details)) d " +
                " where phase = 'Copying Primary Data' ) " +
                " select tm.dbname, timediff(seconds, min(tm.startTime), max(tm.endTime))/60/60 as exec_hours ,max(dm.tb_bytes) total_copy_tbbytes , min(dm.tb_bytes) completed_copy_tbbytes, 100*completed_copy_tbbytes/total_copy_tbbytes percent "+ 
                " from top_monitor tm left join detail_monitor dm " +
                " on tm.dbname = dm.dbname group by tm.dbname " )
            if crossrep.verbose == True:
                print(sfquery)

            ofile6.write(sfquery + ";\n") 
        except Exception as err:
            print('An error occurred during alter database to enable replication for database:' + name + '; ' + str(err)) 
            print('\n refreshing query:' + aquery)
            print('\n monitoring query:' + mquery)
            print('\n switching to primary query:' + squery)
            print('\n monitoring query with snowflake version:' + sfquery)

    # Generate query to monitor Replication Progress
    cquery = ( " select database_name, credits_used,  bytes_transferred/1024/1024/1024/1024 as tbs_transferred, TIMEDIFF('SECOND', END_TIME, START_TIME) " + 
        "from table( information_schema.replication_usage_history(" + 
        "date_range_start=>dateadd(d, -7, current_date),"+
        "date_range_end=>current_date)) order by 1 ") 
    ofile4.write(cquery + ";\n")

# refresh global databases to replication group so it starts replicating stand-by databases 
# Generate query to monitor Replication Progress 
# Generate query to switchover to Standby for all global databases
def refreshGlobalDBs( ofile3, ofile4,ofile5,cursor):
    query = ("select distinct name from " + crossrep.tb_gldb + " where is_primary = 'false' order by name")
    print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        #print ('db name: '+ r[0])
        cquery = " ALTER  DATABASE \"" + name + "\" REFRESH "
        #print (cquery)
        ofile3.write(cquery + ";\n")     

        # Generate DDL to monitor Replication Progress
        mquery = ( " select '"+ name +"' as dbname, value:phaseName::string as Phase," +
            " value:resultName::string as Result," +
            " to_timestamp_ltz(value:startTimeUTC::numeric,3) as startTime," +
            " to_timestamp_ltz(value:endTimeUTC::numeric,3) as endTime," +
            " datediff(mins, startTime, endTime) as Duration" +
            " from table(flatten(input=> parse_json(system$database_refresh_progress('"+ name +"'))))" )
        ofile4.write(mquery + ";\n")    
        squery = " ALTER  DATABASE \"" + name + "\" PRIMARY "
        ofile5.write(squery + ";\n")   

### executing monitor queries on target account in SNOWFLAKE or CUSTOMER mode, will execute in PROD with SNOWFLAKE mode, in customer's account in CUSTOMER mode
### will create a monitoring table TB_MONITOR in the connecting DB/SC
# name: database name to be monitored
# tbname: table name for storing the monitoring information
#def repMonitor(name, tbname, cursor):
def repMonitor(name, tbname, ofile):
    #tbname = 'TB_MONITOR'
    #isCreated = False 

    if crossrep.mode == 'CUSTOMER':
        mquery = ("with top_monitor as ( "+
            " select '"+ name +"' as dbname, f.value:phaseName::string as Phase, f.value:resultName::string as Result, to_timestamp_ltz(f.value:startTimeUTC::numeric,3) as startTime,  "+
            " NVL(to_timestamp_ltz(f.value:endTimeUTC::numeric,3),CURRENT_TIMESTAMP()) as endTime "+
            " from lateral flatten(input=> parse_json(system$database_refresh_progress('"+ name +"'))) f) "+
            ", detail_monitor as ( "+
            "    select '"+ name +"' as dbname, f.value:phaseName::string as Phase "+
            ", (d.value/1024/1024/1024/1024) tb_bytes "+
            " from lateral flatten(input=> parse_json(system$database_refresh_progress('"+ name +"'))) f "+
            ", lateral flatten(input=> parse_json(f.value:details)) d " +
            " where phase = 'Copying Primary Data' ) " +
            " select tm.dbname, timediff(seconds, min(tm.startTime), max(tm.endTime))/60/60 as exec_hours ,max(dm.tb_bytes) total_copy_tbbytes , min(dm.tb_bytes) completed_copy_tbbytes, 100*completed_copy_tbbytes/total_copy_tbbytes percent "+ 
            " from top_monitor tm left join detail_monitor dm " +
            " on tm.dbname = dm.dbname group by tm.dbname " )
    elif crossrep.mode == 'SNOWFLAKE':
        mquery = ("with top_monitor as ( "+
            " select '"+ name +"' as dbname, f.value:phaseName::string as Phase, f.value:resultName::string as Result, to_timestamp_ltz(f.value:startTimeUTC::numeric,3) as startTime,  "+
            " NVL(to_timestamp_ltz(f.value:endTimeUTC::numeric,3),CURRENT_TIMESTAMP()) as endTime "+
            " from lateral flatten(input=> parse_json(system$database_refresh_progress('"+ crossrep.acctpref_qualifier+name +"'))) f) "+
            ", detail_monitor as ( "+
            "    select '"+ name +"' as dbname, f.value:phaseName::string as Phase "+
            ", (d.value/1024/1024/1024/1024) tb_bytes "+
            " from lateral flatten(input=> parse_json(system$database_refresh_progress('"+ crossrep.acctpref_qualifier+name +"'))) f "+
            ", lateral flatten(input=> parse_json(f.value:details)) d " +
            " where phase = 'Copying Primary Data' ) " +
            " select tm.dbname, timediff(seconds, min(tm.startTime), max(tm.endTime))/60/60 as exec_hours ,max(dm.tb_bytes) total_copy_tbbytes , min(dm.tb_bytes) completed_copy_tbbytes, 100*completed_copy_tbbytes/total_copy_tbbytes percent "+ 
            " from top_monitor tm left join detail_monitor dm " +
            " on tm.dbname = dm.dbname group by tm.dbname " )

    #cursor.execute('begin')
    #cursor.execute(mquery)
    iquery = ("INSERT INTO " + tbname + "  (DBNAME, EXEC_HOURS, TOTAL_COPY_TBBYTES, COMPLETED_COPY_TBBYTES, PERCENT, CURRENT_TS) " 
        + " select DBNAME, EXEC_HOURS, TOTAL_COPY_TBBYTES, COMPLETED_COPY_TBBYTES, PERCENT, current_timestamp() "
        + " from  table(result_scan(last_query_id()))")
    #cursor.execute('commit')
    ofile.write(iquery + ';\n\n')

