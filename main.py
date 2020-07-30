#!/usr/bin/env python3
"""
Created on Oct 1 2018
Updated on July 2nd 2020
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'

import argparse
import snowflake.connector
import os, re, string, subprocess, sys, time,getpass
import crossrep
from multiprocessing import Process
import scriptloader

def setDefaultEnv(mode):
    if mode == 'CUSTOMER':
        crossrep.default_usr = crossrep.getEnv('SRC_CUST_USER')
        crossrep.default_acct = crossrep.getEnv('SRC_CUST_ACCOUNT')
        crossrep.default_pwd = crossrep.getEnv('SRC_CUST_PWD')
        crossrep.default_rl = crossrep.getEnv('SRC_CUST_ROLE')
        crossrep.default_wh = crossrep.getEnv('SRC_CUST_WAREHOUSE')
        crossrep.default_db = crossrep.getEnv('SRC_CUST_DATABASE')
        crossrep.default_sc = crossrep.getEnv('SRC_CUST_SCHEMA')
        crossrep.acctpref = ''
        crossrep.acctpref_qualifier = ''
    elif mode == 'SNOWFLAKE':
        crossrep.default_usr = crossrep.getEnv('SRC_PROD_USER')
        crossrep.default_acct = crossrep.getEnv('SRC_PROD_ACCOUNT')
        crossrep.default_rl = crossrep.getEnv('SRC_PROD_ROLE')
        crossrep.default_wh = crossrep.getEnv('SRC_PROD_WAREHOUSE')
        crossrep.default_db = crossrep.getEnv('SRC_PROD_DATABASE')
        crossrep.default_sc = crossrep.getEnv('SRC_PROD_SCHEMA')
    elif mode == 'SNOWHOUSE':
        #print('snowhouse mode')
        crossrep.default_usr = crossrep.getEnv('SNOWHOUSE_USER')
        crossrep.default_acct = crossrep.getEnv('SNOWHOUSE_ACCOUNT')
        crossrep.default_rl = crossrep.getEnv('SNOWHOUSE_ROLE')
        crossrep.default_wh = crossrep.getEnv('SNOWHOUSE_WAREHOUSE')
        crossrep.default_db = crossrep.getEnv('SNOWHOUSE_DATABASE')
        crossrep.default_sc = crossrep.getEnv('SNOWHOUSE_SCHEMA')
    elif mode == 'DR':
        crossrep.default_usr = crossrep.getEnv("TGT_CUST_USER")
        crossrep.default_acct = crossrep.getEnv("TGT_CUST_ACCOUNT")
        crossrep.default_pwd = crossrep.getEnv('TGT_CUST_PWD')
        crossrep.default_rl = crossrep.getEnv('TGT_CUST_ROLE')
        crossrep.default_wh = crossrep.getEnv('TGT_CUST_WAREHOUSE')
        crossrep.default_db = crossrep.getEnv('TGT_CUST_DATABASE')
        crossrep.default_sc = crossrep.getEnv('TGT_CUST_SCHEMA')

##### MAIN #####
parser = argparse.ArgumentParser(description='Fix grants generation by grant ownership and grant PRIVILEGES.',
    epilog='Example: python main.py -a account-name')
parser.add_argument('-a', '--account',required=False,
    help='customer account to replicate from ')
parser.add_argument('-b', '--beyond',action='store_true', default=False,
    help=' generating account level privileges grant commands, such as CREATE WWAREHOUSE etc. ')

parser.add_argument('-c', '--create', type=str,
    help='create tables to store metadata, first time run to create or replace existing metadata table, option 1, 2, 3, 4, 5,6 or all !!! ')

### generate grants of ownership and privileges for databases with input parm: file name or 'all' or nothing(default) of Database
parser.add_argument('-d', '--dbfile',  type=str,
    help='a file name with database lists to grant or all databases (all) or nothing ')

#parser.add_argument('-ddl', '--ddl',action='store_true', default=False,
parser.add_argument('-ddl', '--ddl',type=str,
    help='generating database object DDL with database lists to grant or all databases (all) ')
parser.add_argument('-elt', '--elt',type=str,
    help='generating table elt jobs ')

parser.add_argument('-dis', '--discover',action='store_true', default=False,
    help=' discover account statistics, # of users, roles, objects etc... ')


parser.add_argument('-e', '--evaluate',type=str, 
    help='evalute constraints/sequences in account with version: 1,2,...')

parser.add_argument('-sh', '--snowhouse',type=str,
    help='evalute constraints/sequences in snowhouse with database name')
#parser.add_argument('-e', '--evaluate',action='store_true', default=False,
#    help='evalute constraints/sequences in account ')

parser.add_argument('-f', '--future', action='store_true', default=False,
    help='generate future grants: True or False ?')

parser.add_argument('-fr', '--freeze',action='store_true', default=False,
    help=' In order to freeze account , generating command to disable all users or suspend warehouse . ')

parser.add_argument('-g', '--globaldb', type=str,
    help='alter databases to global for replication: all or a file name with database list ?')

parser.add_argument('-i', '--integration', type=str,
    help='getting ddl of integrations with deployment options: VA, PROD1, PROD2 etc ')

parser.add_argument('-l', '--link', type=str,
    help='linking standby databases with primary: version control - 1,2,... ')

parser.add_argument('-m', '--mode', type=str,
    help='specifying 1 of 3 execution modes: CUSTOMER or SNOWFLAKE or SNOWHOUSE (UPPERCASE ONLY) ')

parser.add_argument('-mon', '--monitor', type=str,
    help='monitoring replication progress : CUSTOMER or SNOWFLAKE mode, input dbfile name  ')

acctobj = ['WAREHOUSE', 'NETWORK_POLICY', 'RESOURCE_MONITOR']
parser.add_argument('-o', '--objlist', nargs='+', 
    help='specify account objects from list (upper-case only): %s' %(str(acctobj)))
parser.add_argument('-p', '--parameter', action='store_true', default=False,
    help='generate account level parameters which are different from the default value : True or False ? ')

parser.add_argument('-pipe', '--pipe',type=str,
    help='create DDL for pipe and grants on pipes ')

# with 3 options on create users: nopwd (no password), samepwd (same password - cr0ss2REP), randpwd (random pwd)
parser.add_argument('-r', '--role', type=str, 
    help='generate SQL for users/roles creation and their relationship grants : nopwd, samepwd, randpwd  ? ')

parser.add_argument('-stage', '--stage',type=str,
    help='create DDL for stage and grants on stages ')


# user name - case sensitive
parser.add_argument('-t', '--test', nargs='+', 
    help='generate DROP statement for testing purpose : user name ? ')

## True (default): will create user tables to store account usage data which will be replicated to target system
## False: will ignore account usage data
parser.add_argument('-u', '--acctusage', action='store_true', default=False,
    help='Need account usage history data: True or False ?')

parser.add_argument('-v', '--verbose', action='store_true', 
    help='printing more diagnotic information ')
parser.add_argument('-val', '--validate', type=str,
    help='validating for row count and hash agg on tables with version : version control - 1,2,... ')

parser.add_argument('-w', '--warehouse', action='store_true', 
    help='Suspend all warehouses ')

### user provide database lists for grants against
### no -l option will generate grants for all database
### -l with a list (space as delimiter), will generate grants for those databases in the list
### -l with nothing, will not generate any grants for any database objects

args=parser.parse_args()

olist = args.objlist
#dlist = args.dblist
#db = args.priv
dbfile = args.dbfile
gldb = args.globaldb
# database list to alter database global
gf_dblist = []
# database list to grant ownership and privileges 
df_dblist = []
share_dblist = []
excludePredicate = ''


migHome = crossrep.getEnv('MIGRATION_HOME')

if args.verbose:
    crossrep.verbose = True
    print('verbose')

#print('mode: '+ crossrep.mode + '; args.mode:' + args.mode) 
if args.mode != None and args.mode != crossrep.mode :
    crossrep.mode = args.mode
    
setDefaultEnv(crossrep.mode) 
print(crossrep.default_acct)
print(crossrep.default_usr)
print(crossrep.default_wh)
print(crossrep.default_rl)
print('mode: '+ crossrep.mode ) 

## passing in account in command override account in the env_xxx.sh file
if args.account != None and crossrep.isBlank(args.account) == False:
    crossrep.default_acct = args.account

if crossrep.mode == 'SNOWHOUSE':
    ctx = snowflake.connector.connect(
            account=crossrep.default_acct,
            user=crossrep.default_usr,
            authenticator='externalbrowser',
            warehouse=crossrep.default_wh,
            role=crossrep.default_rl 
        )
elif crossrep.mode == 'SNOWFLAKE':
    ctx = crossrep.getSFConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_wh, crossrep.default_rl)
elif crossrep.mode == 'CUSTOMER':
    ctx = crossrep.getConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_pwd, crossrep.default_wh, crossrep.default_rl)
elif crossrep.mode == 'DR':
    ctx = crossrep.getConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_pwd, crossrep.default_wh, crossrep.default_rl)

if crossrep.mode == 'DR_TEST':
    cursor = None
else:
    cursor = ctx.cursor()

if crossrep.mode == 'DR' or crossrep.mode == 'DR_TEST':
    filelist  = scriptloader.list_scripts(migHome, "ddl")
    failed_statements = []
    for f in filelist:
        scriptloader.upload_scripts(crossrep.mode, f, cursor, failed_statements)

    if crossrep.mode == 'DR':
        remaining_failures = scriptloader.retry_failed_statements(cursor, failed_statements)
    else:
        remaining_failures = failed_statements
    for item in remaining_failures:
        print("\n-------DDL Statement failed and cannot be retried  ---->")
        print(item["statement"])
        print("\n------- Error: " + item["error"])

    if cursor != None:
        cursor.close()

    sys.exit()


if crossrep.mode == 'CUSTOMER':
    cursor.execute("CREATE DATABASE IF NOT EXISTS "+crossrep.default_db)
    cursor.execute("USE WAREHOUSE "+crossrep.default_wh)
cursor.execute("USE DATABASE "+crossrep.default_db)
if crossrep.mode == 'CUSTOMER' or crossrep.mode == 'SNOWFLAKE' or crossrep.mode == 'DR':
    cursor.execute("CREATE SCHEMA IF NOT EXISTS "+crossrep.default_sc)

cursor.execute("USE SCHEMA "+crossrep.default_sc)
#cursor.execute("USE WAREHOUSE "+wh)

### -u option: create a list of user tables to store account_usage data in source snowflake account, run by customer
if args.acctusage:
    dbau = crossrep.getEnv('AU_DATABASE')
    scau = crossrep.getEnv('AU_SCHEMA')

    try:
        crossrep.crAccountUsage(dbau, scau, cursor)
    except Exception as err:
        print('An error occured during creating tables for account_usage :' + str(err) )
        #pass

if args.create:
    # providing multiple options so we can run it in parallel with separate call on diff wh
    # 'all' cases splits into 5 cases , case 1, 2, 3 can be run in parallel, 4 and 5 needs to wait till 2 finishes and 
    # can be executed in parallel, 6 runs after 5
    coption = args.create
    try:
        if crossrep.verbose:
            print('collecting metadata ... ')
        # RBAC only: user, roles and privileges
        if coption == '0':
            crossrep.crRoles(cursor)
            crossrep.crUsers(cursor)
            crossrep.crParentPriv(cursor)
            # crossrep.crParent( cursor)
            # crossrep.crPriv( cursor)

            '''
        if coption == '1':
            crossrep.crRoles( cursor)
            crossrep.crUsers( cursor)
        elif coption == '2':
            crossrep.crWarehouse( cursor)
            crossrep.crResMonitor(cursor)
            if crossrep.mode == 'CUSTOMER':
                crossrep.crNetworkPolicy(cursor)
        elif coption == '3':    
            crossrep.crShares( cursor) 
            crossrep.crDatabase (cursor)  
            if crossrep.mode == 'SNOWFLAKE':
                crossrep.crSchema ( cursor)             
        elif coption == '4' or coption == '5':
            #crossrep.crParent( cursor)    
            crossrep.crParentPriv (cursor)      
        #elif coption == '5':
        #    crossrep.crPriv( cursor)
        elif coption == '6':    
            crossrep.crTable_DefaultSequence ( cursor)
            
            if crossrep.mode == 'SNOWFLAKE':
                crossrep.crTable_Constraints( cursor)
            '''

        elif coption == 'all':
            crossrep.crWarehouse(cursor)
            crossrep.crResMonitor(cursor)
            crossrep.crRoles(cursor)
            crossrep.crUsers(cursor)
            #crossrep.crParent( cursor)
            crossrep.crParentPriv( cursor)
            #crossrep.crPriv( cursor)
            crossrep.crShares( cursor)
            crossrep.crDatabase ( cursor)
            crossrep.crTable_DefaultSequence ( cursor)
            
            if crossrep.mode == 'CUSTOMER':
                crossrep.crNetworkPolicy( cursor)
            if crossrep.mode == 'SNOWFLAKE':
                crossrep.crSchema (  cursor)
                crossrep.crTable_Constraints( cursor)

    except Exception as err:
        print('An error occured during creating metadata tables :' + str(err) )
        #pass

## -b option: grant all privilege on account
if args.beyond :
    ### grant PRIVILEGES on ACCOUNT
    ofile = open(migHome + "scripts/acctobj/14_grant_acct_level_privs.sql","w")
    try:
        crossrep.grantAcctPrivs(ofile, cursor)
    except Exception as err:
        print('An error occured during generating grant privilege on ACCOUNT :' + str(err) )
    ofile.close()

# -d option to generate grants of ownerships and privileges on database level objects
if dbfile :
    ### grant ownership on  database level objects 
    if len(share_dblist) == 0:
        share_dblist = crossrep.getShareDB( cursor)
        if crossrep.mode == 'CUSTOMER':
            # excluding connection database that's created for storing metadata, no need to generate grants on this DB
            share_dblist.append(crossrep.default_db)
    if crossrep.isBlank(excludePredicate) == True:
        excludePredicate = crossrep.genExcludePredicate(share_dblist)
        if len(crossrep.unsupported_Obj_list) != 0:
            Obj_inlist = ','.join("'"+ otype +"'" for otype in crossrep.unsupported_Obj_list )
            excludePredicate = excludePredicate + " and object_type not in (" + Obj_inlist + ") "

    if dbfile == 'all':
        of = open(migHome + "scripts/rbac/25_grant_owner_dblevel.sql","w") 
        try:
            crossrep.grantAllOwners(excludePredicate, of, cursor)
        except Exception as err:
            print('An error occured during generating grant ownership file on db objects:' + str(err) )
            #pass
        of.close()
        
        ### grant PRIVILEGES on all database level objects
        pf = open(migHome + "scripts/rbac/26_grant_privs_dblevel.sql","w") 
        try:
            crossrep.grantAllPrivs( excludePredicate, pf, cursor)
        except Exception as err:
            print('An error occured during generating grant privilege files on db objects:' + str(err) )
            #pass
        pf.close()
    elif  crossrep.isBlank(dbfile) == False :
        try:
            off = open(migHome + "scripts/rbac/25_grantowner_dblevel_"+dbfile+".sql","w") 
            pff = open(migHome + "scripts/rbac/26_grantpriv_dblevel_"+dbfile+".sql","w") 
            if len(df_dblist) == 0:
                dfile = migHome + dbfile
                df_dblist = crossrep.readFile(dfile)
            for dbname in df_dblist:
                if (share_dblist == None) or (share_dblist != None and dbname not in share_dblist):
                    crossrep.grantOwnerByDatabase(dbname, off, cursor)
                    crossrep.grantPrivsByDatabase( dbname, pff, cursor)
            
            off.close()
            pff.close()
        except Exception as err:
            print('An error occured during generating grant by file on db objects:' + str(err) )
    else:
            print('no grants on database objects')

# -ddl option with 'all' or a file name containing database list, default to 'all' option
if args.ddl:   
    dbf = args.ddl   
    if crossrep.verbose:
        print('ddl option: '+dbf)
    try:
        file_pref = migHome + '/scripts/ddl/01_dbDDL_'

        if dbf == 'all' or crossrep.isBlank(dbf) == True :
            cursor.execute(" select distinct DATABASE_NAME from  "+ crossrep.tb_db + " minus select distinct db_name from "
                + crossrep.tb_share + " where db_name is not null and kind = 'INBOUND'")
            rec = cursor.fetchall()
            for r in rec:
                of = open(file_pref + r[0]+".sql","w")
                crossrep.genDatabaseDDL ( r[0], of, cursor)
                of.close()
        else:
            dbfilepath = migHome + dbf
            ddl_dblist = crossrep.readFile(dbfilepath)
            if crossrep.verbose:
                print('dbfile path/name: '+dbfilepath)
                print(ddl_dblist)
            for adb in ddl_dblist:
                of = open(file_pref + adb+".sql","w")
                crossrep.genDatabaseDDL ( adb, of, cursor)
                of.close()
    except Exception as err:
        print('An error occured during generating DDL for database :' + str(err) )

# -elt option with 'all' or a file name containing database list, default to 'all' option
if args.elt:  
    eltdb = args.elt   
    if crossrep.verbose:
        print('elt option: '+eltdb)   
    try:
        ufile_pref = migHome + 'scripts/elt/02_unload_'
        lfile_pref = migHome + 'scripts/elt/03_load_'

        if eltdb == 'all' or crossrep.isBlank(eltdb) == True :
            #cursor.execute(" select distinct DATABASE_NAME from  "+ crossrep.tb_db )
            cursor.execute(" select distinct DATABASE_NAME from  "+ crossrep.tb_db + " minus select distinct db_name from "
                + crossrep.tb_share + " where db_name is not null and kind = 'INBOUND'")
            rec = cursor.fetchall()
            for r in rec:
                crossrep.ELTByDatabase ( ufile_pref, lfile_pref, crossrep.stage_name, crossrep.fileformat_name, r[0], cursor)
                # ffname=mycsvformat stgname=sfcsupport_csv
                #ffname='mycsvformat'
                #stgname='sfcsupport_csv'
        else:
            dbfname = migHome + eltdb
            elt_dblist = crossrep.readFile(dbfname)
            if crossrep.verbose:
                print('dbfile path/name: '+dbfname)
                print(elt_dblist)
            for adb in elt_dblist:
                crossrep.ELTByDatabase ( ufile_pref, lfile_pref, crossrep.stage_name, crossrep.fileformat_name, adb, cursor)
    except Exception as err:
        print('An error occured during generating unloading/loading jobs :' + str(err) )

### -e option: a version number to generate a diff version file
if args.evaluate:     
    ver = args.evaluate
    try:
        
        ddl_file = open(migHome + "scripts/eval/a1_fk_DDL_"+ ver +".sql","w")
        drop_file = open(migHome + "scripts/eval/a1_drop_fk_"+ ver +".sql","w") 
        add_file = open(migHome + "scripts/eval/a1_add_fk_"+ ver + ".sql","w") 
        crossrep.repFKeys (ddl_file, drop_file, add_file, cursor)
        ddl_file.close()
        drop_file.close()
        add_file.close()
        
        drop_file = open(migHome + "scripts/eval/a2_drop_default_"+ ver +".sql","w") 
        alt_file = open(migHome + "scripts/eval/a2_alter_default_"+ ver +".sql","w") 
        crossrep.repAllSeqTable( drop_file, alt_file,cursor)
        drop_file.close()
        alt_file.close()
        
        # external table reports, drop all external tables (unsupported, needs to drop) 
        drop_file = open(migHome + "scripts/eval/a3_drop_extb_"+ ver +".sql","w") 
        ddl_file = open(migHome + "scripts/eval/a3_extb_DDL_"+ ver +".sql","w") 
        crossrep.repExTable ( drop_file, ddl_file,cursor)
        crossrep.grantByObjType('EXTERNAL_TABLE', ddl_file, cursor)
        ddl_file.close()
        drop_file.close()
        
        # MV reports 
        drop_file = open(migHome + "scripts/eval/a3_drop_MV_"+ ver + ".sql","w") 
        ddl_file = open(migHome + "scripts/eval/a3_crossdb_MVDDL_"+ ver + ".sql","w") 
        crossrep.repMVs (drop_file, ddl_file, cursor)
        crossrep.grantByObjType('MATERIALIZED_VIEW', ddl_file, cursor)
        drop_file.close()
        ddl_file.close()
        
    except Exception as err:
        print('An error occured during evaluating/reporting constraints/sequences/ext-table/MV :' + str(err) )
        #pass

# -f option: generating future grants
if args.future:
    ### grant future grants
    ff = open(migHome + "scripts/rbac/27_future_grants.sql","w") 
    try:
        crossrep.crFGrant(cursor)
        crossrep.grantFutureObj( ff, cursor)
    except Exception as err:
        print('An error occured during generating future grants :' + str(err) )
        #pass
    ff.close()

## -fr option: disable and enable all users, suspend or resume warehouses
if args.freeze:
    ofile1 = open(migHome + "scripts/43_disable_users.sql","w")
    ofile2 = open(migHome + "scripts/43_enable_users.sql","w")
    crossrep.disUsers( ofile1, ofile2, cursor)
    ofile1.close()
    ofile2.close()

    ## -w option: suspend or resume warehouses
    ofile1 = open(migHome + "scripts/42_suspend_warehouses.sql","w")
    ofile2 = open(migHome + "scripts/42_resume_warehouses.sql","w")
    crossrep.susWarehouse( ofile1, ofile2, cursor)
    ofile1.close()
    ofile2.close()

#### to generate ALTER DATABASE ENABLE REPLICATION statements
## -g option: all or database file with list of databases, default to all if empty option
if gldb != None:

    print('global db option:' + gldb)
    ### grant ownership on  database level objects 
    if crossrep.isBlank(gldb) == False:
        gfile = open(migHome + "scripts/rep/b1_alter_replica_dbs.sql","w") 
        lfile = open(migHome + "scripts/rep/b1_alter_failover_dbs.sql","w") 
        # get replication and failover enablement account list 
        ralist = crossrep.getEnv('REP_ACCT_LIST')
        falist = crossrep.getEnv('FAILOVER_ACCT_LIST')
        if gldb == 'all':           
            try:
                print('alter all databases to enable replication/failover')
                crossrep.alterAllDBs(gfile,lfile, ralist,falist, cursor)
            except Exception as err:
                print('An error occured during alter all databases into global:' + str(err) )
                #pass
        else   :
            if gldb == dbfile and len(df_dblist) != 0:
                gf_dblist = df_dblist
            else:
                dfile = migHome + gldb
                gf_dblist = crossrep.readFile(dfile)
            
            #gf_dblist = crossrep.readFile(gldb)
            try:
                for dbname in gf_dblist:
                    #gfile.write("ALTER DATABASE \"" + dbname + "\" GLOBAL;\n")
                    #lfile.write("ALTER DATABASE \"" + dbname + "\" LOCAL;\n")
                    gfile.write("alter database \"" + dbname + "\" enable replication to accounts " + ralist +";\n")    
                    lfile.write("alter database \"" + dbname + "\" enable failover to accounts " + falist +";\n")     

            except Exception as err:
                print('An error occured during generating ALTER DATABASE GLOBAL by file on db objects:' + str(err) )
        gfile.close()
    else:
        print('no database to alter to global')

## only for snowhouse [snowflake-internal only]
if args.integration :
    deployment = args.integration 
    if crossrep.isBlank(deployment) == True :
        deployment = crossrep.getEnv(SNOWHOUSE_DEPLOYMENT)

    of = open(migHome + "scripts/snowhouse/h0_create_integrations_DDL.sql","w") 
    query = ("select  UUID as queryid ,created_on as start_time, role_name, database_name, DESCRIPTION as sql from " 
        +  deployment + ".job_etl_v "
        + " where account_id  in ( " 
        + " select a.ID from VA.ACCOUNT_RAW_V a  " 
        + " where a.NAME =  '" + crossrep.getEnv('SNOWHOUSE_CUST_ACCOUNT') + "') " 
        + " and  upper(DESCRIPTION) LIKE '%INTEGRATION%' " 
        + " and  upper(DESCRIPTION) LIKE 'CREATE%' " 
        + " AND created_on > '2019-03-04 23:10:00'::timestamp_ntz  " 
        + " and error_code is null  " 
        + " order by created_on asc  " )
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        role= r[2]
        sql = r[4]
        of.write("use role " + role + ";\n")
        of.write(sql + ";\n")
    of.close()

# -l option with 'all' or dbfile that contains database list 
if args.link:
    opt = args.link
    print('option:' + opt)
    crossrep.crGlobalDBs( cursor)
    cfile = open(migHome + "scripts/rep/b2_create_standby_global.sql","w")
    rfile = open(migHome + "scripts/rep/b3_refresh_all_global.sql","w")
    mfile = open(migHome + "scripts/rep/b4_monitor_last_refresh.sql","w")
    wfile = open(migHome + "scripts/rep/b5_switchover_to_standby.sql","w")
    sffile = open(migHome + "scripts/rep/prod_monitor_last_refresh.sql","w")
    crossrep.linkGlobalDBsRepGroup( opt, cfile, rfile, mfile, wfile, sffile, cursor)
    cfile.close()
    rfile.close()
    mfile.close()
    wfile.close()
    sffile.close()

## -mon: executing monitoring queries on target account in PROD or customer account
if args.monitor :
    #tbname = 'TB_REPMONITOR'
    tbname = 'TB_MONITOR'
    dbmonfile = migHome + args.monitor
    dbmon_list = crossrep.readFile(dbmonfile)
    rf = open(migHome + "scripts/rep/monitor_report.sql","a") 
    
    cquery = ("create or replace TABLE "+ tbname + "(DBNAME string, EXEC_HOURS number, TOTAL_COPY_TBBYTES number, COMPLETED_COPY_TBBYTES number, PERCENT number, CURRENT_TS TIMESTAMP_LTZ(9)) ")
    rf.write(cquery + ';\n\n')
    #cursor.execute(cquery)
    #cursor.execute('commit')
    for dbname in dbmon_list:
        #crossrep.repMonitor(dbname, tbname, cursor)
        crossrep.repMonitor(dbname, tbname, rf)
    
    #squery = "select DBNAME, MAX(TOTAL_COPY_TBBYTES), MAX(TOTAL_COPY_TBBYTES/EXEC_HOURS ) COPY_RATE_TB_BY_HOUR from "+ tbname + " WHERE PERCENT = 100 GROUP BY DBNAME ORDER BY 3 DESC"
    squery = "select DBNAME, MAX(TOTAL_COPY_TBBYTES) TOTAL_COPY_TBBYTES, MAX(TOTAL_COPY_TBBYTES/EXEC_MINS ) COPY_RATE_TB_BY_HOUR from "+ tbname + " WHERE PERCENT = 100 GROUP BY DBNAME ORDER BY 3 DESC"
    rf.write(squery + ';\n\n')
    rf.close()


if olist :
    if len(olist) != 0:
        ### grant ownership on account level objects by object type list
        #objectTypelist = ['WAREHOUSE', 'NETWORK_POLICY', 'RESOURCE_MONITOR']
        # need separate option to create account level objects
        if 'RESOURCE_MONITOR' in olist:
            
            ### create resource monitors
            rf = open(migHome + "scripts/acctobj/11_creater_monitors_DDL.sql","w") 
            try:
                crossrep.genResMonitor(  rf, cursor)
                crossrep.grantByObjType('RESOURCE_MONITOR', rf, cursor)
            except Exception as err:
                print('An error occured during generating resource monitor DDL :' + str(err) )
                #pass
            rf.close()
        
        if 'NETWORK_POLICY' in olist:
            ### create network policies
            crossrep.crNetworkPolicy( cursor)
            nf = open(migHome + "scripts/acctobj/12_create_network_policies_DDL.sql","w") 
            try:
                crossrep.crNetworkPolicy( cursor)
                crossrep.genNetworkPolicyDDL(nf, cursor)
                crossrep.grantByObjType('NETWORK_POLICY', nf, cursor)
            except Exception as err:
                print('An error occured during creating network policy DDL :' + str(err) )
                #pass
            nf.close()
        
        if 'WAREHOUSE' in olist:
            ### create warehouses
            wf = open(migHome + "scripts/acctobj/13_create_warehouses_DDL.sql","w") 
            try:
                crossrep.genWarehouseDDL(wf, cursor)
                crossrep.grantByObjType('WAREHOUSE', wf, cursor)
            except Exception as err:
                print('An error occured during creating warehouse DDL :' + str(err) )
                #pass
            wf.close()

## -p option: create account parameters that's different from default
if args.parameter:
    crossrep.crAcctParameters(cursor)
    ofile = open(migHome + "scripts/acctobj/41_set_parameters.sql","w")
    # The file parms.txt stores current valid account parameters that user can control, in the same folder as main.py???
    valid_parmlist = crossrep.readFile(migHome+'parms.txt')
    crossrep.setAcctParameters(valid_parmlist,ofile, cursor)
    ofile.close()

if args.pipe :
    dbfname = args.pipe
    sf = open(migHome + "scripts/ddl/32_create_pipes_DDL.sql","w")
    try:
        
        if crossrep.mode == 'SNOWFLAKE' or crossrep.mode == 'CUSTOMER':
            crossrep.crPipes(cursor)
        if dbfname == 'all':
            crossrep.genAllPipeDDL( sf, cursor)
        else:
            dbfilepath = migHome + dbfname
            pipe_dblist = crossrep.readFile(dbfilepath)
            if crossrep.verbose:
                print('dbfile path/name: '+dbfilepath)
                print(pipe_dblist)
    
            crossrep.genPipeDDLByDBList(pipe_dblist, sf, cursor)
        
        crossrep.grantByObjType('PIPE', sf, cursor)

    except Exception as err:
        print('An error occured during creating pipe DDL :' + str(err) )
    sf.close()

#### -r with 3 options (nopwd, samepwd, randpwd): 
#       nopwd-no password, 
#       samepwd-same password for all users (cr0ss2REP) 
#       randpwd-random password
# to create users/roles DDL and the grants between roles and roles, roles and users,
if args.role:
    opt = args.role
    
    ### create users
    uf = open(migHome + "scripts/rbac/21_create_users.sql","w") 
    try:
        crossrep.genUserDDL(opt, uf, cursor)
    except Exception as err:
        print('An error occured during creating user DDL :' + str(err) )
        #pass
    uf.close()
    
    ### create roles
    rf = open(migHome + "scripts/rbac/22_create_roles.sql","w") 
    try:
        crossrep.genRoleDDL( rf, cursor)
    except Exception as err:
        print('An error occured during creating role DDL :' + str(err) )
        #pass
    rf.close()
    
    ### grant roles to users/roles
    gf = open(migHome + "scripts/rbac/23_grant_roles.sql","w") 
    ff = open(migHome + "scripts/rbac/24_grant_target_roles.sql","w") 
    try:
        crossrep.grantAllRoles( gf, cursor)
        crossrep.grantTargetRole(ff, cursor)
    except Exception as err:
        print('An error occured during grant role relationship :' + str(err) )
        #pass
    gf.close()
    ff.close()

## on snowhouse
if args.snowhouse:     
    databasename = args.snowhouse
    try:
        
        ddl_file = open(migHome + "scripts/snowhouse/h1_fk_DDL_"+ databasename +".sql","w") 
        drop_file = open(migHome + "scripts/snowhouse/h1_drop_fk_"+ databasename +".sql","w") 
        add_file = open(migHome + "scripts/snowhouse/h1_add_fk_"+ databasename + ".sql","w") 
        crossrep.repSHFKeys (databasename,ddl_file, drop_file, add_file, cursor)
        ddl_file.close()
        drop_file.close()
        add_file.close()
        
        drop_file = open(migHome + "scripts/snowhouse/h2_drop_default_"+ databasename +".sql","w") 
        add_file = open(migHome + "scripts/snowhouse/h2_alter_default_"+ databasename +".sql","w") 
        crossrep.repSHSeqTable( databasename, drop_file, add_file,cursor)
        drop_file.close()
        add_file.close()
        
    except Exception as err:
        print('An error occured during evaluating/reporting constraints/sequences on snowhouse:' + str(err) )
        #pass

# -stage option : to generating external stage DDL and its grants
if args.stage :
    ### create stages
    dbfname = args.stage
    sf = open(migHome + "scripts/ddl/31_create_stages_DDL.sql","w")
    try:
        if crossrep.mode == 'SNOWFLAKE':
            crossrep.crStages(cursor)
        if dbfname == 'all':
            crossrep.genAllStageDDL( sf, cursor)
        else:
            dbfilepath = migHome + dbfname
            stage_dblist = crossrep.readFile(dbfilepath)
            if crossrep.verbose:
                print('dbfile path/name: '+dbfilepath)
                print(stage_dblist)

            crossrep.genStageDDLByDBList(stage_dblist,sf, cursor)
            #for dbname in df_dblist:
            #    crossrep.genStageDDLByDB(dbname, 'ALL_STAGES', sf, cursor)
        
        crossrep.grantByObjType('STAGE', sf, cursor)

    except Exception as err:
        print('An error occured during creating stage DDL :' + str(err) )
        #pass
    sf.close()


# -t option: dropping all created objects in target account so it can be rerun for testing purpose
if args.test:
    # python main.py -t MYANG MDONOVAN NMAHESH
    # excluding: MYANG MDONOVAN NMAHESH from dropping list
    excludeUserList = args.test
    # drop all account objects including warehouses, resouce monitors, network policies 
    af = open(migHome + "scripts/test/x0_drop_acctobj.sql","w")
    # drop all databases  
    df = open(migHome + "scripts/test/x1_drop_dbobj.sql","w")
    # drop all users and roles  
    rf = open(migHome + "scripts/test/x2_drop_roles.sql","w")
    try:
        crossrep.dropWarehouses( af, cursor)
        crossrep.dropResMonitors( af, cursor)
        if crossrep.mode == 'CUSTOMER':
            crossrep.dropNetworkPolicies( af, cursor)
        crossrep.dropAllDBs( df, cursor)
        crossrep.dropRoles( rf, cursor)     
        crossrep.dropUsers(excludeUserList, rf, cursor)
        
    except Exception as err:
        print('An error occured during testing :' + str(err) )
        #pass
    af.close()
    df.close()
    rf.close()

# option -val, need to connect to both source and target account, passing in a file with database list
if args.validate:
    dbfname = args.validate
    rfile = open(migHome + "scripts/eval/c1_rowcount_hash_"+dbfname+".txt","w")
    try:
        
        if crossrep.mode == 'SNOWFLAKE':
            print('Validation option is only available for CUSTOMER mode')
        if crossrep.mode == 'CUSTOMER':
            dbfilepath = migHome + dbfname
            val_dblist = crossrep.readFile(dbfilepath)
            if crossrep.verbose:
                print('dbfile path/name: '+dbfilepath)
                print(val_dblist)


            crossrep.valCountHash(val_dblist, rfile)

    except Exception as err:
        print('An error occured during validating ELT data:' + str(err) )

    rfile.close()
    #hfile.close()


cursor.close() 
ctx.close()