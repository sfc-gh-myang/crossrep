#!/usr/bin/env python3
import crossrep, re
"""
Created on March 27 2019
Updated on July 2nd 2020
Generate object: warehouse, network policy, resource monitor
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'
# *********************************************************************************************************************
# 
# This module contains the Snowflake-specific logic of creating account level objects and stages:
#   The following user tables are created to store account level objects information.
#  - all_shares, all_warehouses, all_resmonitors, all_networkpolicies
#  - their DDLs that can be generated , then to be executed in target system are as follows:
#   scripts/acctobj/11_creater_monitors_DDL.sql         => DDL to create resource monitors
#   scripts/acctobj/12_create_network_policies_DDL.sql  => DDL to create network policies
#   scripts/acctobj/13_create_warehouses_DDL.sql        => DDL to create warehouses
#   scripts/acctobj/14_grant_acct_level_privs.sql       => grants to grant account level privileges
#   scripts/acctobj/41_set_parameters.sql               => commands to set account level parameters
#
# The following metadata table will be created in local table to store metadata with global variable:
#   tb_wh = 'ALL_WAREHOUSES'
#   tb_rm = 'ALL_RESOURCEMONITORS'
#   tb_np = 'ALL_NETWORKPOLICIES'
#   tb_share = 'ALL_SHARES'
#   tb_parm = 'ALL_PARMS'
# *********************************************************************************************************************

### create table for all shares if it's a new table: all_shares
### update the table if it's not new: update existing records, insert new records, delete the dropped ones
# tb_share: table name for all users
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crShares( cursor):
    tbname = crossrep.tb_share 
    if crossrep.verbose == True:
        print('start creating share table')
    tb_temp = "TEMP_" + tbname

    cursor.execute("begin")
    cursor.execute("show shares in account " + crossrep.acctpref)
    #cursor.execute("create or replace temp table " + tb_temp + " as select distinct select distinct $1 created, $2 kind, $3 name , $4 db_name, $5 to_account, $6 comment from table(result_scan(last_query_id()))") 
    cursor.execute("create or replace temp table " + tb_temp + " as select $2 kind, $3 name , $4 db_name, $5 to_account, $6 comment from table(result_scan(last_query_id())) ") 
    cursor.execute("commit")

    # check whether table exists
    checkquery = ("select count(*) from information_schema.tables where table_catalog = '" + crossrep.default_db + "' and table_schema = '" + crossrep.default_sc + "' and table_name = '" 
    + tbname + "' and table_owner is not null ")
    if crossrep.verbose == True:
        print('query checking share table existing: ' + checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            print ('create new share table')
            cursor.execute(" create table " + tbname + 
                " as select kind, name , db_name, to_account, comment from " + tb_temp )
        else:
            print('update share table')
            updShares(tb_temp, cursor)

    cursor.execute("drop table if exists " + tb_temp )
    cursor.execute("commit")
    if crossrep.verbose == True:
        print('Finish creating share table')

### update all_shares table using current information stored in a temp table
### if the current record exists in the table, update it; if not exists in the table, insert it; 
### if not exists in current temp table but exists in the table, delete it
# tb_share: table name for all shares
# tb_temp: the real time share information in a temp table 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def updShares(tb_temp, cursor):
    tbname = crossrep.tb_share
    if crossrep.verbose == True:         
        print('update share table')
    # insert/update the delta of new information 
    mquery = ("merge into " + tbname + " tgt using (" +
        " select kind, name , db_name, to_account, comment from "+tb_temp + 
        "                 minus " +
        " select kind, name , db_name, to_account, comment from "+tbname + 
        "  ) as src on tgt.kind = src.kind and tgt.name = src.name and tgt.db_name = src.db_name and tgt.to_account = src.to_account" +
        " when matched then update set tgt.comment = src.comment" +
        " when not matched then insert (kind, name , db_name, to_account, comment )" +
        "   values (src.kind, src.name , src.db_name, src.to_account, src.comment )")                   
    if crossrep.verbose == True:
        print(mquery)
    cursor.execute(mquery)
    # delete the ones dropped
    dquery = ( "delete from " + tbname + " tgt using (" +
        " select kind, name , db_name, to_account from "+ tbname  + 
        "                 minus " +
        " select kind, name , db_name, to_account from "+tb_temp + 
        "  ) as src where tgt.kind = src.kind and tgt.name = src.name and tgt.db_name = src.db_name and tgt.to_account = src.to_account" )
    if crossrep.verbose == True:
        print(dquery)
    cursor.execute(dquery)
    cursor.execute("commit")

### create table for all warehouses if it's a new table: all_warehouses
### update the table if it's not new: update existing records, insert new records, delete the dropped ones
# tb_wh: table name for all warehouses
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crWarehouse( cursor):
    if crossrep.verbose == True:
        print('Starting creating warehouse table ... ')
    tbname = crossrep.tb_wh
    tb_temp = "TEMP_" + tbname

    cursor.execute("begin")
    cursor.execute("show warehouses in account " + crossrep.acctpref)
    cursor.execute("create or replace temp table "+ tb_temp+" as select distinct $1 name,  $4 size, $5 min_cluster_count, $6 max_cluster_count, $12 auto_suspend, " + 
    "$13 auto_resume, $22 comment, $23 resource_monitor, $29 scaling_policy from table(result_scan(last_query_id()))")
    cursor.execute("commit")

    # check whether table exists
    checkquery = ("select count(*) from information_schema.tables where table_catalog = '" + crossrep.default_db + "' and table_schema = '" + crossrep.default_sc + "' and table_name = '" 
     + tbname + "' and table_owner is not null")
    if crossrep.verbose == True:
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            # create table if not existing
            print('create new warehouses table')
            cursor.execute("create table " + tbname+" as select name,size,min_cluster_count,max_cluster_count,auto_suspend,auto_resume,comment,resource_monitor,scaling_policy from " + tb_temp ) 
        else:
            print ('update warehouses table')
            # insert/update the delta of new users information 
            mquery = ( "merge into "+tbname+" tgt using (" +
            " select name,size,min_cluster_count,max_cluster_count,auto_suspend,auto_resume,comment,resource_monitor,scaling_policy from " + tb_temp +
            "    minus " +
            " select name,size,min_cluster_count,max_cluster_count,auto_suspend,auto_resume,comment,resource_monitor,scaling_policy from "+tbname +
            "  ) as src on tgt.name = src.name" +
            " when matched then update set tgt.size = src.size" +
            "             ,   tgt.min_cluster_count = src.min_cluster_count" +
            "             ,   tgt.max_cluster_count = src.max_cluster_count" +
            "             ,   tgt.auto_suspend = src.auto_suspend" +
            "             ,   tgt.auto_resume = src.auto_resume" +
            "             ,   tgt.comment = src.comment" +
            "             ,   tgt.resource_monitor = src.resource_monitor" +
            "             ,   tgt.scaling_policy = src.scaling_policy" +
            " when not matched then insert (name,size,min_cluster_count,max_cluster_count,auto_suspend,auto_resume,comment,resource_monitor,scaling_policy )" +
            "    values (src.name,src.size,src.min_cluster_count,src.max_cluster_count,src.auto_suspend,src.auto_resume,src.comment,src.resource_monitor,src.scaling_policy )" )
            if crossrep.verbose == True:
                print(mquery)
            cursor.execute(mquery)

            # delete the one dropped
            dquery = ( "delete from "+tbname+" tgt using (" +
            " select name  from " + tbname +
            "    minus " +
            " select name from "+tb_temp +
            " ) as src where tgt.name = src.name or tgt.name is null" )
            if crossrep.verbose == True:
                print(dquery)
            cursor.execute(dquery)
            cursor.execute("commit")
    cursor.execute("drop table if exists " + tb_temp )
    cursor.execute("commit")
    if crossrep.verbose == True:
        print ('Finishing create warehouse table ...')

### generating DDL for creating all warehouses 
# tb_wh: table name for all warehouses
# ofile: file to write warehouse creation DDLs
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genWarehouseDDL(ofile, cursor):
    tbname = crossrep.tb_wh
    query = ("select name, size, min_cluster_count, max_cluster_count, auto_suspend, auto_resume, comment, resource_monitor, scaling_policy from  " 
    + tbname + " order by name ")
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        size = re.sub(r'-','',r[1]).upper() 
        if size == '4XLARGE' :
            size = 'X4LARGE'
        elif size == '3XLARGE' :
            size = 'XXXLARGE'
        elif size == '2XLARGE' :
            size = 'XXLARGE'
        min_cluster_count = str(r[2])
        max_cluster_count = str(r[3])
        auto_suspend = str(r[4]) 
        auto_resume = r[5] 
        comment = r[6]
        resource_monitor = r[7]
        scaling_policy = r[8] 
        if name.isdigit()==True:
            continue
        if crossrep.isBlank(size) == True:
            size = 'XLARGE'
        if crossrep.verbose == True:
            print(name, size)
        crSQL = "CREATE WAREHOUSE IF NOT EXISTS " + name + " warehouse_size = " + size  
        if crossrep.isBlank (min_cluster_count ) == False :
            if crossrep.verbose == True:
                print(min_cluster_count)
            crSQL = crSQL + " min_cluster_count=" + min_cluster_count 
            if crossrep.verbose == True:
                print(crSQL)
        if crossrep.isBlank (max_cluster_count ) == False :
            if crossrep.verbose == True:
                print(max_cluster_count)
            crSQL = crSQL + " max_cluster_count=" + max_cluster_count 
        if crossrep.isBlank (auto_suspend ) == False :
            if crossrep.verbose == True:
                print(auto_suspend)
            crSQL = crSQL + " auto_suspend = " + auto_suspend 
        if crossrep.isBlank (auto_resume ) == False :
            if crossrep.verbose == True:
                print(auto_resume)
            crSQL = crSQL + " auto_resume = " + auto_resume 
        if crossrep.isBlank (resource_monitor ) == False :
            if crossrep.verbose == True:
                print(resource_monitor)
            crSQL = crSQL + " resource_monitor = " + resource_monitor 
        if crossrep.isBlank (scaling_policy ) == False:
            if crossrep.verbose == True:
                print(scaling_policy)
            crSQL = crSQL + " scaling_policy = " + scaling_policy 
        if crossrep.isBlank (comment) == False:
            if crossrep.verbose == True:
                print(comment)
            crSQL = crSQL + ' comment =  "' + comment + '"'
        ofile.write(crSQL+';\n')
        if crossrep.verbose == True:
            print(crSQL)

### drop all warehouse
# tb_wh: table name for all warehouses
# ofile: file to write dropping statement of all warehouses
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def dropWarehouses( ofile, cursor):
    if crossrep.verbose == True:
        print('Start generating drop warehouse stmt to drop all warehouses')
    
    ofile.write('use role accountadmin;\n')    

    query = "select distinct name from " + crossrep.tb_wh 
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        gsql1 = "drop warehouse if exists " + name +"  ;\n "
        ofile.write(gsql1)

    if crossrep.verbose == True:
        print('Finish generating drop warehouse stmt to drop all warehouses')

### suspend and resume all warehouse
# tb_wh: table name for all warehouses
# ofile1: file to write suspending statement of all warehouses
# ofile2: file to write resuming statement of all warehouses
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def susWarehouse( ofile1, ofile2, cursor):
    if crossrep.verbose == True:
        print('Start generating alter warehouse stmt to suspend all warehouses')
    
    ofile1.write('use role accountadmin;\n')    
    ofile2.write('use role accountadmin;\n')  

    query = "select distinct name from " + crossrep.tb_wh 
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        gsql1 = "alter warehouse if exists " + name +" suspend ;\n "
        ofile1.write(gsql1)
        gsql2 = "alter warehouse if exists " + name +" resume ;\n "
        ofile2.write(gsql2)
    if crossrep.verbose == True:
        print('Finish generating alter warehouse stmt to suspend all warehouses')

'''
108.168.254.33/32,108.168.254.45/32,108.168.254.64/32,108.168.254.169/32,108.168.255.14/32,108.168.255.213/32,108.168.254.232/32,108.168.254.254/32,52.3.49.45/32,52.21.197.207/32,169.45.135.170/32,169.45.141.210/32,169.45.131.52/32,169.45.221.24/32,169.45.225.119/32,198.0.137.241/32,38.104.218.122/32,206.169.144.70/32,38.140.109.58/32,38.122.7.98/32,37.17.34.76/32,80.249.81.220/32,46.20.72.162/32,46.20.72.160/29,141.0.182.34/32,141.0.182.0/26,195.151.169.125/32,195.151.169.120/29,174.37.179.212/32,174.37.179.213/32,206.169.144.70/32,108.168.244.131/32,108.168.255.232/32
'''
### convert allowed ip list or blocked ip list string into string of ip lists for create statement
# iplist : a string for ip list with comma as seperator, format as in show network policy
def genIPlist (iplist):
    iplist = iplist.split(',')
    ips = "'" + "','".join(iplist) + "'"
    return ips

### descible network policy, return a dictionary of ip type (blocked or allowed) and ip list in a python dictionary format 
### desc network policy command can't be executed from PROD, so this can only be called from snowflake account
# name: network policy name
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def descNetworkPolicy(name, cursor):
    cursor.execute("begin")
    cursor.execute("desc network policy \""+ name+"\"")
    cursor.execute(" select $1 listtype ,  $2 listvalue from table(result_scan(last_query_id()))")
    rec = cursor.fetchall()
    cursor.execute('commit')
    ipDic = {}
    for r in rec:
        listtype = r[0]
        iplist = genIPlist(r[1])
        if crossrep.verbose == True:
            print("Before ==>" + listtype + ":" + r[1])
            print("after ==>" + listtype + ":" + iplist)
        ipDic[listtype] = iplist
    return ipDic 

### create a new table to store network policy information if not existing, updating the table if existing
### desc network policy command can't be executed from PROD, so this can only be called from snowflake account (customer mode), SNOW-83395
# tb_np: network policy table name
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crNetworkPolicy(cursor):
    tbname = crossrep.tb_np
    tb_temp = "TEMP_" + tbname

    cursor.execute("begin")
    cursor.execute("show network policies in account " + crossrep.acctpref)
    cursor.execute("create or replace temp table "+ tb_temp+" as select distinct \"$2\" name,  $3 comment, $4 entries_in_allowed_ip_list, $5 entries_in_blocked_ip_list from table(result_scan(last_query_id()))")
    cursor.execute("commit")

    # check whether table exists
    checkquery = ("select count(*) from information_schema.tables where table_catalog = current_database() and table_schema = current_schema() and table_name = '" 
     + tbname + "' and table_owner is not null")
    if crossrep.verbose == True:
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            # create table if not existing
            print('create new network policy table')
            cursor.execute("create table " + tbname+" as select name, comment, entries_in_allowed_ip_list, entries_in_blocked_ip_list from " + tb_temp ) 
        else:
            print ('update network policy table')
            # insert/update the delta of new users information 
            mquery = ( "merge into "+tbname+" tgt using (" +
            " select name, comment, entries_in_allowed_ip_list, entries_in_blocked_ip_list from " + tb_temp +
            "    minus " +
            " select name, comment, entries_in_allowed_ip_list, entries_in_blocked_ip_list from "+tbname +
            "  ) as src on tgt.name = src.name" +
            " when matched then update set tgt.comment = src.comment" +
            "             ,   tgt.entries_in_allowed_ip_list = src.entries_in_allowed_ip_list" +
            "             ,   tgt.entries_in_blocked_ip_list = src.entries_in_blocked_ip_list" +
            " when not matched then insert (name, comment, entries_in_allowed_ip_list, entries_in_blocked_ip_list )" +
            "    values (src.name, src.comment, src.entries_in_allowed_ip_list, src.entries_in_blocked_ip_list )" )
            if crossrep.verbose == True:
                print(mquery)
            cursor.execute(mquery)

            # delete the one dropped
            dquery = ( "delete from "+tbname+" tgt using (" +
            " select name  from " + tbname +
            "    minus " +
            " select name from "+tb_temp +
            " ) as src where tgt.name = src.name or tgt.name is null" )
            if crossrep.verbose == True:
                print(dquery)
            cursor.execute(dquery)
            cursor.execute("commit")
    cursor.execute("drop table if exists " + tb_temp )
    cursor.execute("commit")

### generating DDL for creating all network policies 
# tbname: table name for all network policies 
# ofile: file to write network policy creation DDLs
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genNetworkPolicyDDL( ofile, cursor):
    cursor.execute(" select name, comment,entries_in_allowed_ip_list, entries_in_blocked_ip_list from "+crossrep.tb_np+" order by name")
    rec = cursor.fetchall()
    ipDic = {}
    for r in rec:
        name = r[0]
        comment = r[1]
        entries_in_allowed_ip_list = r[2]
        entries_in_blocked_ip_list = r[3]
        if name.isdigit()==True:
            continue

        crSQL = "CREATE NETWORK POLICY IF NOT EXISTS  \"" + name +"\""
        if entries_in_allowed_ip_list != 0 or entries_in_blocked_ip_list != 0 :
            ipDic = descNetworkPolicy(name, cursor)
            if entries_in_allowed_ip_list != 0: 
                allowed_ip_list = ipDic["ALLOWED_IP_LIST"]
                crSQL = crSQL + " allowed_ip_list =  (" + allowed_ip_list + ")"
            if entries_in_blocked_ip_list != 0 :
                blocked_ip_list = ipDic["BLOCKED_IP_LIST"]
                crSQL = crSQL + " blocked_ip_list =  (" + blocked_ip_list + ")"
        if not crossrep.isBlank (comment ):
              crSQL = crSQL + ' comment =  "' + comment + '"'
        ofile.write(crSQL+';\n')

### drop all network policies
# crossrep.tb_np: table name for all network policies
# ofile: file to write dropping statement of all network policies
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def dropNetworkPolicies( ofile, cursor):
    if crossrep.verbose == True:
        print('Start generating drop network policy stmt ')
    
    ofile.write('use role accountadmin;\n')    

    query = "select distinct name from " + crossrep.tb_np 
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        gsql1 = "drop network policy if exists " + name +"  ;\n "
        ofile.write(gsql1)

    if crossrep.verbose == True:
        print('Finish generating drop network policy stmt ')

### create table for all resource monitors: all_resourcemonitors
# tb_rm: table name for all  resource monitors
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crResMonitor( cursor):
    if crossrep.verbose == True:
        print ('starting create resouce_monnitor table in source account ')
    tbname = crossrep.tb_rm
    tb_temp = "TEMP_" + tbname

    cursor.execute("begin")
    cursor.execute("show resource monitors in account " + crossrep.acctpref)
    cursor.execute("create or replace temp table " + tb_temp +" as select  $1 name, $2 credit_quota , $6 frequency, " + 
        " ( case when $7 is null or $7 < current_timestamp() then 'IMMEDIATELY' " +
        " else to_char(to_timestamp_ntz($7)) end ) start_time , " +
        " ( case when $8 > current_timestamp() then to_char(to_timestamp_ntz($8)) " + 
        " else null end ) end_time , "
        " $9 notify_at, $10 suspend_at, $11 suspend_immediately_at,$13 owner, $14 comment " + 
        " from table(result_scan(last_query_id()))") 
    cursor.execute("commit")

    # check whether table exists
    checkquery = ("select count(*) from information_schema.tables where table_catalog = '" + crossrep.default_db + "' and table_schema = '" + crossrep.default_sc + "' and table_name = '" 
     + tbname + "' and table_owner is not null")
    if crossrep.verbose == True:
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            # create table if not existing
            print('create new resource monitor table')
            cursor.execute("create table " + tbname+" as select distinct name, credit_quota , frequency, start_time,end_time, notify_at, suspend_at, suspend_immediately_at, owner, comment from " + tb_temp ) 
        else:
            print ('update resource monitor table')
            # insert/update the delta of new users information 
            mquery = ( "merge into "+tbname+" tgt using (" +
            " select name, credit_quota , frequency, start_time,end_time, notify_at, suspend_at, suspend_immediately_at, owner, comment from " + tb_temp +
            "    minus " +
            " select name, credit_quota , frequency, start_time,end_time, notify_at, suspend_at, suspend_immediately_at, owner, comment from "+tbname +
            "  ) as src on tgt.name = src.name" +
            " when matched then update set tgt.credit_quota = src.credit_quota" +
            "             ,   tgt.frequency = src.frequency" +
            "             ,   tgt.start_time = src.start_time" +
            "             ,   tgt.end_time = src.end_time" +
            "             ,   tgt.notify_at = src.notify_at" +
            "             ,   tgt.suspend_at = src.suspend_at" +
            "             ,   tgt.suspend_immediately_at = src.suspend_immediately_at" +
            "             ,   tgt.owner = src.owner" +
            "             ,   tgt.comment = src.comment" +
            " when not matched then insert (name, credit_quota , frequency, start_time,end_time, notify_at, suspend_at, suspend_immediately_at, owner, comment )" +
            "    values (src.name, src.credit_quota , src.frequency, src.start_time,src.end_time, src.notify_at, src.suspend_at, src.suspend_immediately_at, src.owner, src.comment )" )
            if crossrep.verbose == True:
                print(mquery)
            cursor.execute(mquery)

            # delete the one dropped
            dquery = ( "delete from "+tbname+" tgt using (" +
            " select name  from " + tbname +
            "    minus " +
            " select name from "+tb_temp +
            " ) as src where tgt.name = src.name or tgt.name is null" )
            if crossrep.verbose == True:
                print(dquery)
            cursor.execute(dquery)
            cursor.execute("commit")
    cursor.execute("drop table if exists " + tb_temp )
    cursor.execute("commit")
    if crossrep.verbose == True:
        print ('finishing create resource_monitor table ' )

### handling resource monitor trigger from information of show resource monitor 
# percent: percent shown in the "show resource monitors" : 50%,70%
# action : notify, suspend, suspend immediate
# return trigger action:    on 75 percent do notify
#                           on 100 percent do suspend
#                           on 110 percent do suspend_immediate
def RMtrigger(percent, action):
    trigger = ''
    if not crossrep.isBlank(percent):
        for p in percent.split(','):
            p = re.sub(r'%','',p)
            trigger = trigger + " ON " + p + " PERCENT DO " + action 
    return trigger

### Generating all resource monitor DDL for target system 
# tb_rm: table name for all  resource monitors
# ofile: file to write creating statement of all resource monitors
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genResMonitor( ofile, cursor):
    query = " select name, credit_quota , frequency, start_time, end_time, notify_at, suspend_at, suspend_immediately_at, comment from " + crossrep.tb_rm + " order by name"
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        credit_quota = r[1]
        frequency = r[2]
        start_time = r[3]
        end_time = r[4]
        notify_at = r[5]
        suspend_at = r[6]
        suspend_immediately_at = r[7]
        comment = r[8]

        if name.isdigit()==True:
            continue
        cquery = " CREATE RESOURCE MONITOR IF NOT EXISTS " + name + " WITH "
        if crossrep.isBlank(credit_quota) == False :
            if credit_quota.isdigit() == True:
                cquery = cquery + " CREDIT_QUOTA=" + credit_quota 
        if crossrep.isBlank(frequency) == True :
            frequency = 'NEVER'
        elif frequency not in ['MONTHLY', 'DAILY', 'WEEKLY', 'YEARLY' ]:
            frequency = 'NEVER'
        if crossrep.isBlank(start_time) == True:
            start_time = 'IMMEDIATELY'
        '''
        else:
            # not setting timestamp due to diff system
            start_time = 'IMMEDIATELY'
        '''
        cquery = cquery + " FREQUENCY='" + frequency + "'" +" START_TIMESTAMP='"+start_time+"'"
        
        #no end time
        if crossrep.isBlank(end_time) == False:
            cquery = cquery + " END_TIMESTAMP='" + end_time +"'"
        
        trigger = RMtrigger(notify_at, 'NOTIFY') + RMtrigger(suspend_at, 'SUSPEND') + RMtrigger(suspend_immediately_at, 'SUSPEND_IMMEDIATE') 
        if  not crossrep.isBlank(trigger) :
            cquery = cquery + " TRIGGERS " + trigger 
        if not crossrep.isBlank (comment ):
            cquery = cquery + ' comment =  "' + comment + '"'
        ofile.write(cquery + ';\n')

### drop all resource monitors
# tb_rm: table name for all resource monitors
# ofile: file to write dropping statement of all resource monitors
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def dropResMonitors( ofile, cursor):
    if crossrep.verbose == True:
        print('Start generating drop resource monitor stmt ')
    
    ofile.write('use role accountadmin;\n')    

    query = "select distinct name from " + crossrep.tb_rm 
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        gsql1 = "drop resource monitor if exists " + name +"  ;\n "
        ofile.write(gsql1)

    if crossrep.verbose == True:
        print('Finish generating drop resource monitor stmt ')

### create table to store account parameters 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crAcctParameters(cursor):
    
    cursor.execute('begin')
    query = 'SHOW PARAMETERS IN ACCOUNT ' + crossrep.acctpref
    cursor.execute(query)
    cursor.execute('create or replace table '+ crossrep.tb_parm + ' as select distinct "key" parm,  "value" val, "default" def, "level" level, "description" description, "type" type '+
        'from table(result_scan(last_query_id())) ')
    cursor.execute('commit')

### setting account parameters whose value is different from default value
# valid_parmlist : current valid parameter list that's allowed to be modified by user (provided by snowflake in a file, then read the file into a python list)
# ofile: output file for writing alter account command for the parameters
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def setAcctParameters( valid_parmlist, ofile, cursor):
    #inPredicate = crossrep.genInPredicate('PARM', valid_parmlist)
    inList = crossrep.genInList( valid_parmlist)
    if inList != '':
        inPredicate = ' and PARM ' + inList
        excludePred = ' and PARM not '+ inList 
    else:
        inPredicate = ''
        excludePred = ''
    fquery = (" select parm, val, type from " + crossrep.tb_parm +"  where level != 'SYSTEM' "+
        " AND (VAL != def OR ( VAL IS NULL AND DEF IS NULL ) )  " + inPredicate )
    if crossrep.verbose == True:
        print(fquery)
    cursor.execute(fquery)
    rec = cursor.fetchall()
    for r in rec:
        parm = r[0]
        val = r[1]
        tp = r[2]

        if tp == 'STRING':
            setparm = "ALTER ACCOUNT SET  " + parm + " = '" + val +"'"
        else :
            setparm = "ALTER ACCOUNT SET  " + parm + "=" + val             
        ofile.write(setparm + ";\n")
    
    pquery = (" select parm, val, def, type from " + crossrep.tb_parm +"  where level != 'SYSTEM' "+
        " AND (VAL != def OR ( VAL IS NULL AND DEF IS NULL ) )  " + excludePred )
    if crossrep.verbose == True:
        print(pquery)
    cursor.execute(pquery)
    rec = cursor.fetchall()
    for r in rec:
        parm = r[0]
        val = r[1]
        df = r[2]
        tp = r[3]            
        ofile.write("snowflake internal parm=> " + parm + "; value:"+ val+"; default:"+df + "; type:"+tp+ ";\n")

### create tables to store acount usage objects information in the configured database and schema, table name is the same as the view name in account_usaage.
# cursor : connection to source system, make sure to use database and schema that's for account_usage history data
# dbname : database name to hold account_usage
# scname : schema name to hold account_usage  
def crAccountUsage(dbname, scname, cursor ):
    cursor.execute("CREATE DATABASE IF NOT EXISTS "+dbname)
    #cursor.execute("USE DATABASE "+dbname)
    cursor.execute("CREATE SCHEMA IF NOT EXISTS "+scname)
    #cursor.execute("USE SCHEMA "+scname)

    cursor.execute("begin")
    cursor.execute("show objects in snowflake.account_usage")
    cursor.execute("select distinct $2 obj_name from table(result_scan(last_query_id()))") 
    rec = cursor.fetchall()
    cursor.execute("commit")
    for r in rec:
        tb =  r[0]
        cursor.execute("create or replace table " + dbname + "." + scname + "."+ tb + " as select * from  snowflake.account_usage." + r[0] )

#def countOBJ( ofile, cursor):
