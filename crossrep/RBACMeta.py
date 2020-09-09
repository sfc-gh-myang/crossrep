#!/usr/bin/env python3
import crossrep,re, threading
"""
Created on Oct 1 2018
Updated on July 12th 2020
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'
# *********************************************************************************************************************
# 
# This module contains the Snowflake-specific logic of creating account level objects and stages:
#   - it can geneate 5 user defined tables in the database and schema for roles, users, parent_child roles, privileges 
#    Note: expecting a long run from 30 mins to a couple of hours depending on how many privileges in the source system
#
#   The following metadata table will be created in local table to store metadata with global variable:
#   tb_role = 'ALL_ROLES'
#   tb_user = 'ALL_USERS'
#   tb_pcrl = 'PARENT_CHILD' 
#   tb_priv = 'PRIVILEGES'
#
# *********************************************************************************************************************
# create table for all users if it's a new table: all_users
# update the table if it's not new: update existing records, insert new records, delete the dropped ones
# tb_user: table name for all users - global variable
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crUsers( cursor):
    tb_temp = "TEMP_" + crossrep.tb_user
    squery = "show users in account " + crossrep.acctpref
    cquery = ( "create or replace temp table " + tb_temp + 
        " as select \"name\" user_name, \"created_on\" created_tm ,\"login_name\" login_name, \"display_name\" display_name, " +
        "\"first_name\" first_name, \"last_name\" last_name, \"email\" email, \"mins_to_unlock\" mins_to_unlock, \"days_to_expiry\" days_to_expiry, "+
        "\"comment\" comment, \"disabled\" disabled ,\"must_change_password\" must_change_password, \"snowflake_lock\" snowflake_lock, "+
        "\"default_warehouse\" default_warehouse, \"default_namespace\" default_namespace, \"default_role\" default_role  " +
        "from table(result_scan(last_query_id()))") 
    # those may need to reconfiguration on the new account, not including
    #   ", \"ext_authn_duo\" ext_authn_duo, \"ext_authn_uid\" ext_authn_uid, \"mins_to_bypass_mfa\" mins_to_bypass_mfa ,\"has_rsa_public_key\" has_rsa_public_key " +
    if crossrep.verbose == True:
        print(squery)
        print(cquery)
    cursor.execute("begin")
    cursor.execute(squery)
    cursor.execute(cquery)
    cursor.execute("commit")

    # check whether table exists
    checkquery = ("select count(*) from information_schema.tables where table_schema = '" + crossrep.default_sc + "' and table_name = '" +
        crossrep.tb_user + "' and table_owner is not null ")
    if crossrep.verbose == True:
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            # create table if not existing
            print('create new user table')
            cursor.execute("create table " + crossrep.tb_user+" as select user_name, created_tm , login_name, display_name, first_name,last_name, email, mins_to_unlock, days_to_expiry, comment, disabled ," +
            "  must_change_password, snowflake_lock,  default_warehouse, default_namespace,  default_role from " + tb_temp ) 
        else:
            print ('update user table')
            # insert/update the delta of new users information 
            mquery = ( "merge into "+crossrep.tb_user+" tgt using (" +
            " select user_name, created_tm , login_name, display_name, first_name,last_name, email, mins_to_unlock, days_to_expiry, comment, disabled , " + 
            "   must_change_password, snowflake_lock,  default_warehouse, default_namespace,  default_role from " + tb_temp +
            "    minus " +
            " select user_name, created_tm , login_name, display_name, first_name,last_name, email, mins_to_unlock, days_to_expiry, comment, disabled , " + 
           "   must_change_password, snowflake_lock,  default_warehouse, default_namespace,  default_role from " + crossrep.tb_user +
            "  ) as src on tgt.user_name = src.user_name" +
            " when matched then update set tgt.created_tm = src.created_tm" +
            "             ,   tgt.login_name = src.login_name" +
            "             ,   tgt.display_name = src.display_name" +
            "             ,   tgt.first_name = src.first_name" +
            "             ,   tgt.last_name = src.last_name" +
            "             ,   tgt.email = src.email" +
            "             ,   tgt.mins_to_unlock = src.mins_to_unlock" +
            "             ,   tgt.days_to_expiry = src.days_to_expiry" +
            "             ,   tgt.comment = src.comment" +
            "             ,   tgt.must_change_password = src.must_change_password" +
            "             ,   tgt.snowflake_lock = src.snowflake_lock" +
            "             ,   tgt.default_warehouse = src.default_warehouse" +
            "             ,   tgt.default_namespace = src.default_namespace" +
            "             ,   tgt.default_role = src.default_role" +
            " when not matched then insert (user_name, created_tm , login_name, display_name, first_name,last_name, email, mins_to_unlock, days_to_expiry, comment, disabled,must_change_password, snowflake_lock, default_warehouse, default_namespace,  default_role )" +
            "    values (src.user_name, src.created_tm , src.login_name, src.display_name, src.first_name, src.last_name, src.email, src.mins_to_unlock, src.days_to_expiry, src.comment, src.disabled, src.must_change_password, src.snowflake_lock, src.default_warehouse, src.default_namespace, src.default_role )" )
            if crossrep.verbose == True:
                print(mquery)
            cursor.execute(mquery)

            # delete the one dropped
            dquery = ( "delete from "+crossrep.tb_user+" tgt using (" +
            " select user_name  from " + crossrep.tb_user +
            "    minus " +
            " select user_name from "+tb_temp +
            " ) as src where tgt.user_name = src.user_name or tgt.user_name is null" )
            if crossrep.verbose == True:
                print(dquery)
            cursor.execute(dquery)
            cursor.execute("commit")
    cursor.execute("drop table if exists " + tb_temp )
    cursor.execute("commit")
    if crossrep.verbose == True:
        print ('Finishing create user table ... ' )

# create table for all roles: all_roles
# create table for all roles if it's a new table: all_users
# update the table if it's not new: update existing records, insert new records, delete the dropped ones
# tb_role : table name for all roles
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crRoles( cursor):
    if crossrep.verbose == True:
        print ('starting create role table in source account ...' )
    tb_temp = "TEMP_" + crossrep.tb_role
    cursor.execute("begin")
    cursor.execute("show roles in account " + crossrep.acctpref )
    cursor.execute("create or replace temp table " + tb_temp + " as select $1 created_at, $2 role_name, $3 is_default , $4 is_current, $5 is_inherited, $6 assigned_to_users,$7 granted_roles, $8 granted_to_roles, $9 owner, $10 comments from table(result_scan(last_query_id()))") 
    cursor.execute("commit")

    # check whether table exists
    checkquery = ("select count(*) from information_schema.tables where table_catalog = '" + crossrep.default_db + "' and table_schema = '" + crossrep.default_sc + "' and table_name = '"
     + crossrep.tb_role + "' and table_owner is not null ")
    if crossrep.verbose == True:
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            print('create new role table')
            cursor.execute("create table " + crossrep.tb_role + " as select created_at, role_name, is_default , is_current, is_inherited, assigned_to_users,granted_roles, granted_to_roles, owner, comments from " + tb_temp) 
        else:
            print('update role table')
            # insert/update the delta of new roles information 
            mquery = ("merge into " + crossrep.tb_role + " tgt using (" +
                " select created_at, role_name, is_default , is_current, is_inherited, assigned_to_users,granted_roles, granted_to_roles, owner, comments from "+tb_temp +
                "                 minus " +
                " select created_at, role_name, is_default , is_current, is_inherited, assigned_to_users,granted_roles, granted_to_roles, owner, comments from "+crossrep.tb_role +
                "  ) as src on tgt.role_name = src.role_name" +
                " when matched then update set tgt.created_at = src.created_at" +
                "                         ,   tgt.is_default = src.is_default" +
                "                         ,   tgt.is_current = src.is_current" +
                "                         ,   tgt.is_inherited = src.is_inherited" +
                "                         ,   tgt.assigned_to_users = src.assigned_to_users" +
                "                         ,   tgt.granted_roles = src.granted_roles" +
                "                         ,   tgt.granted_to_roles = src.granted_to_roles" +
                "                         ,   tgt.owner = src.owner" +
                "                         ,   tgt.comments = src.comments" +
                " when not matched then insert (created_at,role_name, is_default , is_current, is_inherited, assigned_to_users,granted_roles, granted_to_roles, owner, comments )" +
                "   values (src.created_at, src.role_name, src.is_default , src.is_current, src.is_inherited, src.assigned_to_users, src.granted_roles, src.granted_to_roles, src.owner, src.comments )")                   
            if crossrep.verbose == True:
                print(mquery)
            cursor.execute(mquery)
            # delete the ones dropped
            dquery = ( "delete from " + crossrep.tb_role + " tgt using (" +
                " select role_name from "+ crossrep.tb_role +
                "                 minus " +
                " select role_name from "+ tb_temp +
            " ) as src where tgt.role_name = src.role_name or ( src.role_name is null and tgt.role_name is null) " ) 
            if crossrep.verbose == True:
                print(dquery)
            cursor.execute(dquery)
            cursor.execute("commit")
            ### dropping the records in parent_child and privileges table where roles has been dropped now.
            crossrep.updDroppedRole(cursor)
    cursor.execute("drop table if exists " + tb_temp )
    cursor.execute("commit")
    if crossrep.verbose == True:
        print ('Finishing create role table ...')

### credit to Renga, change based on renga's function for the change of using available account_usage views 
### create table parent_child for role and user relationships: parent_child
### if the current record exists in the table, update it; if not exists in the table, insert it; 
### if not exists in current temp table but exists in the parent_child table, delete it
# tb_priv : table name for all privileges
# tb_role: the child role that's created
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crParentPriv( cursor):
    if crossrep.verbose == True:
        print ('Starting create parent_child and privileges tables ...')

    if crossrep.mode == 'CUSTOMER':
        contx = crossrep.getConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_pwd, crossrep.default_wh, crossrep.default_rl)
    elif crossrep.mode == 'SNOWFLAKE':
        contx = crossrep.getSFConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_wh, crossrep.default_rl)
    cs = contx.cursor()
    cs.execute("USE DATABASE "+crossrep.default_db)
    cs.execute("USE SCHEMA "+crossrep.default_sc)
    
    tbname = crossrep.tb_pcrl
    tbpriv = crossrep.tb_priv
    
    sql_stmt1 = ( "create or replace table " + tbname  +
        """
        as 
        select 
            date_trunc('second',created_on) as created_at
            , name as role_name 
            , granted_on as granted_to 
            , grantee_name 
            , granted_by 
        from snowflake.account_usage.grants_to_roles 
        where 1 = 1 
        and deleted_on is null
        and granted_on = 'ROLE' 
        and privilege = 'USAGE' 
        and granted_to = 'ROLE'  
        union all 
        select 
            date_trunc('second',created_on) as created_at
            , role as role_name 
            , granted_to 
            , grantee_name 
            , granted_by 
        from snowflake.account_usage.grants_to_users
        where deleted_on is null
        and granted_to = 'USER'
    """ )
    sql_stmt2 = ("create or replace table " + tbpriv  +
        """
        as  
        select 
            date_trunc('second',created_on) as created_at 
            , PRIVILEGE as priv 
            , case 
                when GRANTED_ON = 'INTEGERATION' then 'INTEGRATION' 
                else GRANTED_ON 
              end as OBJECT_TYPE 
            , CASE 
                WHEN (TABLE_CATALOG IS NULL OR GRANTED_ON = 'DATABASE') THEN NAME
                WHEN (GRANTED_ON = 'SCHEMA') then TABLE_CATALOG || '.' || NAME 
                ELSE COALESCE(TABLE_CATALOG || COALESCE('.' || TABLE_SCHEMA || '.', '.') || NAME, NAME) 
              END as OBJECT_NAME 
            , GRANTEE_NAME as role 
            , GRANT_OPTION 
        from snowflake.account_usage.grants_to_roles
        where 1 = 1
    """ )
    if crossrep.verbose == True:
        print(sql_stmt1)
        print(sql_stmt2)
    cs.execute("begin")
    cs.execute(sql_stmt1)
    cs.execute(sql_stmt2)
    cs.execute("commit")
    cs.close()
    contx.close()
    if crossrep.verbose == True:
        print ('Finishing create parent_child and privileges table ' )

### create table parent_child for role and user relationships: parent_child
### if the current record exists in the table, update it; if not exists in the table, insert it; 
### if not exists in current temp table but exists in the parent_child table, delete it
# tb_pcrl : table name for all parent child relationship
# tb_role: the child role that's created
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crParent( cursor):
    if crossrep.verbose == True:
        print ('Starting create parent_child table ...')

    if crossrep.mode == 'CUSTOMER':
        contx = crossrep.getConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_pwd, crossrep.default_wh, crossrep.default_rl)
    elif crossrep.mode == 'SNOWFLAKE':
        contx = crossrep.getSFConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_wh, crossrep.default_rl)
    cs = contx.cursor()
    cs.execute("USE DATABASE "+crossrep.default_db)
    cs.execute("USE SCHEMA "+crossrep.default_sc)

    #tbpriv = crossrep.tb_priv
    tbroles = crossrep.tb_role
    tbname = crossrep.tb_pcrl
    isCreated = False
    
    checkquery = ("select count(*) from information_schema.tables where table_schema = '" + crossrep.default_sc + "' and table_name = '" 
    + tbname + "' and table_owner is not null")
    if crossrep.verbose == True:
        print("current database: " + crossrep.default_db + "; current schema:" +  crossrep.default_sc )
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            isCreated = False
            cs.execute("begin")
            cs.execute("show grants of role " + crossrep.acctpref_qualifier + "ACCOUNTADMIN")
            cs.execute("create or replace table " + tbname + " as select distinct $1 created_at, $2 role_name, $3 granted_to , $4 grantee_name, $5 granted_by from table(result_scan(last_query_id()))") 

            cs.execute("commit")
        else:
            isCreated = True
    
    if isCreated == False:
        query = "select distinct role_name from " + tbroles + " where role_name != 'ACCOUNTADMIN' order by role_name "
    else:
        #query = "select distinct role_name from " + tbroles + " where role_name = 'AMARKIS_USER_ROLE' order by role_name "
        query = "select distinct role_name from " + tbroles + " order by role_name "
    cursor.execute(query)
    if crossrep.verbose == True:
        print(query)
    rec_set = cursor.fetchall()
    for rec in rec_set:
        role = rec[0]
        tb_temp =  '"'+'TEMP_'+ tbname+'_'+role.strip('"') + '"'
        role = '"'+role.strip('"') + '"'

        squery = "show grants of role "+  crossrep.acctpref_qualifier + role
        #squery = "show grants of role \""+  crossrep.acctpref_qualifier + role + "\""

        cquery = ("create or replace table " + tb_temp +
                " as select distinct $1 created_at, $2 role_name, $3 granted_to , $4 grantee_name, $5 granted_by " +
                " from table(result_scan(last_query_id())) order by role_name" )
        if crossrep.verbose == True:
            print(" tb_temp_role: " + tb_temp )
            print(squery)
            print(cquery)
        cs.execute("begin ")
        cs.execute(squery)
        cs.execute(cquery)
        cs.execute("commit")

        if isCreated == False:
            # Process each insert in parallelized batches...
            #t1 = threading.Thread(target=insPrivByRole, args=(tbpriv, tb_temp, ctx))
            t1 = threading.Thread(target=insParent, args=(tbname, tb_temp, cursor))
            t1.setDaemon(True)
            t1.start()
        else:
            t2 = threading.Thread(target=updParent, args=(tbname, tb_temp, role,cursor))
            #t2 = threading.Thread(target=updPriv, args=(tb_temp, role))
            t2.setDaemon(True)
            t2.start()
            #updPriv(tbpriv, tb_temp, role)
    
    main_thread = threading.currentThread()
    for t in threading.enumerate():
        if t is main_thread:
            continue
        if crossrep.verbose:
            print('Completed %s', t.getName())
        t.join()

    cs.close()
    contx.close()
    if crossrep.verbose == True:
        print ('Finishing create parent_child table ' )

### insert parent_child table using current information stored in a temp table
# tb_pcrl: table name for all parent child relationship
# tb_role: the child role that's updated
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def insParent( tbname, tb_temp ,cursor):
    cursor.execute("insert into " + tbname + " select created_at, role_name, granted_to , grantee_name, granted_by from " + tb_temp + " order by role_name") 
    cursor.execute("drop table if exists " + tb_temp)
    cursor.execute("commit ")

### update parent_child table using current information stored in a temp table
### if the current record exists in the table, update it; if not exists in the table, insert it; 
### if not exists in current temp table but exists in the parent_child table, delete it
# tb_pcrl: table name for all parent child relationship
# tb_role: the child role that's updated
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def updParent( tbname, tb_temp, role, cursor):
    if crossrep.verbose == True:
        print('updating parent_child table ...')

    # delete the ones dropped
    dquery = ( "delete from " + tbname + " tgt using (" +
        " select created_at, role_name, granted_to , grantee_name, granted_by from "+ tbname + " where role_name = '"+ role + "'" +
        "                 minus " +
        " select created_at, role_name, granted_to , grantee_name, granted_by from "+ tb_temp + " where role_name = '"+ role + "'" +
        " ) as src where tgt.role_name = src.role_name and  " + 
        " ( tgt.granted_to = src.granted_to or (tgt.granted_to is null and src.granted_to is null) ) and " + 
        "  ( tgt.grantee_name = src.grantee_name or (tgt.grantee_name is null and src.grantee_name is null ) ) and  " + 
        "  (tgt.granted_by = src.granted_by or ( tgt.granted_by is null and src.granted_by is null) )" ) 

    if crossrep.verbose == True:
        print(dquery)
    cursor.execute(dquery)
    cursor.execute("commit")

    # insert the delta of new information 
    iquery = ( "insert into " + tbname + " select created_at, role_name, granted_to , grantee_name, granted_by from " + tb_temp + 
        " where created_at > (select IFNULL(max(created_at), TO_TIMESTAMP_LTZ('1900-01-01 00:00:00.000 -0000'))  from " + tbname+ " where role_name = '" + role + "' ) order by created_at") 
    cursor.execute(iquery)
    cursor.execute("commit")

    cursor.execute("drop table if exists " + tb_temp)
    cursor.execute("commit")
    if crossrep.verbose == True:
        print('Finish updating parent_child table ...')   

### create table privileges for privilege relationships between object and role
### if the current record exists in the table, update it; if not exists in the table, insert it; 
### if not exists in current temp table but exists in the parent_child table, delete it
# tb_priv : privileges table storing all privileges information
# role: the role's granted privileges that's created
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def crPriv( cursor):
#def crPriv(tbpriv, tbroles,cursor):
    # create an separate connection for multi-station transactions
    if crossrep.mode == 'CUSTOMER':
        contx = crossrep.getConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_pwd, crossrep.default_wh, crossrep.default_rl)
    elif crossrep.mode == 'SNOWFLAKE':
        contx = crossrep.getSFConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_wh, crossrep.default_rl)
    cs = contx.cursor()
    cs.execute("USE DATABASE "+crossrep.default_db)
    cs.execute("USE SCHEMA "+crossrep.default_sc)

    tbpriv = crossrep.tb_priv
    tbroles = crossrep.tb_role
    isCreated = False
    
    checkquery = ("select count(*) from information_schema.tables where table_schema = '" + crossrep.default_sc + "' and table_name = '" 
    + tbpriv + "' and table_owner is not null")
    if crossrep.verbose == True:
        print("current database: " + crossrep.default_db + "; current schema:" +  crossrep.default_sc )
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            isCreated = False
            cs.execute("begin")
            cs.execute("show grants to role " + crossrep.acctpref_qualifier + "ACCOUNTADMIN")
            cs.execute("create or replace table " + tbpriv + " as select $1 created_at, $2 priv, $3 object_type , $4 object_name, $6 role, $7 grant_option from table(result_scan(last_query_id())) order by object_name, object_type ") 
            cs.execute("commit")
        else:
            isCreated = True
    
    if isCreated == False:
        query = "select distinct role_name from " + tbroles + " where role_name != 'ACCOUNTADMIN' order by role_name "
    else:
        #query = "select distinct role_name from " + tbroles + " where role_name = 'AMARKIS_USER_ROLE' order by role_name "
        query = "select distinct role_name from " + tbroles + " order by role_name "
    cursor.execute(query)
    if crossrep.verbose == True:
        print(query)
    rec_set = cursor.fetchall()
    for rec in rec_set:
        role = rec[0]
        tb_temp =  '"'+'TEMP_'+ tbpriv+'_'+role.strip('"') + '"'
        role = '"'+role.strip('"') + '"'

        squery = "show grants to role "+  crossrep.acctpref_qualifier + role
        cquery = ("create or replace table " + tb_temp +
                " as select distinct $1 created_at, $2 priv, $3 object_type , $4 object_name, $6 role, $7 grant_option from table(result_scan(last_query_id())) order by object_name, object_type" )

        if crossrep.verbose == True:
            print(" tb_temp_role: " + tb_temp )
            print(squery)
            print(cquery)
        cs.execute("begin ")
        cs.execute(squery)
        cs.execute(cquery)
        cs.execute("commit")

        if isCreated == False:
            # Process each insert in parallelized batches...
            #t1 = threading.Thread(target=insPrivByRole, args=(tbpriv, tb_temp, ctx))
            t1 = threading.Thread(target=insPrivByRole, args=(tbpriv, tb_temp, cursor))
            t1.setDaemon(True)
            t1.start()
        else:
            t2 = threading.Thread(target=updPriv, args=(tbpriv, tb_temp, role,cursor))
            #t2 = threading.Thread(target=updPriv, args=(tb_temp, role))
            t2.setDaemon(True)
            t2.start()
            #updPriv(tbpriv, tb_temp, role)
    
    main_thread = threading.currentThread()
    for t in threading.enumerate():
        if t is main_thread:
            continue
        if crossrep.verbose:
            print('Completed %s', t.getName())
        t.join()

    cs.close()
    contx.close()
    if crossrep.verbose == True:
        print ('Finishing create privilege table ' )

#def insPrivByRole( tbpriv, tb_temp ,contx):
def insPrivByRole( tbpriv, tb_temp ,cursor):
    cursor.execute("insert into " + tbpriv + " select created_at, priv, object_type , object_name, role, grant_option from " + tb_temp + " order by object_name, object_type ") 
    cursor.execute("drop table if exists " + tb_temp)
    cursor.execute("commit ")
    #cs.close()
    #contx.close()

### update privileges table using current information stored in a temp table
### merge is slow, use insert or delete only 
### if the current record exists in the table, update it; if not exists in the table, insert it; 
### if not exists in current temp table but exists in the parent_child table, delete it
# tbpriv : privileges table storing all privileges information
# role: the role's granted privileges that's updated
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
#def updPriv( tb_temp, role):
#def updPriv(tbpriv, tb_temp, role, contx):
def updPriv(tbpriv, tb_temp, role, cs):
    if crossrep.verbose == True:
        print('updating privilege table')

    # delete the ones dropped
    dquery = ( "delete from " + tbpriv + " tgt using (" +
        " select created_at, priv, object_type , object_name, role from "+ tbpriv + " where role = '"+ role + "'" +
        "                 minus " +
        " select created_at, priv, object_type , object_name, role from "+ tb_temp + " where role = '"+ role + "'" +
        "  ) as src where tgt.role = src.role and tgt.priv = src.priv and tgt.object_type = src.object_type and tgt.object_name = src.object_name") 
    if crossrep.verbose == True:
        print(dquery)
    cs.execute(dquery)
    cs.execute("commit")

    # insert the delta of new information 
    cs.execute("insert into " + tbpriv + " select created_at, priv, object_type , object_name, role, grant_option from " + tb_temp +
         " where created_at > (select IFNULL(max(created_at), TO_TIMESTAMP_LTZ('1900-01-01 00:00:00.000 -0000'))  from " + tbpriv+ " where role = '" + role + "' ) order by object_name, object_type ") 
    cs.execute("commit")
    
    cs.execute("drop table if exists " + tb_temp)
    if crossrep.verbose == True:
        print('Finish updating privilege table')
    #cs.close()
    #contx.close()

### create table futALL_FGRANTSure_grants for privileges granted for future objects in the schema 
# tb_fgrant : future grant table storing all future grant information
# tbschema: the schema table storing all schema 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
#def crFutureGrants(tbname, tbschema,cursor):
def crFGrant(cursor):
    if crossrep.verbose == True:
        print('Creating future grants table ...')

    if crossrep.mode == 'CUSTOMER':
        contx = crossrep.getConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_pwd, crossrep.default_wh, crossrep.default_rl)
    elif crossrep.mode == 'SNOWFLAKE':
        contx = crossrep.getSFConnection(crossrep.default_acct, crossrep.default_usr, crossrep.default_wh, crossrep.default_rl)
    cs = contx.cursor()
    cs.execute("USE DATABASE "+crossrep.default_db)
    cs.execute("USE SCHEMA "+crossrep.default_sc)

    tbname = crossrep.tb_fgrant
    isCreated = False
    isInsert = True 
     
    checkquery = ("select count(*) from information_schema.tables where table_schema = '" + crossrep.default_sc + "' and table_name = '" 
    + tbname + "' and table_owner is not null")
    if crossrep.verbose == True:
        print("current database: " + crossrep.default_db + "; current schema:" +  crossrep.default_sc )
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] == 0:
            # create and insert
            isCreated = False
            isInsert = True 
        else:
            # update
            isCreated = True
            isInsert = False 

    if crossrep.mode == 'CUSTOMER':
        query = (" select distinct CATALOG_NAME, SCHEMA_NAME from snowflake.account_usage.SCHEMATA where SCHEMA_OWNER is not null and DELETED is null " + 
        " and CATALOG_NAME not in ('WORKSHEETS_APP') order by CATALOG_NAME, SCHEMA_NAME ")
    elif crossrep.mode == 'SNOWFLAKE':
        query = (" select distinct CATALOG_NAME, SCHEMA_NAME from " + crossrep.tb_sc + " where SCHEMA_OWNER is not null order by CATALOG_NAME, SCHEMA_NAME ")
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    record = cursor.fetchall()
    for row in record:
        if crossrep.isBlank(row[0]) or crossrep.isBlank(row[1]):
            continue
        dname = crossrep.quoteID(row[0])
        sname = crossrep.quoteID(row[1])
        tb_temp = "\"TEMP_" + tbname+"_"+re.sub(r'\s+',r'_',row[0]) +"_"+re.sub(r'\s+',r'_',row[1])+"\""
       
        scname = crossrep.acctpref_qualifier + dname+"."+sname
        if crossrep.verbose == True:
            print('schema '+ scname)
            print('dname: ' + dname+"; sname:" + sname)

        cs.execute("begin")
        showq = ( "show future grants in schema "+ scname )
        if crossrep.verbose == True:
            print(showq)
        rowcount = cs.execute(showq)
        if ( rowcount != None):
            cs.execute(" create or replace table " + tb_temp +
                " as select distinct '"+dname+"'::string dbname, '"+sname+"'::string scname, $1 created_at, $2 priv, $3 object_type , $4 object_name, $6 grantee_name, $7 grant_option from table(result_scan(last_query_id())) order by $1, $4 " )
            cs.execute("commit")
            
            if  isCreated == False :
                cs.execute(" create or replace table " + tbname +
                " as select dbname, scname, created_at,  priv, object_type , object_name, grantee_name, grant_option from " + tb_temp)
                isCreated = True
            else:
                if isInsert == True:
                    # Process each insert in parallelized batches...
                    #t1 = threading.Thread(target=insPrivByRole, args=(tbpriv, tb_temp, ctx))
                    t1 = threading.Thread(target=insFGrant, args=(tbname, tb_temp, cursor))
                    t1.setDaemon(True)
                    t1.start()
                else:
                    t2 = threading.Thread(target=updFGrant, args=(tbname, tb_temp, dname, sname,cursor))
                    #t2 = threading.Thread(target=updFGrant, args=(tbname, tb_temp, scname,cursor))
                    #t2 = threading.Thread(target=updPriv, args=(tb_temp, role))
                    t2.setDaemon(True)
                    t2.start()
                    #updPriv(tbpriv, tb_temp, role)
        
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is main_thread:
                continue
            if crossrep.verbose:
                print('Completed %s', t.getName())
            t.join()

    cs.close()
    contx.close()
    if crossrep.verbose == True:
        print('Finish creating future grants table')

### Insert ALL_FGRANTS table using current information stored in a temp table
# tb_fgrant : ALL_FGRANTS table storing all future grants information
# tb_temp: temp table storing the current ALL_FGRANTS information (real time)
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def insFGrant( tbname, tb_temp ,cursor):
    cursor.execute("insert into " + tbname + " select dbname, scname, created_at, priv, object_type , object_name, grantee_name, grant_option from " + tb_temp + " order by created_at, object_name") 
    cursor.execute("drop table if exists " + tb_temp)
    cursor.execute("commit ")

### update ALL_FGRANTS table using current information stored in a temp table
### if the current record exists in the table, update it; if not exists in the table, insert it; 
### if not exists in current temp table but exists in the ALL_FGRANTS table, delete it
# tb_fgrant : ALL_FGRANTS table storing all future grants information
# tb_temp: temp table storing the current ALL_FGRANTS information (real time)
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def updFGrant(tbname, tb_temp,dbname, scname,cursor):
    if crossrep.verbose == True:
        print('updating ALL_FGRANTS table')

    # delete the ones dropped
    dquery = ( "delete from " + tbname + " tgt using (" +
        " select dbname, scname, created_at, priv, object_type , object_name, grantee_name from "+ tbname  + " where object_name like '"+ scname + "%'" +
        "                 minus " +
        " select dbname, scname, created_at, priv, object_type , object_name, grantee_name from "+tb_temp + 
        "  ) as src where tgt.priv = src.priv and tgt.object_type = src.object_type and tgt.object_name = src.object_name and tgt.grantee_name = src.grantee_name" )
    iquery = ("insert into " + tbname + " select dbname, scname, created_at, priv, object_type , object_name, grantee_name, grant_option from " + tb_temp +
         " where created_at > (select IFNULL(max(created_at), TO_TIMESTAMP_LTZ('1900-01-01 00:00:00.000 -0000'))  from " + tbname+ 
         " ) and  dbname = '" + dbname+ "' and scname = '"+ scname + "' order by object_name, object_type ")
    #" where object_name like '"+ scname + "%' ) order by object_name, object_type ")

    if crossrep.verbose == True:
        print(dquery)
        print(iquery)
    cursor.execute(dquery)
    cursor.execute("commit")

    # insert the delta of new information 
    cursor.execute(iquery)
    cursor.execute("commit")
    
    cursor.execute("drop table if exists " + tb_temp)
    if crossrep.verbose == True:
        print('Finish updating ALL_FGRANTS table')

### disable and enable all users 
# tb_user : all users table storing all users information
# ofile1: file to write for disabling user command 
# ofile2: file to write for enabling user command 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def disUsers(ofile1, ofile2, cursor):
    tbuser = crossrep.tb_user
    if crossrep.verbose == True:
        print('Start generating stmt to alter all user set them disabled  ')
    ofile1.write('use role security;\n')    
    ofile2.write('use role security;\n')    
    query = "select distinct user_name from " + tbuser 
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        gsql1 = "alter user if exists " + name +" SET DISABLED = TRUE ;\n "
        ofile1.write(gsql1)    
        gsql2 = "alter user if exists " + name +" SET DISABLED = FALSE ;\n "
        ofile2.write(gsql2)  
    if crossrep.verbose == True:
        print('Finish generating stmt to alter all user set them disabled/enabled ')

### drop all roles 
# tb_role : all role table storing all roles information
# ofile: file to write for droping role command 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def dropRoles(ofile, cursor):
    tbrole = crossrep.tb_role
    if crossrep.verbose == True:
        print('Start generating stmt to drop role  ')
    #ofile.write('use role accountadmin;\n')        

    query = "select distinct role_name from " + tbrole + " where role_name not in ('ACCOUNTADMIN','SECURITYADMIN','SYSADMIN','PUBLIC')"
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        sql = "drop role if exists " + name +" ;\n "
        ofile.write(sql)    
        
    if crossrep.verbose == True:
        print('Finish generating stmt to drop role  ')    

### drop all users with exluding list of users
# excludeList: list of users that's excluded from dropping
# tb_user : all users table storing all users information
# ofile: file to write for dropping user command 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def dropUsers(excludeList, ofile, cursor):
    tbuser = crossrep.tb_user
    if crossrep.verbose == True:
        print('Start generating stmt to drop user  ')
    ofile.write(" use role accountadmin;\n ")        
        
    query = "select distinct user_name from " + tbuser 
    inPred = crossrep.list2InPredicate(excludeList)
    if crossrep.isBlank(inPred) == False:
        query = query + " where user_name not " + inPred
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        name = r[0]
        if crossrep.hasSpecial(name):
            name = '"'+name+'"'
        sql = "drop user if exists " + name +" ;\n "
        ofile.write(sql)    
 
    if crossrep.verbose == True:
        print('Finish generating stmt to drop user  ')    

### updating dropped role in PARENT_CHILD and PRIVILEGES tables only if PARENT_CHILD and PRIVILEGES tables have been created
def updDroppedRole( cursor):
    checkquery = ("select count(*) from information_schema.tables where table_schema = '" + crossrep.default_sc + "' and table_name = '" 
    + crossrep.tb_pcrl + "' and table_owner is not null")
    if crossrep.verbose == True:
        print("current database: " + crossrep.default_db + "; current schema:" +  crossrep.default_sc )
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] > 0:
            dquery1 = 'delete from '+crossrep.tb_pcrl + ' where role_name not in ( select role_name from ' + crossrep.tb_role + ')'
            if crossrep.verbose == True:
                print(dquery1)
            cursor.execute(dquery1)
     
    checkquery = ("select count(*) from information_schema.tables where table_schema = '" + crossrep.default_sc + "' and table_name = '" + crossrep.tb_priv + "' and table_owner is not null")
    if crossrep.verbose == True:
        print("current database: " + crossrep.default_db + "; current schema:" +  crossrep.default_sc )
        print(checkquery)
    cursor.execute(checkquery)
    rec = cursor.fetchall()
    for r in rec:
        if r[0] > 0:
            dquery2 = 'delete from '+crossrep.tb_priv + ' where role not in ( select role_name from ' + crossrep.tb_role + ')'
            if crossrep.verbose == True:
                print(dquery2)
            cursor.execute(dquery2)
