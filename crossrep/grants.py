#!/usr/bin/env python3
import crossrep,re
"""
Created on March 27, 2019
Updated on July 2, 2020
Updated on Sept 5, 2020
@author: Minzhen Yang
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'
# *********************************************************************************************************************
# 
# This module contains the Snowflake-specific logic of generating grants commands as well as all DDLs of users and roles:
#   The following user tables are created to store account level objects information.
#  - all_shares, all_warehouses, all_resmonitors, all_networkpolicies
#  - their DDLs that can be generated , then to be executed in target system are as follows:
#    scripts/rbac/21_create_users.sql           => DDL to create all users
#    scripts/rbac/22_create_roles.sql           => DDL to create all roles
#    scripts/rbac/23_grant_roles.sql            => grants to grant roles/users to other roles
#    scripts/rbac/24_grant_target_roles.sql     => grants ownership of TARGET_ROLE to all roles (to avoid prilege issues during replication)
#    scripts/rbac/25_grant_owner_dblevel.sql    => grant ownership for all objects in all databases
#    scripts/rbac/26_grant_privs_dblevel.sql    => grant privileges for all objects in all databases
#    scripts/rbac/25_grantowner_dblevel_<dbfile>.sql  => grant ownership for all objects in the database list as in <dbfile>
#    scripts/rbac/26_grantpriv_dblevel_<dbfile>.sql   => grant privileges for all objects in the database list as in <dbfile>
#    scripts/rbac/27_future_grants.sql          => grant all future grants
#
# *********************************************************************************************************************


### without password for SSO
### generating all users' DDL using all_users table information
# options: nopwd-no password, samepwd-same password for all users (cr0ss2REP) or randpwd-random password
# tb_user: table name for all users information
# ofile: output file for all user DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genUserDDL(options, ofile, cursor):
    user_name = None
    login_name = None
    display_name = None
    first_name = None
    last_name = None
    email = None
    mins_to_unlock = None
    days_to_expiry = None
    comment = None
    
    query = ( "select user_name,login_name,display_name,first_name,last_name,email,mins_to_unlock,days_to_expiry,comment, "+ 
    "   must_change_password, snowflake_lock,  default_warehouse, default_namespace,  default_role from " + crossrep.tb_user + " order by user_name ")
        #" from " + tbusers+ " where disabled = false order by owner, user_name "
    
    ofile.write("use role securityadmin;\n")
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        #user_name = quoteID(r[0])
        user_name = r[0]
        login_name = r[1]
        display_name = r[2]
        first_name = r[3]
        last_name = r[4]
        email = r[5]
        mins_to_unlock = r[6]
        days_to_expiry = r[7]
        comment = r[8]  
        must_change_password = r[9] 
        default_warehouse = r[11] 
        default_namespace = r[12]   
        default_role = r[13] 
        if user_name.isdigit() == True:
            continue
        if crossrep.hasSpecial(user_name) == True:
            user_name = "\"" + user_name + "\"" 
            if crossrep.verbose==True:
                print(' user name: ' + user_name)
        cuserSQL = "CREATE USER IF NOT EXISTS " + user_name 
        if not crossrep.isBlank (login_name) :
            cuserSQL = cuserSQL + " login_name='" + login_name + "'"
        if not crossrep.isBlank (display_name ):
            cuserSQL = cuserSQL + " display_name='" + display_name + "'"
        if not crossrep.isBlank (first_name ):
            cuserSQL = cuserSQL + " first_name='" + first_name + "'"
        if not crossrep.isBlank (last_name ):
            cuserSQL = cuserSQL + " last_name='" + last_name + "'"
        if not crossrep.isBlank (email ):
            cuserSQL = cuserSQL + " email='" + email + "'"
        if not crossrep.isBlank (mins_to_unlock ):
            cuserSQL = cuserSQL + " mins_to_unlock=" + mins_to_unlock 
        if not crossrep.isBlank (days_to_expiry ):
            days_to_expiry = str(int(float(days_to_expiry)))
            cuserSQL = cuserSQL + " days_to_expiry=" + days_to_expiry 
        if not crossrep.isBlank (comment ):
            cuserSQL = cuserSQL + ' comment=\'' + comment + '\''
        if not crossrep.isBlank (default_warehouse ):
            cuserSQL = cuserSQL + ' default_warehouse=\'' + default_warehouse + '\''
        if not crossrep.isBlank (default_namespace ):
            cuserSQL = cuserSQL + ' default_namespace=\'' + default_namespace + '\''
        if not crossrep.isBlank (default_role ):
            cuserSQL = cuserSQL + ' default_role=\'' + default_role + '\''
        if crossrep.verbose == True:
            print(cuserSQL)
        if options == 'samepwd':
            cuserSQL = cuserSQL + " password='cr0ssREP'  MUST_CHANGE_PASSWORD=TRUE "
        elif options == 'randpwd':
            pwd = crossrep.genPWD()
            cuserSQL = cuserSQL + " password='" + pwd + "' MUST_CHANGE_PASSWORD=TRUE "
        ofile.write(cuserSQL+';\n')

### generating all roles' DDL using all_roles table information
# tb_role: table name for all roles information
# ofile: output file for all role DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def genRoleDDL( ofile, cursor):
    role_name = None
    comment = None
    ofile.write("use role securityadmin;\n")
    
    query = "select role_name, comments from "+ crossrep.tb_role + " where role_name not in ('ACCOUNTADMIN','SECURITYADMIN','SYSADMIN','PUBLIC') order by role_name  "
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        role_name = r[0]
        if crossrep.hasSpecial(role_name) == True:
            role_name = "\"" + role_name + "\""
            if crossrep.verbose==True:
                print(' role name: ' + role_name)

        comment = r[1]  
        if role_name.isdigit()==True:
            continue
        croleSQL = "CREATE ROLE IF NOT EXISTS " + role_name 
        if not crossrep.isBlank (comment ):
              croleSQL = croleSQL + ' comment =  "' + comment + '"'
        ofile.write(croleSQL+';\n')

### generating all roles to its parent roles/users 
# tb_pcrl: table name for all parent role relationship information
# tb_role: table name for all roles information
# ofile: output file for all role DDL statement 
# cursor: cursor connects to your snowflake account where it creates tables to store metadata
def grantAllRoles(ofile, cursor):
    query = ( "select role_name, granted_to, grantee_name, granted_by from " + 
    '"' + crossrep.tb_pcrl + '"' + " where role_name != 'PUBLIC' and "
    " (role_name not in ('ACCOUNTADMIN', 'SECURITYADMIN','SYSADMIN')  OR grantee_name not in ('ACCOUNTADMIN', 'SECURITYADMIN','SYSADMIN', 'PUBLIC') ) " +
    " order by role_name ,granted_to, grantee_name ")

    '''
    # SNOW-84205 do not use assigned_to_users data
    query = ( "select role_name, granted_to, grantee_name, granted_by from " + 
    tbgrants + " where role_name in " + 
    "(select role_name from " + tbroles + " where granted_roles != 0 or assigned_to_users != 0 ) " +
    "   and role_name != 'PUBLIC' and "
    " (role_name not in ('ACCOUNTADMIN', 'SECURITYADMIN','SYSADMIN')  OR grantee_name not in ('ACCOUNTADMIN', 'SECURITYADMIN','SYSADMIN', 'PUBLIC') ) " +
    " order by role_name ,granted_to, grantee_name ")
    '''
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        role_name = r[0]
        granted_to = r[1]
        grantee_name = r[2]
        granted_by = r[3]
        if role_name.isdigit() == True or grantee_name.isdigit() == True:
            continue
        if crossrep.hasSpecial(role_name):
            role_name = '"'+role_name+'"'
        if crossrep.hasSpecial(grantee_name):
            role_name = '"'+grantee_name+'"'
        grantSQL = 'GRANT ROLE ' + role_name + ' TO '+ granted_to + ' ' + grantee_name  
        ofile.write(grantSQL+';\n')
        if crossrep.verbose == True:
            print(grantSQL)

### add double quotes if there is lower case or space in the identifier or isKeywords and not quoted
# id : idenifier that's being handled
def quoteID(id):
    if re.search(r'[a-z]', id) or ' ' in id or crossrep.isKeywords(id)==True or crossrep.hasSpecial(id)==True:
        if re.match(r'^[^"]',id):
            return '"%s"' % id
        else:
            return id
    else :
        return id

### K NUMBER => NUMBER
### removing argument name, leaving only data type for function
# parm: input string
# return the data type only of the function
def remArgName(parm):
    parm=parm.strip()
    list = parm.split()
    if len(list) < 2:
        return parm
    return list[1]

'''###function handling: ON FUNCTION NAME (NOT DB OR SC NAME QUALIFIER)
PLAYPEN4J_CLONED.PUBLIC."KELVIN_TO_FAREN(K NUMBER):NUMBER(38,0)"
==>
PLAYPEN4J_CLONED.PUBLIC.KELVIN_TO_FAREN(K NUMBER)
==>
PLAYPEN4J_CLONED.PUBLIC."KELVIN_TO_FAREN"(NUMBER)
'''
### handling function identifier, leaving database and schema name as it is
# id: identifier input
# return the function identifier after removing function input parameter name and its returning data types
def funcID(id):
    id=re.sub(r'\"([^:]+):.+',r'\1',id)
    match = re.search(r'([^\(]+)\(([^\)]+)\)$',id)
    if match:
        fname = match.group(1)
        parm=match.group(2)
        parm = ','.join([remArgName(s) for s in parm.split(',')])
        return '"'+fname+'"'+'('+parm+')'
    else:
        return id

### handling identifier with its quote issue if there is any lower case chars
# objname: object name with its fully qualified name
# isFunction: boolean whether the object is a function or not (True or False)
# return the handled identifier by quoting the identifier if there is lower case and handling the function idenfitier
def quoteIdentifier(objname,isFunction):
    if isFunction:
        idlist = re.split('\.', objname)
        if len(idlist) == 3:
            return '.'.join([idlist[0], idlist[1], funcID(idlist[2]) ])
        else:
            return objname
    else:
        if not re.search(r'[a-z]', objname):
            return objname
        else:
            return '.'.join([quoteID(s) for s in re.split('\.', objname)])

### grant owner role for the specified database and all of the objects within the database
# tb_priv : table name for all privileges information
# dbname : database name whose object owners are transfered as the same as the owners in the source system
# ofile : output file for the GRANT OWNERSHIP scripts
# cursor: cursor connection into the source system
def grantOwnerByDatabase(dbname, ofile, cursor):
    if crossrep.verbose == True:
        print('starting grant ownership on database: ' + dbname)
    if crossrep.isBlank(dbname):
        return
    inPredicate = ( " and ( (object_type = 'DATABASE' and object_name = '"+ dbname + "') or ( object_type != 'DATABASE' and object_name like '" +
         dbname + ".%' ) )")

    query = ( "select distinct object_type, object_name, role from " +
        crossrep.tb_priv + " where priv = 'OWNERSHIP' and object_type not in ('ACCOUNT','MANAGED_ACCOUNT','ROLE','USER','SHARE', 'WAREHOUSE','NETWORK_POLICY','RESOURCE_MONITOR','STAGE') " + 
        inPredicate + " order by object_name,object_type, role ")
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()

    for r in rec:
        object_type = r[0]
        object_name = r[1]
        ownerRole = r[2]

        if crossrep.verbose == True:
            print('object_name: '+ object_name + '; object_type: ' + object_type + '; owner role:' + ownerRole )
        command_object_name = object_name
        command_object_type = object_type
        if object_type == 'NOTIFICATION_SUBSCRIPTION' :
            continue
        elif object_type == 'FUNCTION' :
            #if object_type == 'FUNCTION' and priv == 'USAGE':
            command_object_name = quoteIdentifier(object_name,True)
            if crossrep.verbose == True:
                print('function: '+ object_name + '; ' + command_object_name )
        else:
            command_object_name = quoteIdentifier(object_name,False)
            command_object_type = objTypeHandling(object_type)

        grantSQL = 'grant ownership on ' + command_object_type + ' ' + command_object_name + ' TO ROLE ' + ownerRole  + ' copy current grants ;'
        ofile.write(grantSQL + '\n')
        if crossrep.verbose == True:
            print(grantSQL)
    if crossrep.verbose == True:
        print('Finish grant ownership on database: ' + dbname + ' ... \n')

### Grant privileges based on privileges in the previous system for a specific databases and all of the objects within the databases
# tb_priv : table name for all privileges information
# dbname: database name and the objects within the database that grants are generated against
# ofile : output file for the GRANT privileges scripts
# cursor: cursor connection into the source system
def grantPrivsByDatabase( dbname, ofile, cursor):
    ## query to get owner role and granted role for certain object type, object name, and privilege
    ## for grant privileges, only need to grant on the role that's different from its owner role
    if crossrep.verbose == True:
        print('starting grant privilege on database: ' + dbname + ' ...')
    if crossrep.isBlank(dbname):
        return
    inPredicate = ( " and ( (object_type = 'DATABASE' and object_name = '"+ dbname + "') or ( object_type != 'DATABASE' and object_name like '" +
         dbname + ".%' ) )")

    privquery = ("select p1.object_type, p1.object_name, p1.priv, p1.role , p2.role as ownerrole " +
        " from (" +
        " select distinct object_type, object_name, role, priv from privileges where priv != 'OWNERSHIP' " + inPredicate +
        " and object_type not in ('ACCOUNT','MANAGED_ACCOUNT','ROLE','USER','SHARE','WAREHOUSE','NETWORK_POLICY','RESOURCE_MONITOR', 'STAGE','PIPE', 'NOTIFICATION_SUBSCRIPTION') ) p1" +
        " join  (" +
        "      select distinct object_type, object_name, role from privileges " +
        "        where priv = 'OWNERSHIP' " + inPredicate +
        "        and object_type not in ('ACCOUNT','MANAGED_ACCOUNT','ROLE','USER','SHARE','WAREHOUSE','NETWORK_POLICY','RESOURCE_MONITOR', 'STAGE', 'PIPE', 'NOTIFICATION_SUBSCRIPTION')" +
        "    ) p2 on p1.object_type = p2.object_type" +
        "    and p1.object_name = p2.object_name" +
        "    where p1.role != p2.role" +
        " order by p1.object_name,p1.object_type,p1.priv,p1.role" ) 

    if crossrep.verbose == True:
        print(privquery)    

    cursor.execute(privquery)
    record = cursor.fetchall()
    for row in record:
        object_type = row[0]
        object_name = row[1]
        priv = row[2]
        role = row[3]
        ownerRole = row[4]

        command_object_type = object_type
        command_object_name = object_name
        if crossrep.verbose == True:
            print(object_type, object_name, ownerRole)
            print(command_object_type, command_object_name)

        if object_type == 'FUNCTION' or object_type == 'PROCEDURE' :
            command_object_name = quoteIdentifier(object_name,True)
            if crossrep.verbose == True:
                print('function: '+ object_name + '; ' +command_object_name )
            '''
        elif object_type == 'NOTIFICATION_SUBSCRIPTION' :
            continue
            '''
        else:
            command_object_name = quoteIdentifier(object_name,False)
            command_object_type = objTypeHandling(object_type)

        grantSQL = 'GRANT  ' + priv + ' ON '+ command_object_type + ' ' + command_object_name + ' TO ROLE ' + role  
        ofile.write(grantSQL+';\n')
        if crossrep.verbose == True:
            print(grantSQL)
    if crossrep.verbose == True:
        print('Finish grant privilege on database: ' + dbname + ' ... \n')

### grant owner role for all databases and all of the objects within the database excluding inbound databases
# crossrep.tb_priv : table name for all privileges information
# excludePredicate: predicate excluding inbound databases and its objects
# ofile : output file for the GRANT OWNERSHIP scripts
# cursor: cursor connection into the source system
def grantAllOwners( excludePredicate, ofile, cursor):
    query = ( "select distinct object_type, object_name, role from " +
        crossrep.tb_priv + " where priv = 'OWNERSHIP' " + excludePredicate + " order by object_name,object_type, role ")
    if crossrep.verbose == True:
        print(query)
    cursor.execute(query)
    rec = cursor.fetchall()

    for r in rec:
        object_type = r[0]
        object_name = r[1]
        ownerRole = r[2]
        if object_name.find("ABI_WH.CSDI_C.GET_INVALID_VIEWS()") >=0:
            print("Found target")
        command_object_name = object_name
        command_object_type = object_type

        if crossrep.verbose == True:
            print('object_name: '+ object_name + '; object_type: ' + object_type + '; owner role:' + ownerRole )
        if object_type == 'NOTIFICATION_SUBSCRIPTION' :
            continue
        elif object_type == 'FUNCTION' or object_type == 'PROCEDURE' :
            #if object_type == 'FUNCTION' and priv == 'USAGE':
            colon_location = object_name.find(':')
            if colon_location > 0:
                command_object_name = quoteIdentifier(object_name[:colon_location], True) + object_name[colon_location:]
            else:
                command_object_name = quoteIdentifier(object_name,True)
            if crossrep.verbose == True:
                print('function: '+ object_name + '; ' + command_object_name )
        else:
            command_object_name = quoteIdentifier(object_name,False)
            command_object_type = objTypeHandling(object_type)
            if crossrep.verbose == True:
                print('new object_name: '+ command_object_name + '; new object_type: ' + command_object_type )

        grantSQL = 'grant ownership on ' + command_object_type + ' ' + command_object_name + ' TO ROLE ' + ownerRole  + ' copy current grants ; \n'
        ofile.write(grantSQL)
        if crossrep.verbose == True:
            print(grantSQL)

### convert object type format from information schema/show commands/account_usage to the object type needed for grant commands
# object_type : input object type from information schema/show commands/account_usage
# output object type for grant commands
def objTypeHandling(object_type):
    if object_type == 'FILE_FORMAT':
        object_type = 'FILE FORMAT'
    elif object_type == 'EXTERNAL_TABLE':
        object_type = 'EXTERNAL TABLE'
    elif object_type == 'RESOURCE_MONITOR':
        object_type = 'RESOURCE MONITOR'
    elif object_type == 'MATERIALIZED_VIEW':
        object_type = 'VIEW'
    elif object_type == 'NETWORK_POLICY':
        object_type = 'NETWORK POLICY'
    elif object_type == 'NOTIFICATION_SUBSCRIPTION':
        object_type = 'NOTIFICATION SUBSCRIPTION'
    return object_type

### generate excluding predicate to exclude share inbound database and objects within the inbound DB
def genExcludePredicate(shareDBlist):
    excludePredicate = ''
    if len(shareDBlist) == 0:
        return excludePredicate
    else:
        dbinlist = ','.join("'"+db+"'" for db in shareDBlist )
        excludePredicate = " and object_name not in (" + dbinlist + ") "
        for db in shareDBlist:
            excludePredicate = excludePredicate + " and object_name not like '" + db+ ".%' "

        return excludePredicate

### generate in predicate to include a in list for a column 
# colname: column name for the in predicate
# alist: the list for the in ( ... ) predicate
def genInPredicate(colname, alist):
    if len(alist) > 0:
        alist = ','.join("'"+obj+"'" for obj in alist )
        inPredicate = " and " + colname + " in (" + alist + " )" 
    else:
        inPredicate = ''
    return inPredicate


### grant all privilege on account 
# crossrep.tb_priv: table name contains privilege information
# ofile : output file for the GRANT OWNERSHIP scripts
# cursor: cursor connection into the source system
def grantAcctPrivs( ofile, cursor):
    query = "select distinct role, priv from privileges where object_type ='ACCOUNT'"
    cursor.execute(query)
    rec = cursor.fetchall()
    ownerRole = ''
    for r in rec:
        role = r[0]
        priv = r[1]
        grantSQL = 'GRANT  ' + priv + ' ON ACCOUNT TO ROLE ' + role  
        ofile.write(grantSQL+';\n')
        if crossrep.verbose == True:
            print(grantSQL)

### Grant all privileges based on privileges in the previous system for all databases and all of the objects within the databases
# crossrep.tb_priv : table name for all privileges information
# excludePredicate: excluding all unsupported object type and inbound databases  
# ofile : output file for the GRANT OWNERSHIP scripts
# pfile : output file for the GRANT privileges scripts
# cursor: cursor connection into the source system
def grantAllPrivs( excludePredicate, ofile, cursor):
    ## query to get owner role and granted role for certain object type, object name, and privilege
    ## for grant privileges, only need to grant on the role that's different from its owner role
    privquery = ("select p1.object_type, p1.object_name, p1.priv, p1.role, p2.role as ownerrole" +
        " from (" +
        " select distinct object_type, object_name, role, priv from privileges where priv != 'OWNERSHIP' " + excludePredicate +
        "  ) p1" +
        " join  (" +
        "      select distinct object_type, object_name, role from privileges " +
        "        where priv = 'OWNERSHIP' " + excludePredicate +
        "    ) p2 on p1.object_type = p2.object_type" +
        "    and p1.object_name = p2.object_name" +
        "    where p1.role != p2.role" +
        " order by p1.object_name,p1.object_type,p1.priv,p1.role" ) 
    '''
    privquery = ("select p1.object_type, p1.object_name, p1.priv, p1.role as ownerrole " +
        " from (" +
        " select distinct object_type, object_name, role, priv from privileges where priv != 'OWNERSHIP' " + excludePredicate +
        " and object_type not in ('ACCOUNT','MANAGED_ACCOUNT','ROLE','USER','SHARE','WAREHOUSE','NETWORK_POLICY','RESOURCE_MONITOR', 'STAGE') ) p1" +
        " join  (" +
        "      select distinct object_type, object_name, role from privileges " +
        "        where priv = 'OWNERSHIP' " + excludePredicate +
        "        and object_type not in ('ACCOUNT','MANAGED_ACCOUNT','ROLE','USER','SHARE','WAREHOUSE','NETWORK_POLICY','RESOURCE_MONITOR', 'STAGE')" +
        "    ) p2 on p1.object_type = p2.object_type" +
        "    and p1.object_name = p2.object_name" +
        "    where p1.role != p2.role" +
        " order by p1.object_name,p1.object_type,p1.priv,p1.role" ) 
    '''
    if crossrep.verbose == True:
        print(privquery)      
    cursor.execute(privquery)
    record = cursor.fetchall()
    for row in record:
        object_type = row[0]
        object_name = row[1]
        priv = row[2]
        role = row[3]
        ownerRole = row[4]

        command_object_type = object_type
        command_object_name = object_name
        if crossrep.verbose == True:
            print(object_type, object_name, ownerRole)
        if crossrep.verbose == True:
            print(command_object_type, command_object_name)

        if object_type == 'FUNCTION' or object_type == 'PROCEDURE' :
            command_object_name = quoteIdentifier(object_name,True)
            if crossrep.verbose == True:
                print('function: '+ object_name + '; ' +command_object_name )
        elif object_type == 'NOTIFICATION_SUBSCRIPTION' :
            return
        else:
            command_object_name = quoteIdentifier(object_name,False)
            command_object_type = objTypeHandling(object_type)

        grantSQL = 'GRANT  ' + priv + ' ON '+ command_object_type + ' ' + command_object_name + ' TO ROLE ' + role  
        ofile.write(grantSQL+';\n')
        if crossrep.verbose == True:
            print(grantSQL)

### generating grant commands for future grants
# tb_fgrant: future grant priv table for future grants information
# dblist: database list whose future grants to be generated , empty list for all databases 
# ofile: output file to store future grant commands 
# cursor: connection to source snowflake account
def grantFutureObj (dblist, ofile, cursor):
    inPredicate = ''

    if len(dblist) != 0:
        inPredicate = ','.join("'"+ dbname +"'" for dbname in dblist )
        inPredicate = " and dbname  in (" + inPredicate + ") "

    gquery = ( "select distinct object_name, object_type, priv, grantee_name, grant_option from " + crossrep.tb_fgrant + 
    " where priv in ('OWNERSHIP','SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'USAGE', 'READ', 'WRITE','REBUILD')"
    " and object_type in ('TABLE','VIEW', 'STAGE', 'FILE_FORMAT', 'FUNCTION', 'PROCEDURE', 'SEQUENCE')" 
    + inPredicate + " order by object_name,  object_type ")
    if crossrep.verbose==True:
        print(gquery)
    cursor.execute(gquery)
    row_set = cursor.fetchall()
    for row in row_set:
        obj_name = row[0].split('.')
        obj_type = row[1]
        priv = row[2]
        role = row[3]
        grant_option = row[4]
        db = obj_name[0]
        sc = obj_name[1]
        with_grant = ''
        if not crossrep.isBlank(grant_option):
            if grant_option == 'true':
                with_grant = ' WITH GRANT OPTION'
        
        if priv == 'OWNERSHIP':
            grantSQL = ( "GRANT  ALL ON FUTURE "+ obj_type + "S IN SCHEMA " + db + "." + sc + " TO ROLE " + role + with_grant )
            ofile.write(grantSQL+';\n')
            #print(grantSQL)
        else:
            if obj_type == 'FILE_FORMAT':
                grantSQL = ( "GRANT  " + priv + " ON FUTURE FILE FORMATS IN SCHEMA " + db + "." + sc + " TO ROLE " + role + with_grant )
            else:
                grantSQL = ( "GRANT  " + priv + " ON FUTURE "+ obj_type + "S IN SCHEMA " + db + "." + sc + " TO ROLE " + role + with_grant )
            ofile.write(grantSQL+';\n')
            #print(grantSQL) 
        if crossrep.verbose==True:
            print(grantSQL)

### grant both ownership and privileges by object type
# object_type: object type for grant against
# crossrep.tb_priv : table name for all privileges information
# ofile : output file for the GRANT scripts (both grant ownership and privileges)
# cursor: cursor connection into the source system
def grantByObjType(object_type, ofile, cursor):
    command_object_type = object_type
    # capture the query before object_type and object_name being handled
    privquery = ( "select distinct object_name , priv, role from " + crossrep.tb_priv + 
        " where object_type = '" + object_type + "' order by object_name, role")
    cursor.execute(privquery)
    record = cursor.fetchall()
    for row in record:
        obj_name = row[0]
        priv = row[1]
        role = row[2]
    
        command_object_name = obj_name
        if object_type == 'FUNCTION' :
            command_object_name = quoteIdentifier(obj_name,True)
            if crossrep.verbose == True:
                print('function: '+ objecobj_namet_name + '; ' +command_object_name )
        elif object_type == 'NOTIFICATION_SUBSCRIPTION' :
            return
        else:
            command_object_name = quoteIdentifier(obj_name,False)
            command_object_type = objTypeHandling(object_type)
        
        if object_type == 'ACCOUNT':
            command_object_name = ''

        # Only need to grant if grantee is different from owner role !!
        if priv == 'OWNERSHIP' :
            grantSQL = 'grant ownership on ' + command_object_type + ' ' + command_object_name + ' TO ROLE ' + role  + ' copy current grants ; \n'
            ofile.write(grantSQL)
            if crossrep.verbose == True:
                print(grantSQL)

        else:
            grantSQL = 'GRANT  ' + priv + ' ON '+ command_object_type + ' ' + command_object_name + ' TO ROLE ' + role  
            ofile.write(grantSQL+';\n')
            if crossrep.verbose == True:
                print(grantSQL)

## grant all ownership role to target_role
def grantTargetRole(ofile, cursor):
    trole = crossrep.getEnv('TARGET_ROLE')
    query = ("select distinct role from "+crossrep.tb_priv+" where priv = 'OWNERSHIP' and role != 'PUBLIC' order by role")
    #print(query)
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        rname = r[0]
        if crossrep.hasSpecial(rname):
            rname = '"' + rname + '"'
        ofile.write('GRANT ROLE '+ rname + ' TO ROLE ' + trole + ';\n')

### grant privileges on object types in objectTypelist
# crossrep.tb_priv : table name for all privileges information
# objectTypelist: object type list to be included in generating grant privilege 
# ofile : output file for the GRANT privilege scripts
# cursor: cursor connection into the source system
def grantPrivsByAcctObjects( objectTypelist, ofile, cursor):
    inPredicate = genInPredicate('OBJECT_TYPE',objectTypelist)
    privquery = ("select p1.object_type, p1.object_name, p1.priv, p1.role  " +
        " from (" +
        " select distinct object_type, object_name, role, priv from privileges where priv != 'OWNERSHIP' "  + inPredicate + " ) p1" +
        " join  (" +
        "      select distinct object_type, object_name, role from privileges " +
        "        where priv = 'OWNERSHIP' " + inPredicate +
        "    ) p2 on p1.object_type = p2.object_type" +
        "    and p1.object_name = p2.object_name" +
        "    where p1.role != p2.role" +
        " order by p1.object_name,p1.object_type, p1.role" ) 

    if crossrep.verbose == True:
        print(privquery)      
    cursor.execute(privquery)
    record = cursor.fetchall()
    for row in record:
        object_type = row[0]
        object_name = row[1]
        priv = row[2]
        role = row[3]

        command_object_type = object_type
        command_object_name = object_name
        if crossrep.verbose == True:
            print(object_type, object_name, ownerRole)
        if crossrep.verbose == True:
            print(command_object_type, command_object_name)

        if object_type == 'FUNCTION' or object_type == 'PROCEDURE' :
            command_object_name = quoteIdentifier(object_name,True)
            if crossrep.verbose == True:
                print('function: '+ object_name + '; ' +command_object_name )
        elif object_type == 'NOTIFICATION_SUBSCRIPTION' :
            return
        else:
            command_object_name = quoteIdentifier(object_name,False)
            command_object_type = objTypeHandling(object_type)

        grantSQL = 'GRANT  ' + priv + ' ON '+ command_object_type + ' ' + command_object_name + ' TO ROLE ' + role  
        ofile.write(grantSQL+';\n')
        if crossrep.verbose == True:
            print(grantSQL)


### grant owner on object by object type list
# crossrep.tb_priv: table name contains privilege information
# objectTypelist: object type list to be included in generating grant privilege 
# ofile : output file for the GRANT OWNERSHIP scripts
# cursor: cursor connection into the source system
def grantOwnerByAcctObjects( objectTypelist, ofile, cursor):
    inPredicate = genInPredicate('OBJECT_TYPE', objectTypelist)
    query = ( "select distinct object_type, object_name, role from " +
      crossrep.tb_priv + " where priv = 'OWNERSHIP' "  + inPredicate + " order by object_name,object_type,role ")

    if crossrep.verbose == True:
        print(query)

    cursor.execute(query)
    rec = cursor.fetchall()
    ownerRole = ''
    for r in rec:
        object_type = r[0]
        object_name = r[1]
        ownerRole = r[2]

        command_object_name = object_name
        command_object_type = object_type
        if object_type == 'NOTIFICATION_SUBSCRIPTION' :
            continue
        elif object_type == 'FUNCTION' :
            #if object_type == 'FUNCTION' and priv == 'USAGE':
            command_object_name = quoteIdentifier(object_name,True)
            if crossrep.verbose == True:
                print('function: '+ object_name + '; ' + command_object_name )
        else:
            command_object_name = quoteIdentifier(object_name,False)
            command_object_type = objTypeHandling(object_type)


        if object_name.isdigit() == True or ownerRole.isdigit() == True:
            continue
        grantSQL = 'grant ownership on ' + command_object_type + ' ' + command_object_name + ' TO ROLE ' + ownerRole  + ' copy current grants ; \n'
        ofile.write(grantSQL)
        if crossrep.verbose == True:
            print(grantSQL)
