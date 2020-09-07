#!/usr/bin/env python3
"""
Created on Oct 1 2018
Updated on July 12th 2020
"""
__author__ = 'Minzhen Yang, Advisory Services, Snowflake Computing'

import os, re, string, subprocess, sys, getpass, random
import snowflake.connector
from .RBACMeta import crUsers
from .RBACMeta import crRoles
from .RBACMeta import crParent
from .RBACMeta import crParentPriv
from .RBACMeta import insParent
from .RBACMeta import updParent
from .RBACMeta import crPriv
from .RBACMeta import insPrivByRole
from .RBACMeta import updPriv
from .RBACMeta import crFGrant
from .RBACMeta import insFGrant
from .RBACMeta import updFGrant
from .RBACMeta import disUsers
from .RBACMeta import dropRoles
from .RBACMeta import dropUsers
from .RBACMeta import updDroppedRole
from .acctobject import crShares
from .acctobject import updShares
from .acctobject import crWarehouse
from .acctobject import genWarehouseDDL
from .acctobject import dropWarehouses
from .acctobject import susWarehouse
from .acctobject import genIPlist 
from .acctobject import descNetworkPolicy
from .acctobject import crNetworkPolicy
from .acctobject import genNetworkPolicyDDL
from .acctobject import dropNetworkPolicies
from .acctobject import crResMonitor
from .acctobject import RMtrigger
from .acctobject import genResMonitor
from .acctobject import dropResMonitors
from .acctobject import crAcctParameters
from .acctobject import setAcctParameters
from .acctobject import crAccountUsage

from .dbobject import crDatabase
from .dbobject import genDatabaseDDL
from .dbobject import dropAllDBs
from .dbobject import crSchema
from .dbobject import crStages
from .dbobject import genAllStageDDL
from .dbobject import genStageDDLByDB
from .dbobject import genStageDDLByDBList
from .dbobject import crPipes
from .dbobject import genAllPipeDDL
from .dbobject import genPipeDDLByDB
from .dbobject import genPipeDDLByDBList
from .dbobject import crTable_Constraints
from .dbobject import repFKeys
from .dbobject import FKCrossDB
from .dbobject import crTable_DefaultSequence
from .dbobject import repAllSeqTable
from .dbobject import repExTable
from .dbobject import repMVs

from .grants import genUserDDL
from .grants import genRoleDDL
from .grants import grantAllRoles
from .grants import quoteID
from .grants import remArgName
from .grants import funcID
from .grants import quoteIdentifier
from .grants import grantOwnerByDatabase
from .grants import grantPrivsByDatabase
from .grants import grantAllOwners
from .grants import objTypeHandling
from .grants import genExcludePredicate
from .grants import genInPredicate
from .grants import grantAcctPrivs
from .grants import grantAllPrivs
from .grants import grantFutureObj
from .grants import grantByObjType
from .grants import grantTargetRole

from .reportSnowhouse import repSHFKeys
from .reportSnowhouse import repSHSeqTable

from .elt import ELTByDatabase
from .elt import valCountHash

from .replication import alterAllDBs
from .replication import crGlobalDBs
from .replication import linkGlobalDBsRepGroup
from .replication import refreshGlobalDBs
from .replication import repMonitor

from .drloader import list_scripts
from .drloader import search_for_procedure_or_function
from .drloader import search_for_code_block
from .drloader import safeguard_text_block
from .drloader import neutralize_line_after_comments
from .drloader import use_create_object_if_not_exists
from .drloader import use_create_or_replace_object
from .drloader import get_statement_blocks
from .drloader import replace_semicolon_in_block
from .drloader import try_handle_statement
from .drloader import match_use_statements
from .drloader import match_create_db_or_schema
from .drloader import match_create_db_object
from .drloader import build_ddl_statements
from .drloader import retry_failed_statements
from .drloader import upload_scripts

def getEnv(name):
   if name in os.environ.keys():
      return os.environ[name]
   else:
      return None

def isEmpty(var):
    if var is None :
        return True
    elif var.strip() == '':
        return True
    elif var == 'null':
        return True
    elif var == 'NULL':
        return True
    else:
        return False

def isBlank(var):
    if var == None :
        return True
    elif var.strip() == '':
        return True
    else:
        return False

def hasSpecial(var):
    if ' ' in var or '.' in var or '@' in var or '-' in var or var[0].isdigit():
        return True
    else:
        return False

def isKeywords(var):
    klist = ['IN', 'SELECT', 'FROM', 'WHERE', 'JOIN', 'AND', 'OR','TABLE','VIEW','FUNCTION','GROUP']
    if var in klist:
        return True
    else:
        return False

# adding double quotes to a identifier in all parts of its qualifiers
# stripping the double quotes first if there is one existing
def addQuotes(scname):
    return '.'.join([ ('"' + s.strip('"') + '"') for s in re.split('\.', scname)])

# filename: file name with full path, one object each line (ie, database name, parameter name)
# return a list (ie database list, param list)
def readFile(filename):
    alist = []
    with open( filename,"r") as df:
        for db in df:
            db = db.strip()
            if isBlank(db) == False:
                alist.append(db)
    df.close()
    return alist

def getShareDB( cursor): 
    query = ( "select distinct db_name from "+ default_db + "." + default_sc + "." + tb_share+" where db_name is not null and db_name!='' " + 
    " and kind = 'INBOUND' order by db_name ")
    cursor.execute(query)
    dblist = []
    rec = cursor.fetchall()
    for r in rec:
        dblist.append(r[0])

    if len(dblist) > 0:
        ## origin column in show databases is not null if it's inbound shared database (may not in show shares)
        cursor.execute('begin')
        cursor.execute('show databases in account ' + acctpref) 
        excludePredicate = ''
        dbinlist = ','.join("'"+db+"'" for db in dblist )
        excludePredicate = " and $2 not in (" + dbinlist + ") "
        cursor.execute("select distinct $2 from table(result_scan(last_query_id()) ) where  $5 is not null and $5 != '' " + excludePredicate )
        record = cursor.fetchall()
        cursor.execute('commit')
        for row in record:
            dblist.append(row[0])
    return dblist

# creating needed DB/SC for stages, cleaning up those created objects afterwards (of2), only on mode = 'CUSTOMER'
def genTest( of1, of2, cursor):
    shareDBlist = getShareDB( cursor)
    excludePredicate = ''
    if len(shareDBlist) != 0:
        dbinlist = ','.join("'"+db+"'" for db in shareDBlist )
        excludePredicate = " and stage_catalog not in (" + dbinlist + ") "

    query = ("select distinct stage_catalog, stage_schema from snowflake.account_usage.stages  " +
    " where ( stage_owner is not null or deleted is null )" + excludePredicate + "  order by stage_catalog, stage_schema ")
    cursor.execute(query)
    rec = cursor.fetchall()
    preDB=''
    for r in rec:
        stage_catalog = r[0]
        stage_schema = r[1]  

        if stage_catalog != preDB:
            crSQL1 = "CREATE DATABASE IF NOT EXISTS  " + stage_catalog + ";\n"
            of1.write(crSQL1)
            of2.write("DROP DATABASE IF EXISTS "+ stage_catalog + ";\n")
            preDB = stage_catalog
        crSQL2 = "CREATE SCHEMA IF NOT EXISTS  " + stage_catalog + "." + stage_schema  +";\n"
        of1.write(crSQL2)

        subquery = ("select distinct stage_name from snowflake.account_usage.stages " +
        " where stage_catalog = '" + stage_catalog + "' and stage_schema = '" + stage_schema + "' order by stage_name")
        #print("stageQuery:" + query)
        cursor.execute(query)
        record = cursor.fetchall()
        for row in record:
            stage_name = r[0]
            dropSQL = "DROP STAGE IF EXISTS  " + stage_catalog + "." + stage_schema + "." + stage_name 
            of2.write(dropSQL+';\n')
    
    wquery = ("select distinct name from all_warehouses order by name ")
    cursor.execute(wquery)
    rec = cursor.fetchall()
    for r in rec:
        # for testing
        of2.write('DROP WAREHOUSE IF EXISTS '+ r[0] + ';\n')

    rquery = ("select distinct name from all_resourcemonitors order by name ")
    cursor.execute(rquery)
    rec = cursor.fetchall()
    for r in rec:
        # for testing
        of2.write('DROP RESOURCE MONITOR IF EXISTS '+ r[0] + ';\n')

    nquery = ("select distinct name from all_networkpolicies order by name")
    cursor.execute(nquery)
    rec = cursor.fetchall()
    for r in rec:
        of2.write('DROP NETWORK POLICY IF EXISTS '+ r[0] + ';\n')
    
    uquery = ("select distinct user_name from  all_users order by user_name ")
    cursor.execute(uquery)
    rec = cursor.fetchall()
    for r in rec:
        user_name = r[0]
        if ' ' in user_name or '.' in user_name:
            user_name = '"' + user_name + '"' 
        of2.write('DROP USER IF EXISTS '+ user_name + ';\n')

    query = ("select distinct role_name name from all_roles order by role_name")
    cursor.execute(query)
    rec = cursor.fetchall()
    for r in rec:
        of2.write('DROP ROLE IF EXISTS '+ r[0] + ';\n')

def getConnection(acct, usr, pwd, wh, rl):
    print('Connecting with account: '+ acct + '; user: ' + usr + '; warehouse: '+wh + '; role: '+rl)
    ctx = snowflake.connector.connect(
        user=usr, 
        password=pwd, 
        account=acct,
        warehouse=wh,
        role=rl
    )
    return ctx

def getSFConnection(acct, usr, wh, rl):
    passcode = getpass.getpass(prompt='PASSCODE>')
    ctx = snowflake.connector.connect(
        #account='snowflake.prod1.us-west-2.external-zone',
        account=acct,
        port='8085',
        protocol='https',
        user=usr,
        authenticator='externalbrowser',
        passcode=passcode,
        warehouse=wh,
        role=rl
    )
    return ctx

def list2InPredicate(alist):
    if len(alist) > 0:
        alist = ','.join("'"+obj+"'" for obj in alist )
        inPredicate = " in (" + alist + " )" 
    else:
        inPredicate = ''
    return inPredicate

def genPWD(stringLength=8):
    """Generate a random string of letters, digits and special characters """
    #password_characters = string.ascii_letters + string.digits + string.punctuation
    password_characters = string.ascii_letters + string.digits + '~!@#$%^&*|<>'
    return ''.join(random.choice(password_characters) for i in range(stringLength))

### generate in predicate to include a in list  
# alist: the list for the in ( ... ) predicate
def genInList( alist):
    if len(alist) > 0:
        alist = ','.join("'"+obj+"'" for obj in alist )
        inList = " in (" + alist + " )" 
    else:
        inList = ''
    return inList

## global variable:
source_acct = getEnv('SOURCE_ACCOUNT')
# Global variable conn_src to indicate where the connection is connecting to: 
# PRODUCTION ('PROD') or customer account ('CUST'), or SNOWHOUSE, default to 'PROD'
#conn_src = 'PROD'
verbose = False
## mode: 'CUSTOMER' or 'SNOWFLAKE' or 'SNOWHOUSE'

default_usr = ''
default_acct = ''
default_pwd = ''
default_rl = ''
default_wh = ''
passdefault_wh = ''
default_db = ''
default_sc = ''

mode = getEnv('REP_MODE') 
#setDefaultEnv(mode)

if mode == 'CUSTOMER':
    default_usr = getEnv('SRC_CUST_USER')
    default_acct = getEnv('SRC_CUST_ACCOUNT')
    default_pwd = getEnv('SRC_CUST_PWD')
    default_rl = getEnv('SRC_CUST_ROLE')
    default_wh = getEnv('SRC_CUST_WAREHOUSE')
    default_db = getEnv('SRC_CUST_DATABASE')
    default_sc = getEnv('SRC_CUST_SCHEMA')
    acctpref = ''
    acctpref_qualifier = ''
elif mode == 'SNOWFLAKE':
    default_usr = getEnv('SRC_PROD_USER')
    default_acct = getEnv('SRC_PROD_ACCOUNT')
    default_pwd = getEnv('SRC_PROD_PWD')
    default_rl = getEnv('SRC_PROD_ROLE')
    default_wh = getEnv('SRC_PROD_WAREHOUSE')
    default_db = getEnv('SRC_PROD_DATABASE')
    default_sc = getEnv('SRC_PROD_SCHEMA')
    acctpref = source_acct
    acctpref_qualifier = source_acct + '.'
elif mode == 'SNOWHOUSE':
    default_usr = getEnv('SNOWHOUSE_USER')
    default_acct = getEnv('SNOWHOUSE_ACCOUNT')
    default_rl = getEnv('SNOWHOUSE_ROLE')
    default_wh = getEnv('SNOWHOUSE_WAREHOUSE')
    default_db = getEnv('SNOWHOUSE_DATABASE')
    default_sc = getEnv('SNOWHOUSE_SCHEMA')

unsupported_Obj_list = ['ACCOUNT','MANAGED_ACCOUNT','ROLE','USER','SHARE', 'WAREHOUSE','NETWORK_POLICY','RESOURCE_MONITOR','STAGE', 'PIPE', 'EXTERNAL_TABLE', 'STREAM', 'TASK']

# define all table names for metadata
tb_wh = 'ALL_WAREHOUSES'
tb_rm = 'ALL_RESOURCEMONITORS'
tb_np = 'ALL_NETWORKPOLICIES'
tb_role = 'ALL_ROLES'
tb_user = 'ALL_USERS'
tb_pcrl = 'PARENT_CHILD' 
tb_priv = 'PRIVILEGES'
tb_share = 'ALL_SHARES'
tb_db = 'ALL_DATABASES'
tb_sc = 'ALL_SCHEMAS'
tb_constr = 'TABLE_CONSTRAINTS'
tb_seq = 'ALL_SEQ_DEFAULT'
tb_fgrant = 'ALL_FGRANTS'
tb_stage = 'ALL_STAGES'
tb_pipe = 'ALL_PIPES'
tb_gldb = 'ALL_GLOBAL_DBS'
tb_parm = 'ALL_PARMS'

fileformat_name='mycsvformat'
stage_name='sfcsupport_csv'