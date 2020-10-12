import os
import re
import sys
from datetime import datetime
import snowflake.connector
from snowflake.connector import DictCursor 
import glob
from os import listdir
from os.path import isfile, join
import fnmatch
import threading
import traceback
import time 
import logging 
import string
from codecs import open
import math 
import queue 
import json 
from copy import deepcopy

USE_CREATE_IF_NOT_EXISTS = 1
USE_CREATE_OR_REPLACE = 2

def list_scripts(migHome, objectType=''):
    l_list = [] # changed variable name from list to l_list to make sure keywords are not used for variable names
    root_folder = os.path.join(migHome, "scripts")
    if len(objectType) > 0:
        root_folder = os.path.join(root_folder, objectType)
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".sql") and file.startswith("01_dbDDL"): 
                # sort_on = "key2"
                l_dict = {}
                l_dict['file'] = file
                l_dict['path'] = root
                l_list.append(l_dict)
    return sorted(l_list, key=lambda i: os.path.getsize(os.path.join(root_folder,i['file'])), reverse=False)


def use_create_object_if_not_exists(ddl):
    if ddl.lower().find("replace") >= 0:
        #print("inside replace code ....")
        ddl2 = re.sub(r'create\s+(or\s+replace\s+)?(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|temporary table|stage|pipe|stream|task|masking policy)\s+'
                     , r'create \2 if not exists ', ddl, flags=re.MULTILINE | re.IGNORECASE)
        ddl2 = re.sub(r'create\s+(or\s+replace\s+)?(transient\s+)?(database|schema)+\s+(\S+)(;)?(\s+)?'
                 # , r'create \2\3 if not exists \4;\nuse \3 \4', ddl, flags=re.MULTILINE | re.IGNORECASE)
                 , r'create \2\3 if not exists \4; use \3 \4 ', ddl2, flags=re.MULTILINE | re.IGNORECASE)
        ddl2 = ddl2.replace(';;',';')
    else:
        ddl2 = ddl
    #print(ddl2)
    return ddl2

def use_create_or_replace_object(ddl):
    if ddl.lower().find("exists") >= 0:
        ddl2 = re.sub(r'create\s+(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|temporary table|stage|pipe|stream|task|masking policy)\s+if\s+not\s+exists\s+'
                     , r'create or replace \1 ', ddl, flags=re.MULTILINE | re.IGNORECASE)
        ddl2 = re.sub(r'create\s+(transient\s+)?(database|schema)\s+if\s+not\s+exists\s+(\S+)(\s+)?'
                 # , r'create or replace \1\2 \3;\nuse \2 \3', ddl, flags = re.MULTILINE | re.IGNORECASE)
                 , r'create or replace \1\2 \3', ddl2, flags=re.MULTILINE | re.IGNORECASE)
    else:
        ddl2 = ddl
    return ddl2

def match_create_db_or_schema(statement, option, current_db_schema):
    statement_type = ''
    object_name = ''
    if option == USE_CREATE_IF_NOT_EXISTS:
        # check if object name has double-quotes - doing it as a separate check to not break existing logic
        m = re.match(r'create\s+(transient\s+)?(database|schema)\s+if\s+not\s+exists\s+(\".+?\")(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if not m: 
            m = re.match(r'create\s+(transient\s+)?(database|schema)\s+if\s+not\s+exists\s+(\S+)(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = str(m.groups()[1]).upper()
            object_name = m.groups()[2]

    else:
        # check if object name has double-quotes - doing it as a separate check to not break existing logic
        m = re.match(r'create\s+(or\s+replace\s+)?(transient\s+)?(database|schema)\s+(\".+?\")(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if not m:
            m = re.match(r'create\s+(or\s+replace\s+)?(transient\s+)?(database|schema)\s+(\S+)(\s+)?', statement,
                     flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = m.groups()[2].upper()
            object_name = m.groups()[3]
    if object_name != '':
        current_db_schema[statement_type] = object_name
        statement_type = "CREATE " + statement_type
    return statement_type

def match_create_db_object(statement, option):
    statement_type = ''
    object_name = ''
    if option == USE_CREATE_IF_NOT_EXISTS:
        m = re.match(
            r'create\s+(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|temporary table|stage|pipe|stream|task|masking policy)(\s+if\s+not\s+exists\s+)?'
            , statement
            ,flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = str(m.groups()[0]).upper()
    else:
        m = re.match(
            r'create\s+(or\s+replace\s+)?(table|view|materialized view|secure view|file format|function|external function|sequence|procedure|external table|transient table|temporary table|stage|pipe|stream|task|masking policy)\s+'
            , statement
            , flags=re.MULTILINE | re.IGNORECASE)
        if m:
            statement_type = str(m.groups()[1]).upper()

    if statement_type != '':
        statement_type = "CREATE " + statement_type
    return statement_type

def add_failed_statement_to_queue(con, query_id, filename, failed_queries_queue):
    query_ids_str = f"'{query_id}'"
    sql_stmt = f"""
                select 
                    query_id
                    , query_text as statement 
                    , null as statement_type 
                    , database_name as cur_database 
                    , schema_name as cur_schema 
                    , error_message 
                from table(snowflake.information_schema.query_history_by_session(result_limit=>10))
                where query_id in ({query_ids_str})
                """
    cur = con.cursor(DictCursor)
    cur.execute(sql_stmt)
    for rec in cur:
        # custom replaces for special situations 
        stmt = rec['STATEMENT'].replace("ENCODING = iso-8859-1", "ENCODING = 'iso-8859-1'")
        # end of custom replaces
        failed_queries_queue.put({
            'statement': stmt,
            'statement_type':'',
            'success': False,
            'error_message': rec['ERROR_MESSAGE'], 
            'cur_database': rec['CUR_DATABASE'],
            'cur_schema': rec['CUR_SCHEMA'],
            'query_id': query_id,
            'filename': filename 
            }
        )

def record_status(
        con, 
        return_type,
        status_object,
        filename, 
        logger, 
        failed_queries_queue, 
        return_message=None,
        cur=None,
        exception_record=None):
    if return_type == 'error':
        e = exception_record 
        status_dict = {}
        status_dict['return_message'] = e.msg
        status_dict['query_id'] = e.sfqid
        status_dict['rowcount'] = 0
        status_dict['sqlstate'] = e.sqlstate
        status_dict['errorcode'] = e.errno 
        status_dict['messages'] = e.msg
        status_dict['filename'] = filename
        status_dict['captured_ts'] =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_object['failed_statements'].append(status_dict) 
        #add_failed_statement_to_queue(con, e.sfqid, filename, failed_queries_queue)
        #failed_queries_queue.put(status_dict)
        logger.error(json.dumps(status_dict))
    else:
        status_dict = {}
        status_dict['return_message'] = return_message 
        status_dict['query_id'] = cur.sfqid
        status_dict['rowcount'] = cur.rowcount 
        status_dict['messages'] = cur.messages
        status_dict['filename'] = filename
        status_dict['captured_ts'] =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_object['other_statements'].append(status_dict)
    
def process_stream(con, filename, status_object, curr_pos, logger, failed_queries_queue, continue_on_error=False):
    eof_reached = False
    f = open(filename, 'r', encoding='utf-8') 
    while not eof_reached:
        try:
            for cur in con.execute_stream(f):
                for ret in cur:
                    #print(ret)
                    record_status(con, 'success', status_object, filename, logger, failed_queries_queue, return_message=ret[0], cur=cur)
            con.commit() 
            f.close() 
            eof_reached = True 
        except Exception as e:
            status_object['error_exists'] = 'Y'
            status_object['error_counter'] += 1 
            record_status(con, 'error', status_object, filename, logger, failed_queries_queue, exception_record=e)
            if continue_on_error: 
                eof_reached = False
            else:
                eof_reached = True 
                con.rollback()
                f.close() 
            if status_object['error_counter'] > math.pow(10,9):
                break

# grab a logger
def createLogger(loggerName, log_dir):
   logger = logging.getLogger(loggerName)
   logger.setLevel(logging.DEBUG)
   log_filename = os.path.join(log_dir, loggerName+'.log')
   handler = logging.FileHandler(log_filename)
   #handler = logging.FileHandler(f"/tmp/{loggerName}.log")
   handler.setLevel(logging.DEBUG)
   formatter = logging.Formatter('%(asctime)s  %(threadName)-16s  %(name)-16s %(levelname)-8s %(message)s')
   handler.setFormatter(formatter)
   logger.addHandler(handler)
   return logger

# change lines with multiple spaces followed by ; to ;
# replace // with -- in lines that begin with // and has odd number of single quotes
def cleanse_files(sqlfiles, tmp_dir, tmp_sqlfiles, option):
    for sqlfile in sqlfiles: 
        #print(sqlfile)
        tmp_sqlfile = os.path.join(tmp_dir, os.path.basename(sqlfile['file']))
        with open(os.path.join(sqlfile['path'], sqlfile['file']), "r") as f:
            s = f.read() 
            s1 = re.sub(r'^\s*;',r';',s,flags=re.MULTILINE | re.IGNORECASE)
            s2_arr = []
            for line in s1.split('\n'):
                m = re.match(r"^\s*\/\/.*[^\']\'[^\'].*", line)
                single_quote_count = line.count("'")
                if m and single_quote_count%2 > 0: 
                    line1 = re.sub(r"(^\s*)(\/\/)(.*)", r"\1--\3",line)
                    #print(line1)
                else:
                    line1 = line 
                s2_arr.append(line1)
            s2 = '\n'.join(s2_arr)
            #print(s1)
            if option == USE_CREATE_OR_REPLACE:
                s3 = use_create_or_replace_object(s2)
            else:
                # create or replace <object>
                s3 = use_create_object_if_not_exists(s2)
            with open(tmp_sqlfile,"w") as outf:
                outf.write(s3)
        tmp_sqlfiles.append(tmp_sqlfile)

# worker function: run the workload against the warehouse
# processes each file as a stream of sql statements
# get failed statements from information_schema.query_history 
# add the statements to a queue as queue is thread safe
def workload(worker_id, con, files_arr, continue_on_error, failed_queries_queue,log_dir,job_starttime):
    err_message = ''
    status_object = {'error_counter':0, 'error_exists': 'N', 'failed_statements':[], 'other_statements':[]}
    worker_logger = createLogger("worker-{id}-{job_starttime}".format(id=worker_id,job_starttime=job_starttime),log_dir)
    worker_logger.info("starting work")
    total_worker_duration = 0 
    try: 
        for filename in files_arr:
            failed_statements = [] 
            t0 = datetime.now()
            curr_pos = len(status_object['failed_statements'])
            base_filename = os.path.basename(filename)
            logger = createLogger("{filename}-worker-{id}-{job_starttime}".format(filename=base_filename,id=worker_id,job_starttime=job_starttime),
                log_dir)
            logger.info("starting work")
            t_cur = con.cursor()
            t_cur.execute('select current_timestamp::varchar as end_time_range_start')
            t_cur.execute(f"alter session set query_tag='{base_filename}'")
            end_time_range_start = t_cur.fetchone()[0]
            try:
                # process_stream 
                process_stream(con, filename, status_object, curr_pos, logger, failed_queries_queue, continue_on_error)
                logger.info(f"number of failed statements: {len(status_object['failed_statements'][curr_pos:])}")
            except Exception as e:
                logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)) 
            t1 = datetime.now()
            duration = (t1-t0).total_seconds()
            total_worker_duration += duration 
            logger.debug(f"Worker-{worker_id} finished executing file:{filename} after {duration} seconds")
        worker_logger.debug(f"worker-{worker_id} finished executing in {total_worker_duration} seconds")
    except Exception as ex:
        worker_logger.warning("error in workload %s: %s"%(str(ex), traceback.format_exc()))
    finally:
        con.close()

def retry_failed_statements(cursor, failed_statements, logger, verbose=False, mode='DR'):
    next_retry = []
    next_retry_queryids = {}
    retry_list = [item for item in failed_statements if item['statement'].strip() != ';']
    cur_schema = ''
    cur_database = ''
    cur_query_tag = '' 
    current_db_schema = {}
    loop_no = 1

    retry_sequence = ['SEQUENCE', 'FILE FORMAT', 'TABLE', 'VIEW', 'STAGE', 'PROCEDURE', 'FUNCTION', 'STREAM',
                      'TASK', 'PIPE', '']
    #retry_sequence = ['']
    while len(retry_list) > 0:
        loop_no += 1
        if verbose or mode == 'DR_TEST':
            logger.info("Retry statement loop #" + str(loop_no))
        for selected_type in retry_sequence:
            for item in retry_list:
                if item['filename'] != cur_query_tag:
                    cur_query_tag = item['filename']
                    cursor.execute(f"ALTER SESSION SET QUERY_TAG='{cur_query_tag}-Retry {str(loop_no)}'")
                if "cur_database" not in item.keys() or "cur_schema" not in item.keys() or "statement" not in item.keys() or "statement_type" not in item.keys():
                    # ignore the bad items
                    continue
                if loop_no <= 2:
                    statement_type = match_create_db_or_schema(item["statement"], USE_CREATE_OR_REPLACE, current_db_schema)
                    if len(statement_type) == 0:
                        statement_type = match_create_db_object(item["statement"], USE_CREATE_OR_REPLACE)
                    statement_type = statement_type.upper() 
                    item["statement_type"] = statement_type 
                else:
                    statement_type = item["statement_type"].upper()

                if (selected_type == '' or statement_type.find(selected_type) > 0) and not item["success"] and item not in (next_retry): 
                    item["current_loop"] = loop_no
                    try:
                        if item["cur_database"] != cur_database:
                            cur_database = item["cur_database"]
                            cursor.execute('USE DATABASE "' + cur_database + '"')
                            # reset old schema so that it will execute USE SCHEMA
                            cur_schema = ""
                        if item["cur_schema"] != cur_schema:
                            cur_schema = item["cur_schema"]
                            cursor.execute('USE SCHEMA ' + cur_schema)
                        if verbose:
                            print("-----Retry statement --->\n" + item["statement"])
                        else:
                            pass 
                            #logger.info("-- retry: %s ..." % item["statement"][:60])
                        cursor.execute(item["statement"])
                        if verbose:
                            print("----- succeeded -!")
                        item["success"] = True
                    except snowflake.connector.errors.ProgrammingError as e:
                        item["error"] = str(e)
                        if item not in next_retry:
                            next_retry.append(item)
                            next_retry_queryids[item["query_id"]] = e.sfqid 
                        pass
        logger.info(f"count of previous retry_list vs next_retry: {len(retry_list)} - {len(next_retry)}")
        if len(retry_list) == len(next_retry):
            # this means no more statement can be run successfully
            #logger.info(json.dumps(retry_list)) 
            break
        else:
            retry_list = next_retry
            next_retry = []
    for elem in retry_list:
        elem["last_retry_query_id"] = next_retry_queryids[elem["query_id"]]  
    return retry_list

def get_session_id(con):
    cur = con.cursor(DictCursor)
    cur.execute('select current_session() as session_id')
    rec = cur.fetchone()
    return rec['SESSION_ID']

def get_failed_statements(con, session_id, start_time, end_time, failed_statements):
    print(session_id)
    current_end_time = end_time 
    while True:
        print(f"current end time: {current_end_time}")
        sql_stmt = f"""
                    select 
                        query_id
                        , query_text as statement 
                        , null as statement_type 
                        , database_name as cur_database 
                        , schema_name as cur_schema 
                        , error_message 
                        , execution_status 
                        , query_tag 
                        , start_time::varchar as start_time 
                        , (min(end_time) over ())::varchar as min_end_time 
                    from table(snowflake.information_schema.query_history_by_session(result_limit=>10000,
                        session_id=>{session_id}, 
                        end_time_range_start=>'{start_time}'::timestamp_ltz, 
                        end_time_range_end=>'{current_end_time}'::timestamp_ltz
                    ))
                    --where execution_status <> 'SUCCESS' 
                    order by database_name, schema_name 
                    """
        print(sql_stmt)
        cur = con.cursor(DictCursor)
        cur.execute(sql_stmt)
        current_rowcount = cur.rowcount
        print(f"current rowcount: {current_rowcount}")
        ind = 0 
        for rec in cur:
            if ind == 0: current_end_time = rec['MIN_END_TIME']
            #print(f"ind: {ind}, status: {rec['EXECUTION_STATUS']}")
            if rec['EXECUTION_STATUS'] != 'SUCCESS':
                # custom replaces for special situations 
                #print("found a failed statement")
                stmt = rec['STATEMENT'].replace("ENCODING = iso-8859-1", "ENCODING = 'iso-8859-1'")
                # end of custom replaces
                failed_statements.append({
                    'statement': stmt,
                    'statement_type':'',
                    'success': False,
                    'error_message': rec['ERROR_MESSAGE'], 
                    'cur_database': rec['CUR_DATABASE'],
                    'cur_schema': rec['CUR_SCHEMA'],
                    'query_id': rec['QUERY_ID'],
                    'filename': rec['QUERY_TAG'],
                    'execution_status': rec['EXECUTION_STATUS'],
                    'start_time': rec['START_TIME']
                    }
                )
            ind+=1
        if current_rowcount < 10000:
            break 

"""
  - gets the list of files from a directory 
  - cleanses the files 
  - splits into batches based on thread_count: for example: if thread_count=4, num_of_files/thread_count is the batch size
  - retry failed statements in serial fashion to make sure dependencies are handled properly
"""
def upload_multithreaded(thread_count, mode, con, creds, sqlfiles, tmp_dir, log_dir, verbose, continue_on_error=False, option=USE_CREATE_IF_NOT_EXISTS):
    status_object = {'error_counter':0, 'error_exists': 'N', 'failed_statements':[], 'other_statements':[]}
    failed_sql_filename = os.path.join(log_dir, 'failed_queries.log')
    failed_queries_queue = queue.Queue()
    failed_statements = []
    tmp_sqlfiles = []  
    thread_list = [] 
    session_id_arr = [] 
    t0 = datetime.now()
    print(t0)
    #l_cur = con.cursor()
    #l_cur.execute('ALTER SESSION SET CLIENT_SESSION_KEEP_ALIVE=TRUE') # keep session alive as workers can take a long time 
    job_starttime = t0.strftime("%Y%m%d_%H%M%S")
    logger=createLogger(f"scriptloader-upload_multithreaded-{job_starttime}",log_dir)
    cleanse_files(sqlfiles,tmp_dir,tmp_sqlfiles,option) # cleanse the files and write out the new files to tmp_dir 
    logger.info('started with {t} threads for queries on warehouse {w}'.format(t=thread_count, w=creds['warehouse']))
    # start workload
    batch_size = math.ceil(len(tmp_sqlfiles)/thread_count)
    #print(len(tmp_sqlfiles))
    #print(batch_size)
    for j in range(thread_count):
        curr_file_arr = [] 
        #print(f"inside thread_count for loop: {j}")
        start_pos = j*batch_size
        end_pos = min((j+1)*batch_size, len(tmp_sqlfiles))
        curr_file_arr = tmp_sqlfiles[start_pos:end_pos]
        #print(f"start - end: start_pos, end_pos")
        #print(curr_file_arr)
        curr_session_id = ''
        thread_con = snowflake.connector.connect(
            user=creds['user'], 
            password=creds['password'], 
            account=creds['account'],
            warehouse=creds['warehouse'],
            role=creds['role']
        )
        curr_session_id = get_session_id(thread_con) 
        if curr_session_id:
            session_id_arr.append(curr_session_id)
        t = threading.Thread(
                target=workload, 
                args=(j,thread_con,curr_file_arr,continue_on_error,failed_queries_queue,log_dir,job_starttime), 
                name=(f"WorkerThread-{j}"))
        thread_list.append(t)
        t.start()
    # wait for threads to finish
    for t in thread_list:
        t.join()
    print("finished processing all files ")
    print(datetime.now())
    failed_query_count = 0 
    retry_list = []
    """
    failed_statements = list(failed_queries_queue.queue)
    while True:
        try:
            retry_list.append(failed_queries_queue.get(False))
        except queue.Empty:
            failed_queries_queue.task_done() 
            break 
    """
    retry_con = snowflake.connector.connect(
            user=creds['user'], 
            password=creds['password'], 
            account=creds['account'],
            warehouse=creds['warehouse'],
            role=creds['role']
    )
    time.sleep(30) # sleep for 30 seconds for Information_Schema to get fully updated 
    for session_id in session_id_arr:
        get_failed_statements(retry_con, session_id , t0.strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), retry_list) 
    logger.info(f"number of failed queries: {len(retry_list)}")
    logger.info("finished get_failed_statements")
    print(datetime.now()) 
    #sys.exit() 
    #print(retry_list) 
    with open(failed_sql_filename+'.before_retry', "w") as ff:
        for sq in retry_list:
            #print("inside loop")
            #print(sq)
            ff.write(sq['statement']+'\n')
            ff.write('/* \n')
            sq_copy = deepcopy(sq)
            sq_copy.pop('statement')
            #print(sq_copy)
            ff.write(json.dumps(sq_copy))
            ff.write('\n')
            ff.write('*/ \n')
    final_failed_list = retry_failed_statements(retry_con.cursor(),retry_list,logger)
    logger.info("finished retry_failed_statements") 
    with open(failed_sql_filename, "w") as ff:
        for sq in final_failed_list:
            ff.write(sq['statement']+'\n')
            ff.write('/* \n')
            sq_copy = deepcopy(sq)
            sq_copy.pop('statement')
            #print(sq_copy)
            ff.write(json.dumps(sq_copy))
            ff.write('\n')
            ff.write('*/ \n')
    #logger.info(final_failed_list)
    retry_con.close()
    status_object['final_failed_statements'] = final_failed_list 
    return status_object
    
# Test with following files
"""
cp 01_dbDDL_DEV_TUNING_TEAM.sql /Users/rsreenivasan/git_repos/crossrep/scripts/ddl/ ;
cp 01_dbDDL_PROD_MFG_FAB_4_ODS.sql /Users/rsreenivasan/git_repos/crossrep/scripts/ddl/ ;
cp 01_dbDDL_PROD_UTIL_DB.sql /Users/rsreenivasan/git_repos/crossrep/scripts/ddl/ ;
cp 01_dbDDL_PROD_MFG_LOAD.sql /Users/rsreenivasan/git_repos/crossrep/scripts/ddl/ ;

and file in ("01_dbDDL_TEST_WW_BEMFG_ODS_TBL.sql",
                            "01_dbDDL_TEST_WW_FEMFG_ODS_TBL.sql",
                            "01_dbDDL_TEST_WW_MXADS.sql",
                            "01_dbDDL_TEST_WW_YMS_POC.sql",
                            "01_dbDDL_WORKSHEETS_APP.sql", "01_dbDDL_DEV_TUNING_TEAM.sql", 
                            "01_dbDDL_PROD_MFG_FAB_4_ODS.sql", "01_dbDDL_PROD_UTIL_DB.sql")

Once the command: python3 main.py -m DR -replace -parallel 8 finishes, you can do the following to get the unique errors
cd $MIGRATION_HOME/logs 
grep -i 'error_message' failed_queries.log | awk -F "\"" '{print $10}' | sort -u

"""
