-- this preparation job is an example script for target account where you run generated scripts
-- the main point is that role needs to have MANAGE GRANTS privilege at minimum, accountadmin will work of course.
use role securityadmin ;

create role if not exists  repadmin ;
grant MANAGE GRANTS on account to role repadmin;
use role accountadmin;
-- grant role  accountadmin to role repadmin ;
grant IMPORTED PRIVILEGES on database snowflake to role repadmin ;
grant create database on account to role repadmin ;
grant create warehouse on account to role repadmin ;

grant role repadmin to user xxx ;

use role repadmin ;
CREATE WAREHOUSE IF NOT EXISTS WH_CROSSREP WAREHOUSE_SIZE = 'XLARGE' AUTO_RESUME=TRUE AUTO_SUSPEND=600;
create database if not exists  DB_CROSSREP ;
create schema if not exists  SC_CROSSREP ;

use database DB_CROSSREP;
use schema SC_CROSSREP;
create or replace procedure refresh_database(db varchar)
    returns varchar not null
    language javascript
    execute as caller
    as
    $$
    var result = "";
    try {
        stmt = snowflake.createStatement(
            {sqlText: "alter database " + DB + " refresh;"});
        rs = stmt.execute();
        }
    catch (err) {
        result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
        result += "\n  Message: " + err.message;
        result += "\nStack Trace:\n" + err.stackTraceTxt; 
        }
    return result;
    $$
    ;

show procedures;

-- File format and Stage is only needed if you are replicating manually 
-- using loading and unloading, instead of using snowflake replication engine
-- create the following in both source and target accounts
CREATE OR REPLACE FILE FORMAT mycsvformat
  TYPE = 'CSV'
  SKIP_HEADER = 1
  RECORD_DELIMITER='\n'
  FIELD_DELIMITER='|'
  SKIP_HEADER=1
  DATE_FORMAT=AUTO
  TIME_FORMAT=AUTO
  TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF9'
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  NULL_IF=('NULL')
  EMPTY_FIELD_AS_NULL=FALSE
  COMPRESSION=AUTO
  ERROR_ON_COLUMN_COUNT_MISMATCH=TRUE
;


 create or replace stage sfcsupport_csv url='s3://sfcsupport/migva/'
    credentials=(aws_key_id='xxx' aws_secret_key='xxx'); 
    
-- list to check what's in the stage
   ls @sfcsupport_csv ;

-- for manual unloading/loading process only
CREATE WAREHOUSE IF NOT EXISTS WH_CROSSREP_SMALL WAREHOUSE_SIZE = 'SMALL' AUTO_RESUME=TRUE AUTO_SUSPEND=900 MIN_CLUSTER_COUNT=2 MAX_CLUSTER_COUNT=10 ;
CREATE WAREHOUSE IF NOT EXISTS WH_CROSSREP_LARGE WAREHOUSE_SIZE = 'LARGE' AUTO_RESUME=TRUE AUTO_SUSPEND=900 MIN_CLUSTER_COUNT=2 MAX_CLUSTER_COUNT=10 ;
CREATE WAREHOUSE IF NOT EXISTS WH_CROSSREP_XLARGE WAREHOUSE_SIZE = 'XLARGE' AUTO_RESUME=TRUE AUTO_SUSPEND=900 MIN_CLUSTER_COUNT=2 MAX_CLUSTER_COUNT=10 ;
CREATE WAREHOUSE IF NOT EXISTS WH_CROSSREP_2XLARGE WAREHOUSE_SIZE = 'XXLARGE' AUTO_RESUME=TRUE AUTO_SUSPEND=900 MIN_CLUSTER_COUNT=2 MAX_CLUSTER_COUNT=10 ;
