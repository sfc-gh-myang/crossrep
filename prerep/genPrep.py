#!/usr/bin/env python3
import argparse

def isBlank(var):
    if var == None :
        return True
    elif var.strip() == '':
        return True
    else:
        return False
        
# whname: warehouse name used
# interval: refresh interval in minutes
# dblist: list storing databases (up to 999 databases)
# crt_file: output file for create task statements
# res_file: output file for resume task statements
# sus_file: output file for suspend task statements
def genTasks( whname, interval, dblist, crt_file, res_file, sus_file):
    i = 1
    for db in dblist:
        seq = '{:03}'.format(i) 
        crt_file.write("create or replace task refreshdb" + seq+ " \n")
        crt_file.write("  warehouse = "+ whname + ", schedule = '" + interval+ " MINUTE' as \n")
        crt_file.write("  call refresh_database('" + db + "');\n")

        res_file.write("alter task refreshdb" + seq + " resume;\n ")
        sus_file.write("alter task refreshdb" + seq + " suspend;\n ")
        i += 1

    res_file.write("show tasks in database db_local;" )
    sus_file.write("show tasks in database db_local;" )

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

##### MAIN #####
### example command: python genPrep.py -d dbfile -w WH_CROSSREP -i 120
parser = argparse.ArgumentParser(description='generating DDLs to prepare for replication',
    epilog='Example: python genPrep.py -f dbfile-name')

parser.add_argument('-d', '--dbfile',  type=str,
    help='a file name with database lists ')

parser.add_argument('-w', '--whname',  type=str,
    help='provide a warehouse name ')
parser.add_argument('-i', '--interval',  type=str,
    help='provide a interval by minutes for refreshing interval ')

## testing passing in a file name with database list in it
args=parser.parse_args()
filename = args.dbfile
whname = args.whname
interval = args.interval 

#whname = 'WH_CROSSREP'
#interval = '120'
#dblist = ['TEST', 'MINDB', 'DBTEST']
dblist = readFile(filename)
crt_file = open('./crt_task.sql', 'w')
res_file = open('./res_task.sql', 'w')
sus_file = open('./sus_task.sql', 'w')

## append prep.sql to the begining to create tasks file
prep_file = open('./prep.sql', 'r')
crt_file.write( prep_file.read())

genTasks(whname, interval, dblist, crt_file, res_file, sus_file)