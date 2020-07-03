#!/usr/bin/env python3
import glob

# by warehouse name, by snowsql config file
def genSubprocess(ssconfig, fname, seq, whname, ofile):
    ofile.write("\tif os.stat(\""+fname+"\").st_size != 0:\n" )
    ofile.write("\t\tcmd"+ seq + " = \"snowsql -c " + ssconfig+ " -r repadmin  -w " +whname + " -f " + fname+" -o output_file=" + fname+"_out -o quiet=true & \" \n")
    ofile.write("\t\tout"+ seq + " = subprocess.check_output(cmd"+seq+",stderr=subprocess.STDOUT, shell=True)\n\n")

i = 0
ddllist = [f for f in glob.glob("01_dbDDL_*.sql")]

of = open('./autoDDL.py', 'w')
of.write('import subprocess, os\n')
of.write('try:\n')

for ofname in ddllist: 
    i += 1
    genSubprocess('migwest', ofname, str(i), 'WH_CROSSREP', of)

of.write('except subprocess.CalledProcessError as e:\n')
of.write('\tprint(e)\n')
of.close()


