#!/usr/bin/env python3
import glob
'''
def genSubprocess(fname, seq, ofile):
    ofile.write("\tif os.stat("+fname+").st_size != 0:\n" )
    ofile.write("\t\tcmd"+ seq + " = \"snowsql -c migva -r repadmin  -f \"" + fname+"\" -o output_file=\"" + fname+".out \" -o quiet=true & \" \n")
    ofile.write("\t\tout"+ seq + " = subprocess.check_output(cmd"+seq+",stderr=subprocess.STDOUT, shell=True)\n\n")
'''

# by warehouse name, by snowsql config file
def genSubprocess(ssconfig, fname, seq, whname, ofile):
    ofile.write("\tif os.stat(\""+fname+"\").st_size != 0:\n" )
    ofile.write("\t\tcmd"+ seq + " = \"snowsql -c " + ssconfig+ " -r repadmin  -w " +whname + " -f " + fname+" -o output_file=" + fname+"_out -o quiet=true & \" \n")
    ofile.write("\t\tout"+ seq + " = subprocess.check_output(cmd"+seq+",stderr=subprocess.STDOUT, shell=True)\n\n")

i = 0
unloadlist = [f for f in glob.glob("02_unload_*.sql")]
loadlist = [f for f in glob.glob("03_load_*.sql")]

ul_sm_list = [f for f in glob.glob("02_unload_*_small_*.sql")]
ul_lg_list = [f for f in glob.glob("02_unload_*_large_*.sql")]
ul_xl_list = [f for f in glob.glob("02_unload_*_xlarge_*.sql")]
ul_2xl_list = [f for f in glob.glob("02_unload_*_2xlarge_*.sql")]

ld_sm_list = [f for f in glob.glob("03_load_*_small_*.sql")]
ld_lg_list = [f for f in glob.glob("03_load_*_large_*.sql")]
ld_xl_list = [f for f in glob.glob("03_load_*_xlarge_*.sql")]
ld_2xl_list = [f for f in glob.glob("03_load_*_2xlarge_*.sql")]

of = open('./autounload.py', 'w')
of.write('import subprocess, os\n')
of.write('try:\n')
'''
for ofname in unloadlist: 
    i += 1
    genSubprocess(ofname, str(i), of)
'''
for ofname in ul_sm_list: 
    i += 1
    genSubprocess('migva', ofname, str(i), 'WH_CROSSREP_SMALL', of)

for ofname in ul_lg_list: 
    i += 1
    genSubprocess('migva',ofname, str(i), 'WH_CROSSREP_LARGE', of)

for ofname in ul_xl_list: 
    i += 1
    genSubprocess('migva',ofname, str(i), 'WH_CROSSREP_XLARGE', of)

for ofname in ul_2xl_list: 
    i += 1
    genSubprocess('migva',ofname, str(i), 'WH_CROSSREP_2XLARGE', of)

of.write('except subprocess.CalledProcessError as e:\n')
of.write('\tprint(e)\n')
of.close()

i = 0
pf = open('./autoload.py', 'w')
pf.write('import subprocess, os\n')
pf.write('try:\n')
'''
for pfname in loadlist: 
    i += 1
    genSubprocess(pfname, str(i), pf)
'''
for pfname in ld_sm_list: 
    i += 1
    genSubprocess('migwest', pfname, str(i), 'WH_CROSSREP_SMALL', pf)

for pfname in ld_lg_list: 
    i += 1
    genSubprocess('migwest',pfname, str(i), 'WH_CROSSREP_LARGE', pf)

for pfname in ld_xl_list: 
    i += 1
    genSubprocess('migwest',pfname, str(i), 'WH_CROSSREP_XLARGE', pf)

for pfname in ld_2xl_list: 
    i += 1
    genSubprocess('migwest',pfname, str(i), 'WH_CROSSREP_2XLARGE', pf)

pf.write('except subprocess.CalledProcessError as e:\n')
pf.write('\tprint(e)\n')
pf.close()

