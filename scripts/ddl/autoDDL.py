import subprocess, os
try:
	if os.stat("01_dbDDL_BLANK DB.sql").st_size != 0:
		cmd1 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_BLANK DB.sql -o output_file=01_dbDDL_BLANK DB.sql_out -o quiet=true & " 
		out1 = subprocess.check_output(cmd1,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KM_DB.sql").st_size != 0:
		cmd2 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KM_DB.sql -o output_file=01_dbDDL_KM_DB.sql_out -o quiet=true & " 
		out2 = subprocess.check_output(cmd2,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MN_DB.sql").st_size != 0:
		cmd3 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MN_DB.sql -o output_file=01_dbDDL_MN_DB.sql_out -o quiet=true & " 
		out3 = subprocess.check_output(cmd3,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRATIMA1.sql").st_size != 0:
		cmd4 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRATIMA1.sql -o output_file=01_dbDDL_PRATIMA1.sql_out -o quiet=true & " 
		out4 = subprocess.check_output(cmd4,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDCLONEDB.sql").st_size != 0:
		cmd5 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDCLONEDB.sql -o output_file=01_dbDDL_MDCLONEDB.sql_out -o quiet=true & " 
		out5 = subprocess.check_output(cmd5,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MINDB.sql").st_size != 0:
		cmd6 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MINDB.sql -o output_file=01_dbDDL_MINDB.sql_out -o quiet=true & " 
		out6 = subprocess.check_output(cmd6,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_DEV1.sql").st_size != 0:
		cmd7 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_DEV1.sql -o output_file=01_dbDDL_DB_DEV1.sql_out -o quiet=true & " 
		out7 = subprocess.check_output(cmd7,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATABASE1.sql").st_size != 0:
		cmd8 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATABASE1.sql -o output_file=01_dbDDL_DATABASE1.sql_out -o quiet=true & " 
		out8 = subprocess.check_output(cmd8,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DEV_LANDING_1.sql").st_size != 0:
		cmd9 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DEV_LANDING_1.sql -o output_file=01_dbDDL_DEV_LANDING_1.sql_out -o quiet=true & " 
		out9 = subprocess.check_output(cmd9,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KATIE_TEST2.sql").st_size != 0:
		cmd10 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KATIE_TEST2.sql -o output_file=01_dbDDL_KATIE_TEST2.sql_out -o quiet=true & " 
		out10 = subprocess.check_output(cmd10,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SDS_DEV.sql").st_size != 0:
		cmd11 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SDS_DEV.sql -o output_file=01_dbDDL_SDS_DEV.sql_out -o quiet=true & " 
		out11 = subprocess.check_output(cmd11,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DINESH_TEST.sql").st_size != 0:
		cmd12 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DINESH_TEST.sql -o output_file=01_dbDDL_DINESH_TEST.sql_out -o quiet=true & " 
		out12 = subprocess.check_output(cmd12,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JRYAN_DB_TEST.sql").st_size != 0:
		cmd13 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JRYAN_DB_TEST.sql -o output_file=01_dbDDL_JRYAN_DB_TEST.sql_out -o quiet=true & " 
		out13 = subprocess.check_output(cmd13,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TOAD_DB.sql").st_size != 0:
		cmd14 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TOAD_DB.sql -o output_file=01_dbDDL_TOAD_DB.sql_out -o quiet=true & " 
		out14 = subprocess.check_output(cmd14,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_CROSSREP_MDONOVAN_AU.sql").st_size != 0:
		cmd15 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_CROSSREP_MDONOVAN_AU.sql -o output_file=01_dbDDL_DB_CROSSREP_MDONOVAN_AU.sql_out -o quiet=true & " 
		out15 = subprocess.check_output(cmd15,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MINDB_CLONE.sql").st_size != 0:
		cmd16 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MINDB_CLONE.sql -o output_file=01_dbDDL_MINDB_CLONE.sql_out -o quiet=true & " 
		out16 = subprocess.check_output(cmd16,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_EDW_RPTG_DB11.sql").st_size != 0:
		cmd17 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_EDW_RPTG_DB11.sql -o output_file=01_dbDDL_EDW_RPTG_DB11.sql_out -o quiet=true & " 
		out17 = subprocess.check_output(cmd17,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_CROSSREP.sql").st_size != 0:
		cmd18 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_CROSSREP.sql -o output_file=01_dbDDL_DB_CROSSREP.sql_out -o quiet=true & " 
		out18 = subprocess.check_output(cmd18,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DROPTEST.sql").st_size != 0:
		cmd19 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DROPTEST.sql -o output_file=01_dbDDL_DROPTEST.sql_out -o quiet=true & " 
		out19 = subprocess.check_output(cmd19,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NOTADMIN.sql").st_size != 0:
		cmd20 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NOTADMIN.sql -o output_file=01_dbDDL_NOTADMIN.sql_out -o quiet=true & " 
		out20 = subprocess.check_output(cmd20,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDADBCLONE.sql").st_size != 0:
		cmd21 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDADBCLONE.sql -o output_file=01_dbDDL_HILDADBCLONE.sql_out -o quiet=true & " 
		out21 = subprocess.check_output(cmd21,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_QA.sql").st_size != 0:
		cmd22 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_QA.sql -o output_file=01_dbDDL_DB_QA.sql_out -o quiet=true & " 
		out22 = subprocess.check_output(cmd22,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JRYAN_SECURE.sql").st_size != 0:
		cmd23 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JRYAN_SECURE.sql -o output_file=01_dbDDL_JRYAN_SECURE.sql_out -o quiet=true & " 
		out23 = subprocess.check_output(cmd23,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDAROLE_DB.sql").st_size != 0:
		cmd24 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDAROLE_DB.sql -o output_file=01_dbDDL_HILDAROLE_DB.sql_out -o quiet=true & " 
		out24 = subprocess.check_output(cmd24,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_DV2.sql").st_size != 0:
		cmd25 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_DV2.sql -o output_file=01_dbDDL_DB_DV2.sql_out -o quiet=true & " 
		out25 = subprocess.check_output(cmd25,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ALEX_TESTING.sql").st_size != 0:
		cmd26 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ALEX_TESTING.sql -o output_file=01_dbDDL_ALEX_TESTING.sql_out -o quiet=true & " 
		out26 = subprocess.check_output(cmd26,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDB_PROD_DB.sql").st_size != 0:
		cmd27 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDB_PROD_DB.sql -o output_file=01_dbDDL_MDB_PROD_DB.sql_out -o quiet=true & " 
		out27 = subprocess.check_output(cmd27,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KTEST.sql").st_size != 0:
		cmd28 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KTEST.sql -o output_file=01_dbDDL_KTEST.sql_out -o quiet=true & " 
		out28 = subprocess.check_output(cmd28,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATABASEEE1.sql").st_size != 0:
		cmd29 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATABASEEE1.sql -o output_file=01_dbDDL_DATABASEEE1.sql_out -o quiet=true & " 
		out29 = subprocess.check_output(cmd29,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_ROLE.sql").st_size != 0:
		cmd30 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_ROLE.sql -o output_file=01_dbDDL_DB_ROLE.sql_out -o quiet=true & " 
		out30 = subprocess.check_output(cmd30,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JOEL_TEST_CLONE.sql").st_size != 0:
		cmd31 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JOEL_TEST_CLONE.sql -o output_file=01_dbDDL_JOEL_TEST_CLONE.sql_out -o quiet=true & " 
		out31 = subprocess.check_output(cmd31,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PETCLINIC.sql").st_size != 0:
		cmd32 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PETCLINIC.sql -o output_file=01_dbDDL_PETCLINIC.sql_out -o quiet=true & " 
		out32 = subprocess.check_output(cmd32,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SASANK.sql").st_size != 0:
		cmd33 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SASANK.sql -o output_file=01_dbDDL_SASANK.sql_out -o quiet=true & " 
		out33 = subprocess.check_output(cmd33,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_GWG_DB_OF_KNOWLEDGE.sql").st_size != 0:
		cmd34 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_GWG_DB_OF_KNOWLEDGE.sql -o output_file=01_dbDDL_GWG_DB_OF_KNOWLEDGE.sql_out -o quiet=true & " 
		out34 = subprocess.check_output(cmd34,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TIMSDB.sql").st_size != 0:
		cmd35 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TIMSDB.sql -o output_file=01_dbDDL_TIMSDB.sql_out -o quiet=true & " 
		out35 = subprocess.check_output(cmd35,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CDW_ODS_DEV.sql").st_size != 0:
		cmd36 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CDW_ODS_DEV.sql -o output_file=01_dbDDL_CDW_ODS_DEV.sql_out -o quiet=true & " 
		out36 = subprocess.check_output(cmd36,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VEIWTEST_VAIBS.sql").st_size != 0:
		cmd37 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VEIWTEST_VAIBS.sql -o output_file=01_dbDDL_VEIWTEST_VAIBS.sql_out -o quiet=true & " 
		out37 = subprocess.check_output(cmd37,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_PANKAJ.sql").st_size != 0:
		cmd38 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_PANKAJ.sql -o output_file=01_dbDDL_TEST_PANKAJ.sql_out -o quiet=true & " 
		out38 = subprocess.check_output(cmd38,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SH_USER01_DB.sql").st_size != 0:
		cmd39 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SH_USER01_DB.sql -o output_file=01_dbDDL_SH_USER01_DB.sql_out -o quiet=true & " 
		out39 = subprocess.check_output(cmd39,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_AYUSHI_TEST.sql").st_size != 0:
		cmd40 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_AYUSHI_TEST.sql -o output_file=01_dbDDL_AYUSHI_TEST.sql_out -o quiet=true & " 
		out40 = subprocess.check_output(cmd40,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRODUCTION.sql").st_size != 0:
		cmd41 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRODUCTION.sql -o output_file=01_dbDDL_PRODUCTION.sql_out -o quiet=true & " 
		out41 = subprocess.check_output(cmd41,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBRYANTDB.sql").st_size != 0:
		cmd42 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBRYANTDB.sql -o output_file=01_dbDDL_DBRYANTDB.sql_out -o quiet=true & " 
		out42 = subprocess.check_output(cmd42,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SALES.sql").st_size != 0:
		cmd43 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SALES.sql -o output_file=01_dbDDL_SALES.sql_out -o quiet=true & " 
		out43 = subprocess.check_output(cmd43,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_XYZ_TEST.sql").st_size != 0:
		cmd44 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_XYZ_TEST.sql -o output_file=01_dbDDL_XYZ_TEST.sql_out -o quiet=true & " 
		out44 = subprocess.check_output(cmd44,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TG_DB.sql").st_size != 0:
		cmd45 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TG_DB.sql -o output_file=01_dbDDL_TG_DB.sql_out -o quiet=true & " 
		out45 = subprocess.check_output(cmd45,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_EXAMPLE_1.sql").st_size != 0:
		cmd46 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_EXAMPLE_1.sql -o output_file=01_dbDDL_EXAMPLE_1.sql_out -o quiet=true & " 
		out46 = subprocess.check_output(cmd46,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBCISCO.sql").st_size != 0:
		cmd47 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBCISCO.sql -o output_file=01_dbDDL_DBCISCO.sql_out -o quiet=true & " 
		out47 = subprocess.check_output(cmd47,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATALAKE_SOURCE.sql").st_size != 0:
		cmd48 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATALAKE_SOURCE.sql -o output_file=01_dbDDL_DATALAKE_SOURCE.sql_out -o quiet=true & " 
		out48 = subprocess.check_output(cmd48,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KAPIL1.sql").st_size != 0:
		cmd49 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KAPIL1.sql -o output_file=01_dbDDL_KAPIL1.sql_out -o quiet=true & " 
		out49 = subprocess.check_output(cmd49,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JRYAN_DB.sql").st_size != 0:
		cmd50 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JRYAN_DB.sql -o output_file=01_dbDDL_JRYAN_DB.sql_out -o quiet=true & " 
		out50 = subprocess.check_output(cmd50,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PDX_REPRO.sql").st_size != 0:
		cmd51 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PDX_REPRO.sql -o output_file=01_dbDDL_PDX_REPRO.sql_out -o quiet=true & " 
		out51 = subprocess.check_output(cmd51,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CLONESCHEMATEST.sql").st_size != 0:
		cmd52 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CLONESCHEMATEST.sql -o output_file=01_dbDDL_CLONESCHEMATEST.sql_out -o quiet=true & " 
		out52 = subprocess.check_output(cmd52,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MEHUL_DB.sql").st_size != 0:
		cmd53 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MEHUL_DB.sql -o output_file=01_dbDDL_MEHUL_DB.sql_out -o quiet=true & " 
		out53 = subprocess.check_output(cmd53,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KHOYLE_DB_AWS_RS.sql").st_size != 0:
		cmd54 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KHOYLE_DB_AWS_RS.sql -o output_file=01_dbDDL_KHOYLE_DB_AWS_RS.sql_out -o quiet=true & " 
		out54 = subprocess.check_output(cmd54,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TPC_DV_TEST.sql").st_size != 0:
		cmd55 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TPC_DV_TEST.sql -o output_file=01_dbDDL_TPC_DV_TEST.sql_out -o quiet=true & " 
		out55 = subprocess.check_output(cmd55,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_HOL.sql").st_size != 0:
		cmd56 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_HOL.sql -o output_file=01_dbDDL_DB_HOL.sql_out -o quiet=true & " 
		out56 = subprocess.check_output(cmd56,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ALOOMA.sql").st_size != 0:
		cmd57 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ALOOMA.sql -o output_file=01_dbDDL_ALOOMA.sql_out -o quiet=true & " 
		out57 = subprocess.check_output(cmd57,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_FUTURE_GRANT_TEST_1.sql").st_size != 0:
		cmd58 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_FUTURE_GRANT_TEST_1.sql -o output_file=01_dbDDL_FUTURE_GRANT_TEST_1.sql_out -o quiet=true & " 
		out58 = subprocess.check_output(cmd58,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBTEMP.sql").st_size != 0:
		cmd59 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBTEMP.sql -o output_file=01_dbDDL_DBTEMP.sql_out -o quiet=true & " 
		out59 = subprocess.check_output(cmd59,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CLONE_TEST2.sql").st_size != 0:
		cmd60 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CLONE_TEST2.sql -o output_file=01_dbDDL_CLONE_TEST2.sql_out -o quiet=true & " 
		out60 = subprocess.check_output(cmd60,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDACLONETEST2.sql").st_size != 0:
		cmd61 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDACLONETEST2.sql -o output_file=01_dbDDL_HILDACLONETEST2.sql_out -o quiet=true & " 
		out61 = subprocess.check_output(cmd61,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_MIGRATION_TESTING.sql").st_size != 0:
		cmd62 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_MIGRATION_TESTING.sql -o output_file=01_dbDDL_DB_MIGRATION_TESTING.sql_out -o quiet=true & " 
		out62 = subprocess.check_output(cmd62,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDB_.sql").st_size != 0:
		cmd63 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDB_.sql -o output_file=01_dbDDL_TESTDB_.sql_out -o quiet=true & " 
		out63 = subprocess.check_output(cmd63,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NIKITA_DB.sql").st_size != 0:
		cmd64 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NIKITA_DB.sql -o output_file=01_dbDDL_NIKITA_DB.sql_out -o quiet=true & " 
		out64 = subprocess.check_output(cmd64,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIES_TEST_CLONE1.sql").st_size != 0:
		cmd65 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIES_TEST_CLONE1.sql -o output_file=01_dbDDL_MERRIES_TEST_CLONE1.sql_out -o quiet=true & " 
		out65 = subprocess.check_output(cmd65,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NEDWPRD_DB.sql").st_size != 0:
		cmd66 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NEDWPRD_DB.sql -o output_file=01_dbDDL_NEDWPRD_DB.sql_out -o quiet=true & " 
		out66 = subprocess.check_output(cmd66,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_AYUSHI_DB.sql").st_size != 0:
		cmd67 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_AYUSHI_DB.sql -o output_file=01_dbDDL_AYUSHI_DB.sql_out -o quiet=true & " 
		out67 = subprocess.check_output(cmd67,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KHATHIB_DB.sql").st_size != 0:
		cmd68 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KHATHIB_DB.sql -o output_file=01_dbDDL_KHATHIB_DB.sql_out -o quiet=true & " 
		out68 = subprocess.check_output(cmd68,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRD_CDW_DB_20180928.sql").st_size != 0:
		cmd69 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRD_CDW_DB_20180928.sql -o output_file=01_dbDDL_PRD_CDW_DB_20180928.sql_out -o quiet=true & " 
		out69 = subprocess.check_output(cmd69,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KEN_TEST_A.sql").st_size != 0:
		cmd70 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KEN_TEST_A.sql -o output_file=01_dbDDL_KEN_TEST_A.sql_out -o quiet=true & " 
		out70 = subprocess.check_output(cmd70,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KHOYLE_DB_WW.sql").st_size != 0:
		cmd71 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KHOYLE_DB_WW.sql -o output_file=01_dbDDL_KHOYLE_DB_WW.sql_out -o quiet=true & " 
		out71 = subprocess.check_output(cmd71,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_WORKSHEETS_APP.sql").st_size != 0:
		cmd72 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_WORKSHEETS_APP.sql -o output_file=01_dbDDL_WORKSHEETS_APP.sql_out -o quiet=true & " 
		out72 = subprocess.check_output(cmd72,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_DEV_CLO2.sql").st_size != 0:
		cmd73 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_DEV_CLO2.sql -o output_file=01_dbDDL_VAIBHAVI_DEV_CLO2.sql_out -o quiet=true & " 
		out73 = subprocess.check_output(cmd73,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DARWOODS_DB1_TEST.sql").st_size != 0:
		cmd74 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DARWOODS_DB1_TEST.sql -o output_file=01_dbDDL_DARWOODS_DB1_TEST.sql_out -o quiet=true & " 
		out74 = subprocess.check_output(cmd74,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDONOVAN_DB_ET_CLONE.sql").st_size != 0:
		cmd75 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDONOVAN_DB_ET_CLONE.sql -o output_file=01_dbDDL_MDONOVAN_DB_ET_CLONE.sql_out -o quiet=true & " 
		out75 = subprocess.check_output(cmd75,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KJDB.sql").st_size != 0:
		cmd76 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KJDB.sql -o output_file=01_dbDDL_KJDB.sql_out -o quiet=true & " 
		out76 = subprocess.check_output(cmd76,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NIKITA_TEST.sql").st_size != 0:
		cmd77 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NIKITA_TEST.sql -o output_file=01_dbDDL_NIKITA_TEST.sql_out -o quiet=true & " 
		out77 = subprocess.check_output(cmd77,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDACLONETEST1.sql").st_size != 0:
		cmd78 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDACLONETEST1.sql -o output_file=01_dbDDL_HILDACLONETEST1.sql_out -o quiet=true & " 
		out78 = subprocess.check_output(cmd78,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SONY_ADA.sql").st_size != 0:
		cmd79 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SONY_ADA.sql -o output_file=01_dbDDL_SONY_ADA.sql_out -o quiet=true & " 
		out79 = subprocess.check_output(cmd79,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SONY_MSR.sql").st_size != 0:
		cmd80 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SONY_MSR.sql -o output_file=01_dbDDL_SONY_MSR.sql_out -o quiet=true & " 
		out80 = subprocess.check_output(cmd80,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CLONE_TEST.sql").st_size != 0:
		cmd81 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CLONE_TEST.sql -o output_file=01_dbDDL_CLONE_TEST.sql_out -o quiet=true & " 
		out81 = subprocess.check_output(cmd81,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TOSHARE.sql").st_size != 0:
		cmd82 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TOSHARE.sql -o output_file=01_dbDDL_TOSHARE.sql_out -o quiet=true & " 
		out82 = subprocess.check_output(cmd82,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRD_CDW_DB_201809281.sql").st_size != 0:
		cmd83 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRD_CDW_DB_201809281.sql -o output_file=01_dbDDL_PRD_CDW_DB_201809281.sql_out -o quiet=true & " 
		out83 = subprocess.check_output(cmd83,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TUTORIAL_DB.sql").st_size != 0:
		cmd84 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TUTORIAL_DB.sql -o output_file=01_dbDDL_TUTORIAL_DB.sql_out -o quiet=true & " 
		out84 = subprocess.check_output(cmd84,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDB01.sql").st_size != 0:
		cmd85 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDB01.sql -o output_file=01_dbDDL_TESTDB01.sql_out -o quiet=true & " 
		out85 = subprocess.check_output(cmd85,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MYCLONE.sql").st_size != 0:
		cmd86 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MYCLONE.sql -o output_file=01_dbDDL_MYCLONE.sql_out -o quiet=true & " 
		out86 = subprocess.check_output(cmd86,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VIEWDB_1.sql").st_size != 0:
		cmd87 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VIEWDB_1.sql -o output_file=01_dbDDL_VIEWDB_1.sql_out -o quiet=true & " 
		out87 = subprocess.check_output(cmd87,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_TEST_ACCT.sql").st_size != 0:
		cmd88 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_TEST_ACCT.sql -o output_file=01_dbDDL_VAIBHAVI_TEST_ACCT.sql_out -o quiet=true & " 
		out88 = subprocess.check_output(cmd88,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SNOWFLAKE_AUTOCLUSTERING_TRAINING.sql").st_size != 0:
		cmd89 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SNOWFLAKE_AUTOCLUSTERING_TRAINING.sql -o output_file=01_dbDDL_SNOWFLAKE_AUTOCLUSTERING_TRAINING.sql_out -o quiet=true & " 
		out89 = subprocess.check_output(cmd89,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PlayPenNew.sql").st_size != 0:
		cmd90 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PlayPenNew.sql -o output_file=01_dbDDL_PlayPenNew.sql_out -o quiet=true & " 
		out90 = subprocess.check_output(cmd90,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MR_NEW_DB_1.sql").st_size != 0:
		cmd91 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MR_NEW_DB_1.sql -o output_file=01_dbDDL_MR_NEW_DB_1.sql_out -o quiet=true & " 
		out91 = subprocess.check_output(cmd91,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_DEV_CLO1.sql").st_size != 0:
		cmd92 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_DEV_CLO1.sql -o output_file=01_dbDDL_VAIBHAVI_DEV_CLO1.sql_out -o quiet=true & " 
		out92 = subprocess.check_output(cmd92,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRATIMA_00052584_TEST2.sql").st_size != 0:
		cmd93 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRATIMA_00052584_TEST2.sql -o output_file=01_dbDDL_PRATIMA_00052584_TEST2.sql_out -o quiet=true & " 
		out93 = subprocess.check_output(cmd93,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDBCLONE.sql").st_size != 0:
		cmd94 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDBCLONE.sql -o output_file=01_dbDDL_TESTDBCLONE.sql_out -o quiet=true & " 
		out94 = subprocess.check_output(cmd94,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDBNEW_CLONE.sql").st_size != 0:
		cmd95 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDBNEW_CLONE.sql -o output_file=01_dbDDL_TESTDBNEW_CLONE.sql_out -o quiet=true & " 
		out95 = subprocess.check_output(cmd95,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_IQUANTI_PRODUCTION_ARCHIVE.sql").st_size != 0:
		cmd96 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_IQUANTI_PRODUCTION_ARCHIVE.sql -o output_file=01_dbDDL_IQUANTI_PRODUCTION_ARCHIVE.sql_out -o quiet=true & " 
		out96 = subprocess.check_output(cmd96,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SCADMIN.sql").st_size != 0:
		cmd97 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SCADMIN.sql -o output_file=01_dbDDL_SCADMIN.sql_out -o quiet=true & " 
		out97 = subprocess.check_output(cmd97,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SCDB_CLONE.sql").st_size != 0:
		cmd98 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SCDB_CLONE.sql -o output_file=01_dbDDL_SCDB_CLONE.sql_out -o quiet=true & " 
		out98 = subprocess.check_output(cmd98,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTS.sql").st_size != 0:
		cmd99 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTS.sql -o output_file=01_dbDDL_TESTS.sql_out -o quiet=true & " 
		out99 = subprocess.check_output(cmd99,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST1_CLONE.sql").st_size != 0:
		cmd100 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST1_CLONE.sql -o output_file=01_dbDDL_TEST1_CLONE.sql_out -o quiet=true & " 
		out100 = subprocess.check_output(cmd100,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_SHARE.sql").st_size != 0:
		cmd101 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_SHARE.sql -o output_file=01_dbDDL_DB_SHARE.sql_out -o quiet=true & " 
		out101 = subprocess.check_output(cmd101,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DWH_DEV.sql").st_size != 0:
		cmd102 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DWH_DEV.sql -o output_file=01_dbDDL_DWH_DEV.sql_out -o quiet=true & " 
		out102 = subprocess.check_output(cmd102,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_40357.sql").st_size != 0:
		cmd103 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_40357.sql -o output_file=01_dbDDL_DB_40357.sql_out -o quiet=true & " 
		out103 = subprocess.check_output(cmd103,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_LOCAL.sql").st_size != 0:
		cmd104 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_LOCAL.sql -o output_file=01_dbDDL_DB_LOCAL.sql_out -o quiet=true & " 
		out104 = subprocess.check_output(cmd104,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SUPPORT_DEMO2.sql").st_size != 0:
		cmd105 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SUPPORT_DEMO2.sql -o output_file=01_dbDDL_SUPPORT_DEMO2.sql_out -o quiet=true & " 
		out105 = subprocess.check_output(cmd105,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JESSIESDB.sql").st_size != 0:
		cmd106 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JESSIESDB.sql -o output_file=01_dbDDL_JESSIESDB.sql_out -o quiet=true & " 
		out106 = subprocess.check_output(cmd106,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATA_CLONING_00049821.sql").st_size != 0:
		cmd107 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATA_CLONING_00049821.sql -o output_file=01_dbDDL_DATA_CLONING_00049821.sql_out -o quiet=true & " 
		out107 = subprocess.check_output(cmd107,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_SECURE_VIEW.sql").st_size != 0:
		cmd108 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_SECURE_VIEW.sql -o output_file=01_dbDDL_TEST_SECURE_VIEW.sql_out -o quiet=true & " 
		out108 = subprocess.check_output(cmd108,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MANAGE_CLONE.sql").st_size != 0:
		cmd109 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MANAGE_CLONE.sql -o output_file=01_dbDDL_MANAGE_CLONE.sql_out -o quiet=true & " 
		out109 = subprocess.check_output(cmd109,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CLONE_DB.sql").st_size != 0:
		cmd110 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CLONE_DB.sql -o output_file=01_dbDDL_CLONE_DB.sql_out -o quiet=true & " 
		out110 = subprocess.check_output(cmd110,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDONOVAN_DB_DEV.sql").st_size != 0:
		cmd111 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDONOVAN_DB_DEV.sql -o output_file=01_dbDDL_MDONOVAN_DB_DEV.sql_out -o quiet=true & " 
		out111 = subprocess.check_output(cmd111,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MINDB_CLONE1.sql").st_size != 0:
		cmd112 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MINDB_CLONE1.sql -o output_file=01_dbDDL_MINDB_CLONE1.sql_out -o quiet=true & " 
		out112 = subprocess.check_output(cmd112,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_GURSOYDB.sql").st_size != 0:
		cmd113 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_GURSOYDB.sql -o output_file=01_dbDDL_GURSOYDB.sql_out -o quiet=true & " 
		out113 = subprocess.check_output(cmd113,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_FRANKLIN_DB.sql").st_size != 0:
		cmd114 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_FRANKLIN_DB.sql -o output_file=01_dbDDL_FRANKLIN_DB.sql_out -o quiet=true & " 
		out114 = subprocess.check_output(cmd114,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PC_STITCH_DB.sql").st_size != 0:
		cmd115 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PC_STITCH_DB.sql -o output_file=01_dbDDL_PC_STITCH_DB.sql_out -o quiet=true & " 
		out115 = subprocess.check_output(cmd115,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NEWDB.sql").st_size != 0:
		cmd116 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NEWDB.sql -o output_file=01_dbDDL_NEWDB.sql_out -o quiet=true & " 
		out116 = subprocess.check_output(cmd116,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_SAM1.sql").st_size != 0:
		cmd117 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_SAM1.sql -o output_file=01_dbDDL_DB_SAM1.sql_out -o quiet=true & " 
		out117 = subprocess.check_output(cmd117,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SHASHI2.sql").st_size != 0:
		cmd118 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SHASHI2.sql -o output_file=01_dbDDL_SHASHI2.sql_out -o quiet=true & " 
		out118 = subprocess.check_output(cmd118,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PTU2.sql").st_size != 0:
		cmd119 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PTU2.sql -o output_file=01_dbDDL_PTU2.sql_out -o quiet=true & " 
		out119 = subprocess.check_output(cmd119,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRATIMA_00052584_1.sql").st_size != 0:
		cmd120 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRATIMA_00052584_1.sql -o output_file=01_dbDDL_PRATIMA_00052584_1.sql_out -o quiet=true & " 
		out120 = subprocess.check_output(cmd120,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_WHDB.sql").st_size != 0:
		cmd121 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_WHDB.sql -o output_file=01_dbDDL_WHDB.sql_out -o quiet=true & " 
		out121 = subprocess.check_output(cmd121,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DMITRY_DB1.sql").st_size != 0:
		cmd122 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DMITRY_DB1.sql -o output_file=01_dbDDL_DMITRY_DB1.sql_out -o quiet=true & " 
		out122 = subprocess.check_output(cmd122,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TRAINING_DB.sql").st_size != 0:
		cmd123 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TRAINING_DB.sql -o output_file=01_dbDDL_TRAINING_DB.sql_out -o quiet=true & " 
		out123 = subprocess.check_output(cmd123,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRATIMA.sql").st_size != 0:
		cmd124 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRATIMA.sql -o output_file=01_dbDDL_PRATIMA.sql_out -o quiet=true & " 
		out124 = subprocess.check_output(cmd124,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST1.sql").st_size != 0:
		cmd125 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST1.sql -o output_file=01_dbDDL_TEST1.sql_out -o quiet=true & " 
		out125 = subprocess.check_output(cmd125,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SYSADMIN_DB.sql").st_size != 0:
		cmd126 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SYSADMIN_DB.sql -o output_file=01_dbDDL_SYSADMIN_DB.sql_out -o quiet=true & " 
		out126 = subprocess.check_output(cmd126,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB22.sql").st_size != 0:
		cmd127 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB22.sql -o output_file=01_dbDDL_DB22.sql_out -o quiet=true & " 
		out127 = subprocess.check_output(cmd127,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DAVID_LIM.sql").st_size != 0:
		cmd128 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DAVID_LIM.sql -o output_file=01_dbDDL_DAVID_LIM.sql_out -o quiet=true & " 
		out128 = subprocess.check_output(cmd128,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DAVID_DB_4.sql").st_size != 0:
		cmd129 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DAVID_DB_4.sql -o output_file=01_dbDDL_DAVID_DB_4.sql_out -o quiet=true & " 
		out129 = subprocess.check_output(cmd129,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MINDB_CL.sql").st_size != 0:
		cmd130 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MINDB_CL.sql -o output_file=01_dbDDL_MINDB_CL.sql_out -o quiet=true & " 
		out130 = subprocess.check_output(cmd130,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MR_DEMO.sql").st_size != 0:
		cmd131 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MR_DEMO.sql -o output_file=01_dbDDL_MR_DEMO.sql_out -o quiet=true & " 
		out131 = subprocess.check_output(cmd131,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CONVERT_DB.sql").st_size != 0:
		cmd132 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CONVERT_DB.sql -o output_file=01_dbDDL_CONVERT_DB.sql_out -o quiet=true & " 
		out132 = subprocess.check_output(cmd132,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_1.sql").st_size != 0:
		cmd133 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_1.sql -o output_file=01_dbDDL_DB_1.sql_out -o quiet=true & " 
		out133 = subprocess.check_output(cmd133,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DINESHTEST123.sql").st_size != 0:
		cmd134 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DINESHTEST123.sql -o output_file=01_dbDDL_DINESHTEST123.sql_out -o quiet=true & " 
		out134 = subprocess.check_output(cmd134,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DV.sql").st_size != 0:
		cmd135 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DV.sql -o output_file=01_dbDDL_DV.sql_out -o quiet=true & " 
		out135 = subprocess.check_output(cmd135,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KHOYLE_DB.sql").st_size != 0:
		cmd136 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KHOYLE_DB.sql -o output_file=01_dbDDL_KHOYLE_DB.sql_out -o quiet=true & " 
		out136 = subprocess.check_output(cmd136,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_CROSSREP_MDONOVAN.sql").st_size != 0:
		cmd137 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_CROSSREP_MDONOVAN.sql -o output_file=01_dbDDL_DB_CROSSREP_MDONOVAN.sql_out -o quiet=true & " 
		out137 = subprocess.check_output(cmd137,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_DB_SDIGU.sql").st_size != 0:
		cmd138 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_DB_SDIGU.sql -o output_file=01_dbDDL_TEST_DB_SDIGU.sql_out -o quiet=true & " 
		out138 = subprocess.check_output(cmd138,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KWATSON_TT_TEST_DB.sql").st_size != 0:
		cmd139 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KWATSON_TT_TEST_DB.sql -o output_file=01_dbDDL_KWATSON_TT_TEST_DB.sql_out -o quiet=true & " 
		out139 = subprocess.check_output(cmd139,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_BLACKBOARD.sql").st_size != 0:
		cmd140 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_BLACKBOARD.sql -o output_file=01_dbDDL_BLACKBOARD.sql_out -o quiet=true & " 
		out140 = subprocess.check_output(cmd140,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBHOL.sql").st_size != 0:
		cmd141 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBHOL.sql -o output_file=01_dbDDL_DBHOL.sql_out -o quiet=true & " 
		out141 = subprocess.check_output(cmd141,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KATIE_TEST.sql").st_size != 0:
		cmd142 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KATIE_TEST.sql -o output_file=01_dbDDL_KATIE_TEST.sql_out -o quiet=true & " 
		out142 = subprocess.check_output(cmd142,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_DEV.sql").st_size != 0:
		cmd143 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_DEV.sql -o output_file=01_dbDDL_DB_DEV.sql_out -o quiet=true & " 
		out143 = subprocess.check_output(cmd143,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SID_DB.sql").st_size != 0:
		cmd144 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SID_DB.sql -o output_file=01_dbDDL_SID_DB.sql_out -o quiet=true & " 
		out144 = subprocess.check_output(cmd144,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SF_TUTS.sql").st_size != 0:
		cmd145 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SF_TUTS.sql -o output_file=01_dbDDL_SF_TUTS.sql_out -o quiet=true & " 
		out145 = subprocess.check_output(cmd145,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ANKUR_DB_TEST.sql").st_size != 0:
		cmd146 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ANKUR_DB_TEST.sql -o output_file=01_dbDDL_ANKUR_DB_TEST.sql_out -o quiet=true & " 
		out146 = subprocess.check_output(cmd146,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_EA.sql").st_size != 0:
		cmd147 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_EA.sql -o output_file=01_dbDDL_EA.sql_out -o quiet=true & " 
		out147 = subprocess.check_output(cmd147,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NOTADMINCLONE.sql").st_size != 0:
		cmd148 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NOTADMINCLONE.sql -o output_file=01_dbDDL_NOTADMINCLONE.sql_out -o quiet=true & " 
		out148 = subprocess.check_output(cmd148,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_LAB_SFCSUPPORT_DB1.sql").st_size != 0:
		cmd149 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_LAB_SFCSUPPORT_DB1.sql -o output_file=01_dbDDL_LAB_SFCSUPPORT_DB1.sql_out -o quiet=true & " 
		out149 = subprocess.check_output(cmd149,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MR_NEW_DB.sql").st_size != 0:
		cmd150 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MR_NEW_DB.sql -o output_file=01_dbDDL_MR_NEW_DB.sql_out -o quiet=true & " 
		out150 = subprocess.check_output(cmd150,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_P44PRODDB.sql").st_size != 0:
		cmd151 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_P44PRODDB.sql -o output_file=01_dbDDL_P44PRODDB.sql_out -o quiet=true & " 
		out151 = subprocess.check_output(cmd151,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_RETENTION_TIMETRAVEL.sql").st_size != 0:
		cmd152 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_RETENTION_TIMETRAVEL.sql -o output_file=01_dbDDL_RETENTION_TIMETRAVEL.sql_out -o quiet=true & " 
		out152 = subprocess.check_output(cmd152,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SANDBOX_SRINI.sql").st_size != 0:
		cmd153 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SANDBOX_SRINI.sql -o output_file=01_dbDDL_SANDBOX_SRINI.sql_out -o quiet=true & " 
		out153 = subprocess.check_output(cmd153,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DEV_VAIBS_TEST.sql").st_size != 0:
		cmd154 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DEV_VAIBS_TEST.sql -o output_file=01_dbDDL_DEV_VAIBS_TEST.sql_out -o quiet=true & " 
		out154 = subprocess.check_output(cmd154,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIES_TESTCLONE1.sql").st_size != 0:
		cmd155 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIES_TESTCLONE1.sql -o output_file=01_dbDDL_MERRIES_TESTCLONE1.sql_out -o quiet=true & " 
		out155 = subprocess.check_output(cmd155,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MINDB2.sql").st_size != 0:
		cmd156 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MINDB2.sql -o output_file=01_dbDDL_MINDB2.sql_out -o quiet=true & " 
		out156 = subprocess.check_output(cmd156,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRODUCTION_DATAPIPE.sql").st_size != 0:
		cmd157 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRODUCTION_DATAPIPE.sql -o output_file=01_dbDDL_PRODUCTION_DATAPIPE.sql_out -o quiet=true & " 
		out157 = subprocess.check_output(cmd157,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_ACCTUSAGE.sql").st_size != 0:
		cmd158 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_ACCTUSAGE.sql -o output_file=01_dbDDL_DB_ACCTUSAGE.sql_out -o quiet=true & " 
		out158 = subprocess.check_output(cmd158,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SUPPORT_DEMO.sql").st_size != 0:
		cmd159 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SUPPORT_DEMO.sql -o output_file=01_dbDDL_SUPPORT_DEMO.sql_out -o quiet=true & " 
		out159 = subprocess.check_output(cmd159,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SF_GWGTUTS.sql").st_size != 0:
		cmd160 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SF_GWGTUTS.sql -o output_file=01_dbDDL_SF_GWGTUTS.sql_out -o quiet=true & " 
		out160 = subprocess.check_output(cmd160,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_TPCH.sql").st_size != 0:
		cmd161 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_TPCH.sql -o output_file=01_dbDDL_DB_TPCH.sql_out -o quiet=true & " 
		out161 = subprocess.check_output(cmd161,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBTEST.sql").st_size != 0:
		cmd162 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBTEST.sql -o output_file=01_dbDDL_DBTEST.sql_out -o quiet=true & " 
		out162 = subprocess.check_output(cmd162,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KELVIN_PYTHON_TEST_DB.sql").st_size != 0:
		cmd163 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KELVIN_PYTHON_TEST_DB.sql -o output_file=01_dbDDL_KELVIN_PYTHON_TEST_DB.sql_out -o quiet=true & " 
		out163 = subprocess.check_output(cmd163,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_FK_PUBLIC.sql").st_size != 0:
		cmd164 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_FK_PUBLIC.sql -o output_file=01_dbDDL_FK_PUBLIC.sql_out -o quiet=true & " 
		out164 = subprocess.check_output(cmd164,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SCOTTDB.sql").st_size != 0:
		cmd165 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SCOTTDB.sql -o output_file=01_dbDDL_SCOTTDB.sql_out -o quiet=true & " 
		out165 = subprocess.check_output(cmd165,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB8730.sql").st_size != 0:
		cmd166 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB8730.sql -o output_file=01_dbDDL_DB8730.sql_out -o quiet=true & " 
		out166 = subprocess.check_output(cmd166,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PANKAJARORA.sql").st_size != 0:
		cmd167 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PANKAJARORA.sql -o output_file=01_dbDDL_PANKAJARORA.sql_out -o quiet=true & " 
		out167 = subprocess.check_output(cmd167,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_LAB_LETS_START_SHARING_DB.sql").st_size != 0:
		cmd168 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_LAB_LETS_START_SHARING_DB.sql -o output_file=01_dbDDL_LAB_LETS_START_SHARING_DB.sql_out -o quiet=true & " 
		out168 = subprocess.check_output(cmd168,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_TPCH_DV2.sql").st_size != 0:
		cmd169 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_TPCH_DV2.sql -o output_file=01_dbDDL_DB_TPCH_DV2.sql_out -o quiet=true & " 
		out169 = subprocess.check_output(cmd169,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_FILEFORMAT_STAGE.sql").st_size != 0:
		cmd170 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_FILEFORMAT_STAGE.sql -o output_file=01_dbDDL_FILEFORMAT_STAGE.sql_out -o quiet=true & " 
		out170 = subprocess.check_output(cmd170,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DEMODB.sql").st_size != 0:
		cmd171 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DEMODB.sql -o output_file=01_dbDDL_DEMODB.sql_out -o quiet=true & " 
		out171 = subprocess.check_output(cmd171,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PLD_SANDBOX.sql").st_size != 0:
		cmd172 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PLD_SANDBOX.sql -o output_file=01_dbDDL_PLD_SANDBOX.sql_out -o quiet=true & " 
		out172 = subprocess.check_output(cmd172,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDTESTDB.sql").st_size != 0:
		cmd173 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDTESTDB.sql -o output_file=01_dbDDL_MDTESTDB.sql_out -o quiet=true & " 
		out173 = subprocess.check_output(cmd173,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_DB_CLONE.sql").st_size != 0:
		cmd174 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_DB_CLONE.sql -o output_file=01_dbDDL_TEST_DB_CLONE.sql_out -o quiet=true & " 
		out174 = subprocess.check_output(cmd174,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBTEST1.sql").st_size != 0:
		cmd175 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBTEST1.sql -o output_file=01_dbDDL_DBTEST1.sql_out -o quiet=true & " 
		out175 = subprocess.check_output(cmd175,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KHAT_CLONE.sql").st_size != 0:
		cmd176 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KHAT_CLONE.sql -o output_file=01_dbDDL_KHAT_CLONE.sql_out -o quiet=true & " 
		out176 = subprocess.check_output(cmd176,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SHAREDB.sql").st_size != 0:
		cmd177 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SHAREDB.sql -o output_file=01_dbDDL_SHAREDB.sql_out -o quiet=true & " 
		out177 = subprocess.check_output(cmd177,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_Z8445CLONE.sql").st_size != 0:
		cmd178 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_Z8445CLONE.sql -o output_file=01_dbDDL_Z8445CLONE.sql_out -o quiet=true & " 
		out178 = subprocess.check_output(cmd178,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_BRT_DB.sql").st_size != 0:
		cmd179 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_BRT_DB.sql -o output_file=01_dbDDL_BRT_DB.sql_out -o quiet=true & " 
		out179 = subprocess.check_output(cmd179,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATASOLUTIONS_DELIVERY_ATT_DB.sql").st_size != 0:
		cmd180 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATASOLUTIONS_DELIVERY_ATT_DB.sql -o output_file=01_dbDDL_DATASOLUTIONS_DELIVERY_ATT_DB.sql_out -o quiet=true & " 
		out180 = subprocess.check_output(cmd180,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SPORTS.sql").st_size != 0:
		cmd181 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SPORTS.sql -o output_file=01_dbDDL_SPORTS.sql_out -o quiet=true & " 
		out181 = subprocess.check_output(cmd181,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTING_DB.sql").st_size != 0:
		cmd182 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTING_DB.sql -o output_file=01_dbDDL_TESTING_DB.sql_out -o quiet=true & " 
		out182 = subprocess.check_output(cmd182,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_LAB_SFCSUPPORT_DB2.sql").st_size != 0:
		cmd183 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_LAB_SFCSUPPORT_DB2.sql -o output_file=01_dbDDL_LAB_SFCSUPPORT_DB2.sql_out -o quiet=true & " 
		out183 = subprocess.check_output(cmd183,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KHAT_EXAMPLE.sql").st_size != 0:
		cmd184 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KHAT_EXAMPLE.sql -o output_file=01_dbDDL_KHAT_EXAMPLE.sql_out -o quiet=true & " 
		out184 = subprocess.check_output(cmd184,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DAVID_DB.sql").st_size != 0:
		cmd185 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DAVID_DB.sql -o output_file=01_dbDDL_DAVID_DB.sql_out -o quiet=true & " 
		out185 = subprocess.check_output(cmd185,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MARION.sql").st_size != 0:
		cmd186 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MARION.sql -o output_file=01_dbDDL_MARION.sql_out -o quiet=true & " 
		out186 = subprocess.check_output(cmd186,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DAVID_DB_3.sql").st_size != 0:
		cmd187 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DAVID_DB_3.sql -o output_file=01_dbDDL_DAVID_DB_3.sql_out -o quiet=true & " 
		out187 = subprocess.check_output(cmd187,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_FOO.sql").st_size != 0:
		cmd188 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_FOO.sql -o output_file=01_dbDDL_FOO.sql_out -o quiet=true & " 
		out188 = subprocess.check_output(cmd188,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_DB_F.sql").st_size != 0:
		cmd189 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_DB_F.sql -o output_file=01_dbDDL_TEST_DB_F.sql_out -o quiet=true & " 
		out189 = subprocess.check_output(cmd189,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MEHUL_1.sql").st_size != 0:
		cmd190 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MEHUL_1.sql -o output_file=01_dbDDL_MEHUL_1.sql_out -o quiet=true & " 
		out190 = subprocess.check_output(cmd190,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VID.sql").st_size != 0:
		cmd191 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VID.sql -o output_file=01_dbDDL_VID.sql_out -o quiet=true & " 
		out191 = subprocess.check_output(cmd191,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_D1.sql").st_size != 0:
		cmd192 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_D1.sql -o output_file=01_dbDDL_D1.sql_out -o quiet=true & " 
		out192 = subprocess.check_output(cmd192,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DUMMY.sql").st_size != 0:
		cmd193 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DUMMY.sql -o output_file=01_dbDDL_DUMMY.sql_out -o quiet=true & " 
		out193 = subprocess.check_output(cmd193,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDADB.sql").st_size != 0:
		cmd194 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDADB.sql -o output_file=01_dbDDL_HILDADB.sql_out -o quiet=true & " 
		out194 = subprocess.check_output(cmd194,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_DB1.sql").st_size != 0:
		cmd195 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_DB1.sql -o output_file=01_dbDDL_TEST_DB1.sql_out -o quiet=true & " 
		out195 = subprocess.check_output(cmd195,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_USER67.sql").st_size != 0:
		cmd196 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_USER67.sql -o output_file=01_dbDDL_DB_USER67.sql_out -o quiet=true & " 
		out196 = subprocess.check_output(cmd196,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_LACEWORK_TEST.sql").st_size != 0:
		cmd197 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_LACEWORK_TEST.sql -o output_file=01_dbDDL_LACEWORK_TEST.sql_out -o quiet=true & " 
		out197 = subprocess.check_output(cmd197,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VINDB2.sql").st_size != 0:
		cmd198 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VINDB2.sql -o output_file=01_dbDDL_VINDB2.sql_out -o quiet=true & " 
		out198 = subprocess.check_output(cmd198,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_GWGDB.sql").st_size != 0:
		cmd199 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_GWGDB.sql -o output_file=01_dbDDL_GWGDB.sql_out -o quiet=true & " 
		out199 = subprocess.check_output(cmd199,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATALAKE_SANDBOX.sql").st_size != 0:
		cmd200 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATALAKE_SANDBOX.sql -o output_file=01_dbDDL_DATALAKE_SANDBOX.sql_out -o quiet=true & " 
		out200 = subprocess.check_output(cmd200,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SNS_TESTING.sql").st_size != 0:
		cmd201 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SNS_TESTING.sql -o output_file=01_dbDDL_SNS_TESTING.sql_out -o quiet=true & " 
		out201 = subprocess.check_output(cmd201,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PP_CLOND.sql").st_size != 0:
		cmd202 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PP_CLOND.sql -o output_file=01_dbDDL_PP_CLOND.sql_out -o quiet=true & " 
		out202 = subprocess.check_output(cmd202,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDAROLEDB.sql").st_size != 0:
		cmd203 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDAROLEDB.sql -o output_file=01_dbDDL_HILDAROLEDB.sql_out -o quiet=true & " 
		out203 = subprocess.check_output(cmd203,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBMIG.sql").st_size != 0:
		cmd204 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBMIG.sql -o output_file=01_dbDDL_DBMIG.sql_out -o quiet=true & " 
		out204 = subprocess.check_output(cmd204,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CLONE_BUG_TEST.sql").st_size != 0:
		cmd205 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CLONE_BUG_TEST.sql -o output_file=01_dbDDL_CLONE_BUG_TEST.sql_out -o quiet=true & " 
		out205 = subprocess.check_output(cmd205,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_AF_DB.sql").st_size != 0:
		cmd206 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_AF_DB.sql -o output_file=01_dbDDL_AF_DB.sql_out -o quiet=true & " 
		out206 = subprocess.check_output(cmd206,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB1.sql").st_size != 0:
		cmd207 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB1.sql -o output_file=01_dbDDL_DB1.sql_out -o quiet=true & " 
		out207 = subprocess.check_output(cmd207,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SFDC_MIGRATION.sql").st_size != 0:
		cmd208 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SFDC_MIGRATION.sql -o output_file=01_dbDDL_SFDC_MIGRATION.sql_out -o quiet=true & " 
		out208 = subprocess.check_output(cmd208,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PLAYPEN4J_CLONED.sql").st_size != 0:
		cmd209 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PLAYPEN4J_CLONED.sql -o output_file=01_dbDDL_PLAYPEN4J_CLONED.sql_out -o quiet=true & " 
		out209 = subprocess.check_output(cmd209,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PROD.sql").st_size != 0:
		cmd210 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PROD.sql -o output_file=01_dbDDL_PROD.sql_out -o quiet=true & " 
		out210 = subprocess.check_output(cmd210,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SALES2.sql").st_size != 0:
		cmd211 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SALES2.sql -o output_file=01_dbDDL_SALES2.sql_out -o quiet=true & " 
		out211 = subprocess.check_output(cmd211,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DARWOODS_DB1.sql").st_size != 0:
		cmd212 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DARWOODS_DB1.sql -o output_file=01_dbDDL_DARWOODS_DB1.sql_out -o quiet=true & " 
		out212 = subprocess.check_output(cmd212,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST.sql").st_size != 0:
		cmd213 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST.sql -o output_file=01_dbDDL_TEST.sql_out -o quiet=true & " 
		out213 = subprocess.check_output(cmd213,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIES_TEST_CLONE.sql").st_size != 0:
		cmd214 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIES_TEST_CLONE.sql -o output_file=01_dbDDL_MERRIES_TEST_CLONE.sql_out -o quiet=true & " 
		out214 = subprocess.check_output(cmd214,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDB_DEV_DB1.sql").st_size != 0:
		cmd215 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDB_DEV_DB1.sql -o output_file=01_dbDDL_MDB_DEV_DB1.sql_out -o quiet=true & " 
		out215 = subprocess.check_output(cmd215,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HYU_DB.sql").st_size != 0:
		cmd216 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HYU_DB.sql -o output_file=01_dbDDL_HYU_DB.sql_out -o quiet=true & " 
		out216 = subprocess.check_output(cmd216,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HOL_USER_HOL.sql").st_size != 0:
		cmd217 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HOL_USER_HOL.sql -o output_file=01_dbDDL_HOL_USER_HOL.sql_out -o quiet=true & " 
		out217 = subprocess.check_output(cmd217,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KEN_DB.sql").st_size != 0:
		cmd218 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KEN_DB.sql -o output_file=01_dbDDL_KEN_DB.sql_out -o quiet=true & " 
		out218 = subprocess.check_output(cmd218,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_FGRANT.sql").st_size != 0:
		cmd219 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_FGRANT.sql -o output_file=01_dbDDL_DB_FGRANT.sql_out -o quiet=true & " 
		out219 = subprocess.check_output(cmd219,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KIRAN.sql").st_size != 0:
		cmd220 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KIRAN.sql -o output_file=01_dbDDL_KIRAN.sql_out -o quiet=true & " 
		out220 = subprocess.check_output(cmd220,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SHASHI.sql").st_size != 0:
		cmd221 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SHASHI.sql -o output_file=01_dbDDL_SHASHI.sql_out -o quiet=true & " 
		out221 = subprocess.check_output(cmd221,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MYTESTDB.sql").st_size != 0:
		cmd222 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MYTESTDB.sql -o output_file=01_dbDDL_MYTESTDB.sql_out -o quiet=true & " 
		out222 = subprocess.check_output(cmd222,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MINDBTEST.sql").st_size != 0:
		cmd223 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MINDBTEST.sql -o output_file=01_dbDDL_MINDBTEST.sql_out -o quiet=true & " 
		out223 = subprocess.check_output(cmd223,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MRAINEY_TEST.sql").st_size != 0:
		cmd224 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MRAINEY_TEST.sql -o output_file=01_dbDDL_MRAINEY_TEST.sql_out -o quiet=true & " 
		out224 = subprocess.check_output(cmd224,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SS_DB.sql").st_size != 0:
		cmd225 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SS_DB.sql -o output_file=01_dbDDL_SS_DB.sql_out -o quiet=true & " 
		out225 = subprocess.check_output(cmd225,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDBNEW.sql").st_size != 0:
		cmd226 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDBNEW.sql -o output_file=01_dbDDL_TESTDBNEW.sql_out -o quiet=true & " 
		out226 = subprocess.check_output(cmd226,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SCDB_CLONE1.sql").st_size != 0:
		cmd227 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SCDB_CLONE1.sql -o output_file=01_dbDDL_SCDB_CLONE1.sql_out -o quiet=true & " 
		out227 = subprocess.check_output(cmd227,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_1_VAIBS.sql").st_size != 0:
		cmd228 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_1_VAIBS.sql -o output_file=01_dbDDL_TEST_1_VAIBS.sql_out -o quiet=true & " 
		out228 = subprocess.check_output(cmd228,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DEV_TEST.sql").st_size != 0:
		cmd229 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DEV_TEST.sql -o output_file=01_dbDDL_DEV_TEST.sql_out -o quiet=true & " 
		out229 = subprocess.check_output(cmd229,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MYSOURCE.sql").st_size != 0:
		cmd230 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MYSOURCE.sql -o output_file=01_dbDDL_MYSOURCE.sql_out -o quiet=true & " 
		out230 = subprocess.check_output(cmd230,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATALAKE_BASE.sql").st_size != 0:
		cmd231 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATALAKE_BASE.sql -o output_file=01_dbDDL_DATALAKE_BASE.sql_out -o quiet=true & " 
		out231 = subprocess.check_output(cmd231,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PARTSTRADER_1.sql").st_size != 0:
		cmd232 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PARTSTRADER_1.sql -o output_file=01_dbDDL_PARTSTRADER_1.sql_out -o quiet=true & " 
		out232 = subprocess.check_output(cmd232,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIESSYSADMIN_TEST.sql").st_size != 0:
		cmd233 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIESSYSADMIN_TEST.sql -o output_file=01_dbDDL_MERRIESSYSADMIN_TEST.sql_out -o quiet=true & " 
		out233 = subprocess.check_output(cmd233,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDADB2.sql").st_size != 0:
		cmd234 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDADB2.sql -o output_file=01_dbDDL_HILDADB2.sql_out -o quiet=true & " 
		out234 = subprocess.check_output(cmd234,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRATIMA_00052584.sql").st_size != 0:
		cmd235 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRATIMA_00052584.sql -o output_file=01_dbDDL_PRATIMA_00052584.sql_out -o quiet=true & " 
		out235 = subprocess.check_output(cmd235,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB2.sql").st_size != 0:
		cmd236 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB2.sql -o output_file=01_dbDDL_DB2.sql_out -o quiet=true & " 
		out236 = subprocess.check_output(cmd236,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DSHEN_DB.sql").st_size != 0:
		cmd237 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DSHEN_DB.sql -o output_file=01_dbDDL_DSHEN_DB.sql_out -o quiet=true & " 
		out237 = subprocess.check_output(cmd237,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_S1_MYTESTDB_RET.sql").st_size != 0:
		cmd238 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_S1_MYTESTDB_RET.sql -o output_file=01_dbDDL_S1_MYTESTDB_RET.sql_out -o quiet=true & " 
		out238 = subprocess.check_output(cmd238,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PROD1_VAIBS_TEST.sql").st_size != 0:
		cmd239 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PROD1_VAIBS_TEST.sql -o output_file=01_dbDDL_PROD1_VAIBS_TEST.sql_out -o quiet=true & " 
		out239 = subprocess.check_output(cmd239,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_TEST_DINESH.sql").st_size != 0:
		cmd240 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_TEST_DINESH.sql -o output_file=01_dbDDL_DB_TEST_DINESH.sql_out -o quiet=true & " 
		out240 = subprocess.check_output(cmd240,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PLAYPEN4J_CLONED_HCODED.sql").st_size != 0:
		cmd241 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PLAYPEN4J_CLONED_HCODED.sql -o output_file=01_dbDDL_PLAYPEN4J_CLONED_HCODED.sql_out -o quiet=true & " 
		out241 = subprocess.check_output(cmd241,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_BRT_CLONE.sql").st_size != 0:
		cmd242 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_BRT_CLONE.sql -o output_file=01_dbDDL_BRT_CLONE.sql_out -o quiet=true & " 
		out242 = subprocess.check_output(cmd242,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SC_DEMO.sql").st_size != 0:
		cmd243 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SC_DEMO.sql -o output_file=01_dbDDL_SC_DEMO.sql_out -o quiet=true & " 
		out243 = subprocess.check_output(cmd243,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HYU.sql").st_size != 0:
		cmd244 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HYU.sql -o output_file=01_dbDDL_HYU.sql_out -o quiet=true & " 
		out244 = subprocess.check_output(cmd244,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HR_DB.sql").st_size != 0:
		cmd245 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HR_DB.sql -o output_file=01_dbDDL_HR_DB.sql_out -o quiet=true & " 
		out245 = subprocess.check_output(cmd245,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_TEST_1.sql").st_size != 0:
		cmd246 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_TEST_1.sql -o output_file=01_dbDDL_DB_TEST_1.sql_out -o quiet=true & " 
		out246 = subprocess.check_output(cmd246,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDB1.sql").st_size != 0:
		cmd247 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDB1.sql -o output_file=01_dbDDL_TESTDB1.sql_out -o quiet=true & " 
		out247 = subprocess.check_output(cmd247,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SECURE_RAW_VAULT.sql").st_size != 0:
		cmd248 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SECURE_RAW_VAULT.sql -o output_file=01_dbDDL_SECURE_RAW_VAULT.sql_out -o quiet=true & " 
		out248 = subprocess.check_output(cmd248,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIES00049821.sql").st_size != 0:
		cmd249 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIES00049821.sql -o output_file=01_dbDDL_MERRIES00049821.sql_out -o quiet=true & " 
		out249 = subprocess.check_output(cmd249,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SOOZ_TEST_DB.sql").st_size != 0:
		cmd250 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SOOZ_TEST_DB.sql -o output_file=01_dbDDL_SOOZ_TEST_DB.sql_out -o quiet=true & " 
		out250 = subprocess.check_output(cmd250,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JSKARPHOL_DB.sql").st_size != 0:
		cmd251 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JSKARPHOL_DB.sql -o output_file=01_dbDDL_JSKARPHOL_DB.sql_out -o quiet=true & " 
		out251 = subprocess.check_output(cmd251,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ABC_DB.sql").st_size != 0:
		cmd252 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ABC_DB.sql -o output_file=01_dbDDL_ABC_DB.sql_out -o quiet=true & " 
		out252 = subprocess.check_output(cmd252,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_TEST1.sql").st_size != 0:
		cmd253 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_TEST1.sql -o output_file=01_dbDDL_DB_TEST1.sql_out -o quiet=true & " 
		out253 = subprocess.check_output(cmd253,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KT_WGOV_DB.sql").st_size != 0:
		cmd254 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KT_WGOV_DB.sql -o output_file=01_dbDDL_KT_WGOV_DB.sql_out -o quiet=true & " 
		out254 = subprocess.check_output(cmd254,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_LABDB.sql").st_size != 0:
		cmd255 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_LABDB.sql -o output_file=01_dbDDL_LABDB.sql_out -o quiet=true & " 
		out255 = subprocess.check_output(cmd255,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SHASHI_DB.sql").st_size != 0:
		cmd256 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SHASHI_DB.sql -o output_file=01_dbDDL_SHASHI_DB.sql_out -o quiet=true & " 
		out256 = subprocess.check_output(cmd256,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NEDWDEV_DB1.sql").st_size != 0:
		cmd257 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NEDWDEV_DB1.sql -o output_file=01_dbDDL_NEDWDEV_DB1.sql_out -o quiet=true & " 
		out257 = subprocess.check_output(cmd257,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_OLITESTDB.sql").st_size != 0:
		cmd258 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_OLITESTDB.sql -o output_file=01_dbDDL_OLITESTDB.sql_out -o quiet=true & " 
		out258 = subprocess.check_output(cmd258,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MY_DB.sql").st_size != 0:
		cmd259 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MY_DB.sql -o output_file=01_dbDDL_MY_DB.sql_out -o quiet=true & " 
		out259 = subprocess.check_output(cmd259,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MINDB_CL_021219.sql").st_size != 0:
		cmd260 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MINDB_CL_021219.sql -o output_file=01_dbDDL_MINDB_CL_021219.sql_out -o quiet=true & " 
		out260 = subprocess.check_output(cmd260,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HEALTHCARE_DATA_SHARING.sql").st_size != 0:
		cmd261 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HEALTHCARE_DATA_SHARING.sql -o output_file=01_dbDDL_HEALTHCARE_DATA_SHARING.sql_out -o quiet=true & " 
		out261 = subprocess.check_output(cmd261,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DEMOTESTDB2.sql").st_size != 0:
		cmd262 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DEMOTESTDB2.sql -o output_file=01_dbDDL_DEMOTESTDB2.sql_out -o quiet=true & " 
		out262 = subprocess.check_output(cmd262,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_GDB2.sql").st_size != 0:
		cmd263 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_GDB2.sql -o output_file=01_dbDDL_GDB2.sql_out -o quiet=true & " 
		out263 = subprocess.check_output(cmd263,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SUPPORT_BASE.sql").st_size != 0:
		cmd264 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SUPPORT_BASE.sql -o output_file=01_dbDDL_SUPPORT_BASE.sql_out -o quiet=true & " 
		out264 = subprocess.check_output(cmd264,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_S2.sql").st_size != 0:
		cmd265 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_S2.sql -o output_file=01_dbDDL_S2.sql_out -o quiet=true & " 
		out265 = subprocess.check_output(cmd265,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DECISIVE.sql").st_size != 0:
		cmd266 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DECISIVE.sql -o output_file=01_dbDDL_DECISIVE.sql_out -o quiet=true & " 
		out266 = subprocess.check_output(cmd266,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBREP.sql").st_size != 0:
		cmd267 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBREP.sql -o output_file=01_dbDDL_DBREP.sql_out -o quiet=true & " 
		out267 = subprocess.check_output(cmd267,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIES_TEST_CLONE49821_1.sql").st_size != 0:
		cmd268 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIES_TEST_CLONE49821_1.sql -o output_file=01_dbDDL_MERRIES_TEST_CLONE49821_1.sql_out -o quiet=true & " 
		out268 = subprocess.check_output(cmd268,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_LANDING.sql").st_size != 0:
		cmd269 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_LANDING.sql -o output_file=01_dbDDL_LANDING.sql_out -o quiet=true & " 
		out269 = subprocess.check_output(cmd269,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CLONE_BUG_TEST3D.sql").st_size != 0:
		cmd270 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CLONE_BUG_TEST3D.sql -o output_file=01_dbDDL_CLONE_BUG_TEST3D.sql_out -o quiet=true & " 
		out270 = subprocess.check_output(cmd270,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SCDB.sql").st_size != 0:
		cmd271 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SCDB.sql -o output_file=01_dbDDL_SCDB.sql_out -o quiet=true & " 
		out271 = subprocess.check_output(cmd271,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDB4.sql").st_size != 0:
		cmd272 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDB4.sql -o output_file=01_dbDDL_TESTDB4.sql_out -o quiet=true & " 
		out272 = subprocess.check_output(cmd272,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TMPDB2.sql").st_size != 0:
		cmd273 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TMPDB2.sql -o output_file=01_dbDDL_TMPDB2.sql_out -o quiet=true & " 
		out273 = subprocess.check_output(cmd273,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_FOR_WH_LAB.sql").st_size != 0:
		cmd274 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_FOR_WH_LAB.sql -o output_file=01_dbDDL_TEST_FOR_WH_LAB.sql_out -o quiet=true & " 
		out274 = subprocess.check_output(cmd274,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_USEREXAMPLES.sql").st_size != 0:
		cmd275 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_USEREXAMPLES.sql -o output_file=01_dbDDL_USEREXAMPLES.sql_out -o quiet=true & " 
		out275 = subprocess.check_output(cmd275,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBTEST_MIN.sql").st_size != 0:
		cmd276 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBTEST_MIN.sql -o output_file=01_dbDDL_DBTEST_MIN.sql_out -o quiet=true & " 
		out276 = subprocess.check_output(cmd276,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATA_CLONING_00049821_1.sql").st_size != 0:
		cmd277 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATA_CLONING_00049821_1.sql -o output_file=01_dbDDL_DATA_CLONING_00049821_1.sql_out -o quiet=true & " 
		out277 = subprocess.check_output(cmd277,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PLAYPEN4JB.sql").st_size != 0:
		cmd278 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PLAYPEN4JB.sql -o output_file=01_dbDDL_PLAYPEN4JB.sql_out -o quiet=true & " 
		out278 = subprocess.check_output(cmd278,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_GDB.sql").st_size != 0:
		cmd279 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_GDB.sql -o output_file=01_dbDDL_GDB.sql_out -o quiet=true & " 
		out279 = subprocess.check_output(cmd279,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DEV_LANDING.sql").st_size != 0:
		cmd280 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DEV_LANDING.sql -o output_file=01_dbDDL_DEV_LANDING.sql_out -o quiet=true & " 
		out280 = subprocess.check_output(cmd280,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CSD_BI.sql").st_size != 0:
		cmd281 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CSD_BI.sql -o output_file=01_dbDDL_CSD_BI.sql_out -o quiet=true & " 
		out281 = subprocess.check_output(cmd281,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MYDB.sql").st_size != 0:
		cmd282 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MYDB.sql -o output_file=01_dbDDL_MYDB.sql_out -o quiet=true & " 
		out282 = subprocess.check_output(cmd282,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VID$.sql").st_size != 0:
		cmd283 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VID$.sql -o output_file=01_dbDDL_VID$.sql_out -o quiet=true & " 
		out283 = subprocess.check_output(cmd283,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PC_ALOOMA_DB.sql").st_size != 0:
		cmd284 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PC_ALOOMA_DB.sql -o output_file=01_dbDDL_PC_ALOOMA_DB.sql_out -o quiet=true & " 
		out284 = subprocess.check_output(cmd284,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SONY_PNT.sql").st_size != 0:
		cmd285 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SONY_PNT.sql -o output_file=01_dbDDL_SONY_PNT.sql_out -o quiet=true & " 
		out285 = subprocess.check_output(cmd285,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB11.sql").st_size != 0:
		cmd286 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB11.sql -o output_file=01_dbDDL_DB11.sql_out -o quiet=true & " 
		out286 = subprocess.check_output(cmd286,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDBV1.sql").st_size != 0:
		cmd287 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDBV1.sql -o output_file=01_dbDDL_TESTDBV1.sql_out -o quiet=true & " 
		out287 = subprocess.check_output(cmd287,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_EX1_GOR_Y.sql").st_size != 0:
		cmd288 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_EX1_GOR_Y.sql -o output_file=01_dbDDL_EX1_GOR_Y.sql_out -o quiet=true & " 
		out288 = subprocess.check_output(cmd288,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDCLONEDB_CLONE.sql").st_size != 0:
		cmd289 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDCLONEDB_CLONE.sql -o output_file=01_dbDDL_MDCLONEDB_CLONE.sql_out -o quiet=true & " 
		out289 = subprocess.check_output(cmd289,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIES_TEST_CLONE49821_3W.sql").st_size != 0:
		cmd290 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIES_TEST_CLONE49821_3W.sql -o output_file=01_dbDDL_MERRIES_TEST_CLONE49821_3W.sql_out -o quiet=true & " 
		out290 = subprocess.check_output(cmd290,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDONOVAN_DB_SNOWCHANGE_DEV.sql").st_size != 0:
		cmd291 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDONOVAN_DB_SNOWCHANGE_DEV.sql -o output_file=01_dbDDL_MDONOVAN_DB_SNOWCHANGE_DEV.sql_out -o quiet=true & " 
		out291 = subprocess.check_output(cmd291,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDATESTDB.sql").st_size != 0:
		cmd292 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDATESTDB.sql -o output_file=01_dbDDL_HILDATESTDB.sql_out -o quiet=true & " 
		out292 = subprocess.check_output(cmd292,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SHASHI_DB2.sql").st_size != 0:
		cmd293 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SHASHI_DB2.sql -o output_file=01_dbDDL_SHASHI_DB2.sql_out -o quiet=true & " 
		out293 = subprocess.check_output(cmd293,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_S1.sql").st_size != 0:
		cmd294 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_S1.sql -o output_file=01_dbDDL_S1.sql_out -o quiet=true & " 
		out294 = subprocess.check_output(cmd294,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DARWOODS_DB.sql").st_size != 0:
		cmd295 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DARWOODS_DB.sql -o output_file=01_dbDDL_DARWOODS_DB.sql_out -o quiet=true & " 
		out295 = subprocess.check_output(cmd295,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDB_DEV_DB.sql").st_size != 0:
		cmd296 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDB_DEV_DB.sql -o output_file=01_dbDDL_MDB_DEV_DB.sql_out -o quiet=true & " 
		out296 = subprocess.check_output(cmd296,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB1_TEST.sql").st_size != 0:
		cmd297 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB1_TEST.sql -o output_file=01_dbDDL_DB1_TEST.sql_out -o quiet=true & " 
		out297 = subprocess.check_output(cmd297,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_TEST_SYSADMIN234.sql").st_size != 0:
		cmd298 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_TEST_SYSADMIN234.sql -o output_file=01_dbDDL_VAIBHAVI_TEST_SYSADMIN234.sql_out -o quiet=true & " 
		out298 = subprocess.check_output(cmd298,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JLG.sql").st_size != 0:
		cmd299 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JLG.sql -o output_file=01_dbDDL_JLG.sql_out -o quiet=true & " 
		out299 = subprocess.check_output(cmd299,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PDX_DEV.sql").st_size != 0:
		cmd300 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PDX_DEV.sql -o output_file=01_dbDDL_PDX_DEV.sql_out -o quiet=true & " 
		out300 = subprocess.check_output(cmd300,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_EX1_GOR_X.sql").st_size != 0:
		cmd301 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_EX1_GOR_X.sql -o output_file=01_dbDDL_EX1_GOR_X.sql_out -o quiet=true & " 
		out301 = subprocess.check_output(cmd301,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NEDWDEV_DB3.sql").st_size != 0:
		cmd302 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NEDWDEV_DB3.sql -o output_file=01_dbDDL_NEDWDEV_DB3.sql_out -o quiet=true & " 
		out302 = subprocess.check_output(cmd302,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TMPDB1.sql").st_size != 0:
		cmd303 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TMPDB1.sql -o output_file=01_dbDDL_TMPDB1.sql_out -o quiet=true & " 
		out303 = subprocess.check_output(cmd303,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SHASHI_DUMMY.sql").st_size != 0:
		cmd304 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SHASHI_DUMMY.sql -o output_file=01_dbDDL_SHASHI_DUMMY.sql_out -o quiet=true & " 
		out304 = subprocess.check_output(cmd304,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATA_CLONING_00049821_2.sql").st_size != 0:
		cmd305 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATA_CLONING_00049821_2.sql -o output_file=01_dbDDL_DATA_CLONING_00049821_2.sql_out -o quiet=true & " 
		out305 = subprocess.check_output(cmd305,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_COMMON_DB.sql").st_size != 0:
		cmd306 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_COMMON_DB.sql -o output_file=01_dbDDL_COMMON_DB.sql_out -o quiet=true & " 
		out306 = subprocess.check_output(cmd306,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SANDBOX_JB.sql").st_size != 0:
		cmd307 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SANDBOX_JB.sql -o output_file=01_dbDDL_SANDBOX_JB.sql_out -o quiet=true & " 
		out307 = subprocess.check_output(cmd307,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_YDB.sql").st_size != 0:
		cmd308 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_YDB.sql -o output_file=01_dbDDL_YDB.sql_out -o quiet=true & " 
		out308 = subprocess.check_output(cmd308,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KHOYLE_DB_CISCO.sql").st_size != 0:
		cmd309 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KHOYLE_DB_CISCO.sql -o output_file=01_dbDDL_KHOYLE_DB_CISCO.sql_out -o quiet=true & " 
		out309 = subprocess.check_output(cmd309,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATA_RECOVERY_DB.sql").st_size != 0:
		cmd310 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATA_RECOVERY_DB.sql -o output_file=01_dbDDL_DATA_RECOVERY_DB.sql_out -o quiet=true & " 
		out310 = subprocess.check_output(cmd310,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATABASE_A.sql").st_size != 0:
		cmd311 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATABASE_A.sql -o output_file=01_dbDDL_DATABASE_A.sql_out -o quiet=true & " 
		out311 = subprocess.check_output(cmd311,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DAVID_NOADMIN.sql").st_size != 0:
		cmd312 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DAVID_NOADMIN.sql -o output_file=01_dbDDL_DAVID_NOADMIN.sql_out -o quiet=true & " 
		out312 = subprocess.check_output(cmd312,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDB_CLONE.sql").st_size != 0:
		cmd313 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDB_CLONE.sql -o output_file=01_dbDDL_TESTDB_CLONE.sql_out -o quiet=true & " 
		out313 = subprocess.check_output(cmd313,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CLONE_SCOTTDB1.sql").st_size != 0:
		cmd314 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CLONE_SCOTTDB1.sql -o output_file=01_dbDDL_CLONE_SCOTTDB1.sql_out -o quiet=true & " 
		out314 = subprocess.check_output(cmd314,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRATIDB_CLONE.sql").st_size != 0:
		cmd315 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRATIDB_CLONE.sql -o output_file=01_dbDDL_PRATIDB_CLONE.sql_out -o quiet=true & " 
		out315 = subprocess.check_output(cmd315,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_FUTURE_GRANT_TEST.sql").st_size != 0:
		cmd316 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_FUTURE_GRANT_TEST.sql -o output_file=01_dbDDL_FUTURE_GRANT_TEST.sql_out -o quiet=true & " 
		out316 = subprocess.check_output(cmd316,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SONY_DM.sql").st_size != 0:
		cmd317 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SONY_DM.sql -o output_file=01_dbDDL_SONY_DM.sql_out -o quiet=true & " 
		out317 = subprocess.check_output(cmd317,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_EDW_RPTG_DB.sql").st_size != 0:
		cmd318 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_EDW_RPTG_DB.sql -o output_file=01_dbDDL_EDW_RPTG_DB.sql_out -o quiet=true & " 
		out318 = subprocess.check_output(cmd318,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_TEST_ACCT1.sql").st_size != 0:
		cmd319 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_TEST_ACCT1.sql -o output_file=01_dbDDL_VAIBHAVI_TEST_ACCT1.sql_out -o quiet=true & " 
		out319 = subprocess.check_output(cmd319,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_Z8445.sql").st_size != 0:
		cmd320 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_Z8445.sql -o output_file=01_dbDDL_Z8445.sql_out -o quiet=true & " 
		out320 = subprocess.check_output(cmd320,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_WEDDINGWIRE.sql").st_size != 0:
		cmd321 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_WEDDINGWIRE.sql -o output_file=01_dbDDL_WEDDINGWIRE.sql_out -o quiet=true & " 
		out321 = subprocess.check_output(cmd321,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DWH_PROD.sql").st_size != 0:
		cmd322 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DWH_PROD.sql -o output_file=01_dbDDL_DWH_PROD.sql_out -o quiet=true & " 
		out322 = subprocess.check_output(cmd322,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MYTESTDB2.sql").st_size != 0:
		cmd323 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MYTESTDB2.sql -o output_file=01_dbDDL_MYTESTDB2.sql_out -o quiet=true & " 
		out323 = subprocess.check_output(cmd323,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SF_TUTS1.sql").st_size != 0:
		cmd324 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SF_TUTS1.sql -o output_file=01_dbDDL_SF_TUTS1.sql_out -o quiet=true & " 
		out324 = subprocess.check_output(cmd324,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDONOVAN_DB_SNOWCHANGE_TEST.sql").st_size != 0:
		cmd325 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDONOVAN_DB_SNOWCHANGE_TEST.sql -o output_file=01_dbDDL_MDONOVAN_DB_SNOWCHANGE_TEST.sql_out -o quiet=true & " 
		out325 = subprocess.check_output(cmd325,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TRAININGDB.sql").st_size != 0:
		cmd326 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TRAININGDB.sql -o output_file=01_dbDDL_TRAININGDB.sql_out -o quiet=true & " 
		out326 = subprocess.check_output(cmd326,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_EDW_RPTG_DB11TEST.sql").st_size != 0:
		cmd327 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_EDW_RPTG_DB11TEST.sql -o output_file=01_dbDDL_EDW_RPTG_DB11TEST.sql_out -o quiet=true & " 
		out327 = subprocess.check_output(cmd327,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SUPPORT_DB.sql").st_size != 0:
		cmd328 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SUPPORT_DB.sql -o output_file=01_dbDDL_SUPPORT_DB.sql_out -o quiet=true & " 
		out328 = subprocess.check_output(cmd328,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PROD_VAIBS_TEST.sql").st_size != 0:
		cmd329 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PROD_VAIBS_TEST.sql -o output_file=01_dbDDL_PROD_VAIBS_TEST.sql_out -o quiet=true & " 
		out329 = subprocess.check_output(cmd329,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SFSUPPORT_DB.sql").st_size != 0:
		cmd330 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SFSUPPORT_DB.sql -o output_file=01_dbDDL_SFSUPPORT_DB.sql_out -o quiet=true & " 
		out330 = subprocess.check_output(cmd330,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SECURE_USERXX_DB.sql").st_size != 0:
		cmd331 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SECURE_USERXX_DB.sql -o output_file=01_dbDDL_SECURE_USERXX_DB.sql_out -o quiet=true & " 
		out331 = subprocess.check_output(cmd331,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_FK_TEST.sql").st_size != 0:
		cmd332 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_FK_TEST.sql -o output_file=01_dbDDL_FK_TEST.sql_out -o quiet=true & " 
		out332 = subprocess.check_output(cmd332,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_YAMAHA.sql").st_size != 0:
		cmd333 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_YAMAHA.sql -o output_file=01_dbDDL_YAMAHA.sql_out -o quiet=true & " 
		out333 = subprocess.check_output(cmd333,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DV_TEST.sql").st_size != 0:
		cmd334 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DV_TEST.sql -o output_file=01_dbDDL_DV_TEST.sql_out -o quiet=true & " 
		out334 = subprocess.check_output(cmd334,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBRYANT_DB.sql").st_size != 0:
		cmd335 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBRYANT_DB.sql -o output_file=01_dbDDL_DBRYANT_DB.sql_out -o quiet=true & " 
		out335 = subprocess.check_output(cmd335,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_BB_SF_TUTS.sql").st_size != 0:
		cmd336 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_BB_SF_TUTS.sql -o output_file=01_dbDDL_BB_SF_TUTS.sql_out -o quiet=true & " 
		out336 = subprocess.check_output(cmd336,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ROHAN_TESTING.sql").st_size != 0:
		cmd337 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ROHAN_TESTING.sql -o output_file=01_dbDDL_ROHAN_TESTING.sql_out -o quiet=true & " 
		out337 = subprocess.check_output(cmd337,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_AYUSHI_TEST_CLONE.sql").st_size != 0:
		cmd338 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_AYUSHI_TEST_CLONE.sql -o output_file=01_dbDDL_AYUSHI_TEST_CLONE.sql_out -o quiet=true & " 
		out338 = subprocess.check_output(cmd338,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CITADEL.sql").st_size != 0:
		cmd339 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CITADEL.sql -o output_file=01_dbDDL_CITADEL.sql_out -o quiet=true & " 
		out339 = subprocess.check_output(cmd339,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KELVIN_TEST_DB.sql").st_size != 0:
		cmd340 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KELVIN_TEST_DB.sql -o output_file=01_dbDDL_KELVIN_TEST_DB.sql_out -o quiet=true & " 
		out340 = subprocess.check_output(cmd340,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ABC.sql").st_size != 0:
		cmd341 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ABC.sql -o output_file=01_dbDDL_ABC.sql_out -o quiet=true & " 
		out341 = subprocess.check_output(cmd341,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDB.sql").st_size != 0:
		cmd342 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDB.sql -o output_file=01_dbDDL_TESTDB.sql_out -o quiet=true & " 
		out342 = subprocess.check_output(cmd342,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JIMMYADMINONLY.sql").st_size != 0:
		cmd343 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JIMMYADMINONLY.sql -o output_file=01_dbDDL_JIMMYADMINONLY.sql_out -o quiet=true & " 
		out343 = subprocess.check_output(cmd343,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JIMWHITE.sql").st_size != 0:
		cmd344 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JIMWHITE.sql -o output_file=01_dbDDL_JIMWHITE.sql_out -o quiet=true & " 
		out344 = subprocess.check_output(cmd344,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DAYBREAK.sql").st_size != 0:
		cmd345 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DAYBREAK.sql -o output_file=01_dbDDL_DAYBREAK.sql_out -o quiet=true & " 
		out345 = subprocess.check_output(cmd345,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_00051189.sql").st_size != 0:
		cmd346 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_00051189.sql -o output_file=01_dbDDL_TEST_00051189.sql_out -o quiet=true & " 
		out346 = subprocess.check_output(cmd346,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KHAT_TEST.sql").st_size != 0:
		cmd347 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KHAT_TEST.sql -o output_file=01_dbDDL_KHAT_TEST.sql_out -o quiet=true & " 
		out347 = subprocess.check_output(cmd347,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDONOVAN_DB_ET.sql").st_size != 0:
		cmd348 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDONOVAN_DB_ET.sql -o output_file=01_dbDDL_MDONOVAN_DB_ET.sql_out -o quiet=true & " 
		out348 = subprocess.check_output(cmd348,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TSTEV3.sql").st_size != 0:
		cmd349 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TSTEV3.sql -o output_file=01_dbDDL_TSTEV3.sql_out -o quiet=true & " 
		out349 = subprocess.check_output(cmd349,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SONY_REF.sql").st_size != 0:
		cmd350 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SONY_REF.sql -o output_file=01_dbDDL_SONY_REF.sql_out -o quiet=true & " 
		out350 = subprocess.check_output(cmd350,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_DB.sql").st_size != 0:
		cmd351 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_DB.sql -o output_file=01_dbDDL_TEST_DB.sql_out -o quiet=true & " 
		out351 = subprocess.check_output(cmd351,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ORDERMANAGEMENT.sql").st_size != 0:
		cmd352 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ORDERMANAGEMENT.sql -o output_file=01_dbDDL_ORDERMANAGEMENT.sql_out -o quiet=true & " 
		out352 = subprocess.check_output(cmd352,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PC_FIVETRAN_DB.sql").st_size != 0:
		cmd353 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PC_FIVETRAN_DB.sql -o output_file=01_dbDDL_PC_FIVETRAN_DB.sql_out -o quiet=true & " 
		out353 = subprocess.check_output(cmd353,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIES_TEST.sql").st_size != 0:
		cmd354 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIES_TEST.sql -o output_file=01_dbDDL_MERRIES_TEST.sql_out -o quiet=true & " 
		out354 = subprocess.check_output(cmd354,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JOEL_TEST.sql").st_size != 0:
		cmd355 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JOEL_TEST.sql -o output_file=01_dbDDL_JOEL_TEST.sql_out -o quiet=true & " 
		out355 = subprocess.check_output(cmd355,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_42401_2.sql").st_size != 0:
		cmd356 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_42401_2.sql -o output_file=01_dbDDL_DB_42401_2.sql_out -o quiet=true & " 
		out356 = subprocess.check_output(cmd356,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_00024298.sql").st_size != 0:
		cmd357 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_00024298.sql -o output_file=01_dbDDL_DB_00024298.sql_out -o quiet=true & " 
		out357 = subprocess.check_output(cmd357,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_BSW_SUPPORT.sql").st_size != 0:
		cmd358 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_BSW_SUPPORT.sql -o output_file=01_dbDDL_BSW_SUPPORT.sql_out -o quiet=true & " 
		out358 = subprocess.check_output(cmd358,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_GRANTS_TEST_DB.sql").st_size != 0:
		cmd359 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_GRANTS_TEST_DB.sql -o output_file=01_dbDDL_GRANTS_TEST_DB.sql_out -o quiet=true & " 
		out359 = subprocess.check_output(cmd359,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TUSHAR_GARG_HOL.sql").st_size != 0:
		cmd360 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TUSHAR_GARG_HOL.sql -o output_file=01_dbDDL_TUSHAR_GARG_HOL.sql_out -o quiet=true & " 
		out360 = subprocess.check_output(cmd360,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JL_TEST.sql").st_size != 0:
		cmd361 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JL_TEST.sql -o output_file=01_dbDDL_JL_TEST.sql_out -o quiet=true & " 
		out361 = subprocess.check_output(cmd361,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_HILDA_CLONE.sql").st_size != 0:
		cmd362 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_HILDA_CLONE.sql -o output_file=01_dbDDL_HILDA_CLONE.sql_out -o quiet=true & " 
		out362 = subprocess.check_output(cmd362,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SONY_ODS.sql").st_size != 0:
		cmd363 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SONY_ODS.sql -o output_file=01_dbDDL_SONY_ODS.sql_out -o quiet=true & " 
		out363 = subprocess.check_output(cmd363,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SOURCE_DB.sql").st_size != 0:
		cmd364 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SOURCE_DB.sql -o output_file=01_dbDDL_SOURCE_DB.sql_out -o quiet=true & " 
		out364 = subprocess.check_output(cmd364,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MD_DATALOAD_TEST1004.sql").st_size != 0:
		cmd365 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MD_DATALOAD_TEST1004.sql -o output_file=01_dbDDL_MD_DATALOAD_TEST1004.sql_out -o quiet=true & " 
		out365 = subprocess.check_output(cmd365,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MYDATABASE.sql").st_size != 0:
		cmd366 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MYDATABASE.sql -o output_file=01_dbDDL_MYDATABASE.sql_out -o quiet=true & " 
		out366 = subprocess.check_output(cmd366,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_JIMMYS.sql").st_size != 0:
		cmd367 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_JIMMYS.sql -o output_file=01_dbDDL_JIMMYS.sql_out -o quiet=true & " 
		out367 = subprocess.check_output(cmd367,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ABHI_REDDY_DB.sql").st_size != 0:
		cmd368 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ABHI_REDDY_DB.sql -o output_file=01_dbDDL_ABHI_REDDY_DB.sql_out -o quiet=true & " 
		out368 = subprocess.check_output(cmd368,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_TEST_2.sql").st_size != 0:
		cmd369 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_TEST_2.sql -o output_file=01_dbDDL_VAIBHAVI_TEST_2.sql_out -o quiet=true & " 
		out369 = subprocess.check_output(cmd369,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDB155.sql").st_size != 0:
		cmd370 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDB155.sql -o output_file=01_dbDDL_TESTDB155.sql_out -o quiet=true & " 
		out370 = subprocess.check_output(cmd370,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DAVID_DB_TT.sql").st_size != 0:
		cmd371 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DAVID_DB_TT.sql -o output_file=01_dbDDL_DAVID_DB_TT.sql_out -o quiet=true & " 
		out371 = subprocess.check_output(cmd371,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRD_PF_DB.sql").st_size != 0:
		cmd372 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRD_PF_DB.sql -o output_file=01_dbDDL_PRD_PF_DB.sql_out -o quiet=true & " 
		out372 = subprocess.check_output(cmd372,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_EDW_RPTG_DB1.sql").st_size != 0:
		cmd373 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_EDW_RPTG_DB1.sql -o output_file=01_dbDDL_EDW_RPTG_DB1.sql_out -o quiet=true & " 
		out373 = subprocess.check_output(cmd373,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SRINI_SELVARAJ_DB.sql").st_size != 0:
		cmd374 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SRINI_SELVARAJ_DB.sql -o output_file=01_dbDDL_SRINI_SELVARAJ_DB.sql_out -o quiet=true & " 
		out374 = subprocess.check_output(cmd374,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_REP.sql").st_size != 0:
		cmd375 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_REP.sql -o output_file=01_dbDDL_DB_REP.sql_out -o quiet=true & " 
		out375 = subprocess.check_output(cmd375,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NEDWPRD_DB1.sql").st_size != 0:
		cmd376 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NEDWPRD_DB1.sql -o output_file=01_dbDDL_NEDWPRD_DB1.sql_out -o quiet=true & " 
		out376 = subprocess.check_output(cmd376,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_BRT_SAMPLE.sql").st_size != 0:
		cmd377 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_BRT_SAMPLE.sql -o output_file=01_dbDDL_BRT_SAMPLE.sql_out -o quiet=true & " 
		out377 = subprocess.check_output(cmd377,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TESTDB_1.sql").st_size != 0:
		cmd378 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TESTDB_1.sql -o output_file=01_dbDDL_TESTDB_1.sql_out -o quiet=true & " 
		out378 = subprocess.check_output(cmd378,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DBRYANT_TEST_DATATYPES.sql").st_size != 0:
		cmd379 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DBRYANT_TEST_DATATYPES.sql -o output_file=01_dbDDL_DBRYANT_TEST_DATATYPES.sql_out -o quiet=true & " 
		out379 = subprocess.check_output(cmd379,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VIN1.sql").st_size != 0:
		cmd380 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VIN1.sql -o output_file=01_dbDDL_VIN1.sql_out -o quiet=true & " 
		out380 = subprocess.check_output(cmd380,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDONOVAN_SFC_DB_COPY.sql").st_size != 0:
		cmd381 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDONOVAN_SFC_DB_COPY.sql -o output_file=01_dbDDL_MDONOVAN_SFC_DB_COPY.sql_out -o quiet=true & " 
		out381 = subprocess.check_output(cmd381,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_PROD.sql").st_size != 0:
		cmd382 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_PROD.sql -o output_file=01_dbDDL_VAIBHAVI_PROD.sql_out -o quiet=true & " 
		out382 = subprocess.check_output(cmd382,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_CIRCLECI.sql").st_size != 0:
		cmd383 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_CIRCLECI.sql -o output_file=01_dbDDL_CIRCLECI.sql_out -o quiet=true & " 
		out383 = subprocess.check_output(cmd383,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PRATIDB.sql").st_size != 0:
		cmd384 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PRATIDB.sql -o output_file=01_dbDDL_PRATIDB.sql_out -o quiet=true & " 
		out384 = subprocess.check_output(cmd384,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PLAYPEN4J_CLONED_DECEMBER.sql").st_size != 0:
		cmd385 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PLAYPEN4J_CLONED_DECEMBER.sql -o output_file=01_dbDDL_PLAYPEN4J_CLONED_DECEMBER.sql_out -o quiet=true & " 
		out385 = subprocess.check_output(cmd385,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MDONOVAN_DB.sql").st_size != 0:
		cmd386 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MDONOVAN_DB.sql -o output_file=01_dbDDL_MDONOVAN_DB.sql_out -o quiet=true & " 
		out386 = subprocess.check_output(cmd386,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PLAYPEN4J.sql").st_size != 0:
		cmd387 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PLAYPEN4J.sql -o output_file=01_dbDDL_PLAYPEN4J.sql_out -o quiet=true & " 
		out387 = subprocess.check_output(cmd387,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ELRONTESTDB.sql").st_size != 0:
		cmd388 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ELRONTESTDB.sql -o output_file=01_dbDDL_ELRONTESTDB.sql_out -o quiet=true & " 
		out388 = subprocess.check_output(cmd388,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_KEN_DB_CLONE.sql").st_size != 0:
		cmd389 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_KEN_DB_CLONE.sql -o output_file=01_dbDDL_KEN_DB_CLONE.sql_out -o quiet=true & " 
		out389 = subprocess.check_output(cmd389,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_TEST_1.sql").st_size != 0:
		cmd390 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_TEST_1.sql -o output_file=01_dbDDL_VAIBHAVI_TEST_1.sql_out -o quiet=true & " 
		out390 = subprocess.check_output(cmd390,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_RETENTION.sql").st_size != 0:
		cmd391 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_RETENTION.sql -o output_file=01_dbDDL_RETENTION.sql_out -o quiet=true & " 
		out391 = subprocess.check_output(cmd391,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_NEGATIVE.sql").st_size != 0:
		cmd392 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_NEGATIVE.sql -o output_file=01_dbDDL_NEGATIVE.sql_out -o quiet=true & " 
		out392 = subprocess.check_output(cmd392,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_SECURE_VW_DB.sql").st_size != 0:
		cmd393 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_SECURE_VW_DB.sql -o output_file=01_dbDDL_SECURE_VW_DB.sql_out -o quiet=true & " 
		out393 = subprocess.check_output(cmd393,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DINESH.sql").st_size != 0:
		cmd394 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DINESH.sql -o output_file=01_dbDDL_DINESH.sql_out -o quiet=true & " 
		out394 = subprocess.check_output(cmd394,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DATAVAULT_MIN.sql").st_size != 0:
		cmd395 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DATAVAULT_MIN.sql -o output_file=01_dbDDL_DATAVAULT_MIN.sql_out -o quiet=true & " 
		out395 = subprocess.check_output(cmd395,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_ROLE2_DB.sql").st_size != 0:
		cmd396 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_ROLE2_DB.sql -o output_file=01_dbDDL_ROLE2_DB.sql_out -o quiet=true & " 
		out396 = subprocess.check_output(cmd396,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MERRIES_TEST_CLONE49821.sql").st_size != 0:
		cmd397 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MERRIES_TEST_CLONE49821.sql -o output_file=01_dbDDL_MERRIES_TEST_CLONE49821.sql_out -o quiet=true & " 
		out397 = subprocess.check_output(cmd397,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_WAREHOUSE_MGMT_LOAD_DB.sql").st_size != 0:
		cmd398 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_WAREHOUSE_MGMT_LOAD_DB.sql -o output_file=01_dbDDL_WAREHOUSE_MGMT_LOAD_DB.sql_out -o quiet=true & " 
		out398 = subprocess.check_output(cmd398,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MIGRATION_DB.sql").st_size != 0:
		cmd399 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MIGRATION_DB.sql -o output_file=01_dbDDL_MIGRATION_DB.sql_out -o quiet=true & " 
		out399 = subprocess.check_output(cmd399,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TEST_RO1.sql").st_size != 0:
		cmd400 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TEST_RO1.sql -o output_file=01_dbDDL_TEST_RO1.sql_out -o quiet=true & " 
		out400 = subprocess.check_output(cmd400,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VAIBHAVI_DEV.sql").st_size != 0:
		cmd401 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VAIBHAVI_DEV.sql -o output_file=01_dbDDL_VAIBHAVI_DEV.sql_out -o quiet=true & " 
		out401 = subprocess.check_output(cmd401,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_TEST.sql").st_size != 0:
		cmd402 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_TEST.sql -o output_file=01_dbDDL_DB_TEST.sql_out -o quiet=true & " 
		out402 = subprocess.check_output(cmd402,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_AYUSHI_TEST_CLONE1.sql").st_size != 0:
		cmd403 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_AYUSHI_TEST_CLONE1.sql -o output_file=01_dbDDL_AYUSHI_TEST_CLONE1.sql_out -o quiet=true & " 
		out403 = subprocess.check_output(cmd403,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_PARTSTRADER.sql").st_size != 0:
		cmd404 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_PARTSTRADER.sql -o output_file=01_dbDDL_PARTSTRADER.sql_out -o quiet=true & " 
		out404 = subprocess.check_output(cmd404,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_TARGET_DB.sql").st_size != 0:
		cmd405 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_TARGET_DB.sql -o output_file=01_dbDDL_TARGET_DB.sql_out -o quiet=true & " 
		out405 = subprocess.check_output(cmd405,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_DB_DATABASE.sql").st_size != 0:
		cmd406 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_DB_DATABASE.sql -o output_file=01_dbDDL_DB_DATABASE.sql_out -o quiet=true & " 
		out406 = subprocess.check_output(cmd406,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_MANAGE_DB.sql").st_size != 0:
		cmd407 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_MANAGE_DB.sql -o output_file=01_dbDDL_MANAGE_DB.sql_out -o quiet=true & " 
		out407 = subprocess.check_output(cmd407,stderr=subprocess.STDOUT, shell=True)

	if os.stat("01_dbDDL_VERIVOX_DWH.sql").st_size != 0:
		cmd408 = "snowsql -c migwest -r repadmin  -w WH_CROSSREP -f 01_dbDDL_VERIVOX_DWH.sql -o output_file=01_dbDDL_VERIVOX_DWH.sql_out -o quiet=true & " 
		out408 = subprocess.check_output(cmd408,stderr=subprocess.STDOUT, shell=True)

except subprocess.CalledProcessError as e:
	print(e)
