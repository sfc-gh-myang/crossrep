from unittest import TestCase
import os
import re

class TestGenDatabaseDDL(TestCase):
    def test_genDatabaseDDL(self):
        cwd = os.getcwd()
        filep_ath = os.path.join(cwd, "../scripts/test_ddl.txt")
        f = open(filep_ath, "r")
        ddl = f.read()
        # ddl=re.sub(r'\"([^:]+):.+',r'\1',ddl)
        # print('before ===> ' + ddl)
        # ddl=re.sub(r'create\s+(or\s+replace\s+)?(database|schema|table|view|materialized view|file format|function|sequence|procedure|materliazed view|external table|stage|pipe|stream|task)\s+',r'create \2 if not exists ',ddl,flags=re.MULTILINE|re.IGNORECASE)
        # new get_ddl will output with "if not exists", no need to replace any more, comment out line 120,121 (next 2 lines) and 124
        # ddl=re.sub(r'create\s+(or\s+replace\s+)?(table|view|materialized view|file format|function|sequence|procedure|external table|stage|pipe|stream|task)\s+',r'create \2 if not exists ',ddl,flags=re.MULTILINE|re.IGNORECASE)
        # ddl=re.sub(r'create\s+(or\s+replace\s+)?(database|schema)\s+(\S+)(\s+)?;',r'create \2 if not exists \3;\n use \2 \3;',ddl,flags=re.MULTILINE|re.IGNORECASE)
        # create VIEW if not exists S2.VIEW1 COPY GRANTS AS SELECT * FROM S1.TAB1;
        # remove COPY GRANTS on create view - not needed for newly created objects/grants
        # ddl=re.sub(r'create\s+(view if not exists)\s+(\S)+\s+(COPY GRANTS)\s+',r'create view if not exists \2 ',ddl,flags=re.MULTILINE|re.IGNORECASE)
        # print('after ===> ' + ddl)
        # FIELD_OPTIONALLY_ENCLOSED_BY = ''' ==> FIELD_OPTIONALLY_ENCLOSED_BY = ''''
        # ddl=re.sub(r'FIELD_OPTIONALLY_ENCLOSED_BY = \'\'\'',r'FIELD_OPTIONALLY_ENCLOSED_BY = "\'"',ddl,flags=re.MULTILINE|re.IGNORECASE)

        ddl2 = re.sub(r"FIELD_OPTIONALLY_ENCLOSED_BY = '''", r"FIELD_OPTIONALLY_ENCLOSED_BY = ''''", ddl,
                     flags=re.MULTILINE | re.IGNORECASE)
        tokens = ddl.split(';\n')
        tokens2 = ddl2.split(';\n')

        for i in range(len(tokens)):
            results = ''
            results2 = ''
            results3 = ''
            if tokens[i] != tokens2[i]:
                for j in range(min(len(tokens[i]), len(tokens2[i]))):
                    if tokens[i][j] == tokens2[i][j]:
                        results += tokens[i];
                    else:
                        results += '-->'
                        results2 = tokens[i][j:]
                        results3 = tokens2[i][j:]
                        break;
            print(results)
            if results2 != '':
                print(results2)
                print(results3)
        self.fail()
