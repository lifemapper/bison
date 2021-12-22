import csv
import sys 
csv.field_size_limit(sys.maxsize)
fname = '/tank/data/bison/2019/Terr/tmp/smoketest_10m_2020_04_21.csv'
fname = '/tank/data/bison/2019/Terr/tmp/step3_occurrence_lines_60000001-70000001_1931703-2897553.csv'
fname = '/tank/data/bison/2019/Terr/tmp/step1_occurrence_lines_60000001-70000001.csv'
gbidid_idx = 0
locality_idx = 45
mrgid_idx = 46
eez_idx = 47
problem_row = 2839477
problem_id = '2031886996'

i = 0
with open(fname, 'r', encoding='utf-8') as f:
    r = csv.reader(f, delimiter='$', quoting=csv.QUOTE_NONE)        
    for row in r:
        if i == 0:
            header = row
        else:
            gbifid = row[gbidid_idx]
            verbatim_locality = row[locality_idx]
            mrgid = row[mrgid_idx]
            eez = row[eez_idx]
            if i == problem_row or gbifid == problem_id:
                print('gbifid {}, locality {}, mrgid {}, eez {}'.format(
                    gbifid, verbatim_locality, mrgid, eez))
                for j in range(len(row)):
                    print('{} $$*$${}$$*$$'.format(j, row[j]))
                break
        i = i+1
print(i)

fname = '/tank/data/bison/2019/Terr/occurrence_lines_60000001-70000001.csv'
locality_idx = 124
vlocality_idx = 125
i = 0
with open(fname, 'r', encoding='utf-8') as f:
    r = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)        
    for row in r:
        gbifid = row[0]
        locality = row[locality_idx]
        verbatim_locality = row[vlocality_idx]
        if gbifid == problem_id:
            print('gbifid {}, locality {}, vlocality {}'.format(
                gbifid, locality, verbatim_locality))
            for j in range(len(row)):
                print('{} $$*$${}$$*$$'.format(j, row[j]))
                if j == 124:
                    pass
            break
        i = i+1
print(i)

"""
row 165048, gbifid 2213699335, column 45 verbatim_locality is 3218607 chars long
row 8731100, gbifid 2046976295, column 45 verbatim_locality is 491772 chars long

"""