import csv
import sys 
csv.field_size_limit(sys.maxsize)
fname = '/tank/data/bison/2019/Terr/tmp/smoketest_10m_2020_04_11.csv'
gbidid_idx = 37
locality_idx = 45

i = 0
with open(fname, 'r', encoding='utf-8') as f:
    r = csv.reader(f, delimiter='$', quoting=csv.QUOTE_NONE)        
    for row in r:
        if i == 0:
            header = row
        else:
            for j in len(header):
                if(len(row[j]) > 2000):
                    print('row {}, gbifid {}, column {} {} is {} chars long'.format(
                        i, row[gbidid_idx], j, header[j], len(row[j])))
        i = i+1
print(i)

"""
row 165048, gbifid 2213699335, column 45 verbatim_locality is 3218607 chars long
row 8731100, gbifid 2046976295, column 45 verbatim_locality is 491772 chars long

"""