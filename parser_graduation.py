import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "BEGIN BATCH \n"
with open('graduation_clear.csv') as tsvfile:
    reader = csv.reader(tsvfile)
    header = False
    counter = 0
    for row in reader:
        if not header:
            header = True
        else:
            state = row[0]
            year = 2000
            for rate in row[1:]:
                year += 1
                sql_single = 'INSERT INTO graduation(state,year,grad_rate) VALUES (' + '\''+state+'\'' + ','+str(year)+',' + str(rate)+');\n'
                sql += sql_single
                print(sql_single)
                counter += 1
                if counter % 100 == 0:
                    sql += "APPLY BATCH;"
                    session.execute(sql)
                    sql = "BEGIN BATCH \n"

    if sql != "BEGIN BATCH \n":
        sql += "APPLY BATCH;"
        session.execute(sql)
        session.execute(sql)
    print(counter)
    cluster.shutdown()