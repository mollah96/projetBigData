import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "BEGIN BATCH \n"
with open('police.csv') as tsvfile:
    reader = csv.reader(tsvfile)
    header = False
    counter = 0
    for row in reader:
        if not header:
            header = True
        else:
            state = row[0]
            total = row[1]
            police = row[2]
            rate = row[3]
            sql_single = 'INSERT INTO police(state,police_spending,rate,total_income) VALUES (' + '\''+state+'\'' + ','+str(police)+',' + str(rate)+',' + str(total)+');\n'
            sql += sql_single
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