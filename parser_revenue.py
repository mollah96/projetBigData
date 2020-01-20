import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "BEGIN BATCH \n"
with open('revenue.csv') as tsvfile:
    reader = csv.reader(tsvfile,delimiter=';')
    header = False
    counter = 0
    for row in reader:
        if not header:
            header = True
        else:
            print(row)
            state = row[0]
            total = row[1]
            cost = row[2]
            pct = row[3]
            sql_single = 'INSERT INTO revenue(state,total_revenue,cost_per_pupil,pct_state_total) VALUES (' + '\''+state+'\'' + ','+str(total)+',' + str(cost)+','+str(pct)+');\n'
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