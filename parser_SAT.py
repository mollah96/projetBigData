import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "BEGIN BATCH \n"
with open('SAT.csv') as tsvfile:
    reader = csv.reader(tsvfile)
    header = False
    counter = 0
    for row in reader:
        if not header:
            header = True
        else:
            state = row[0]
            score = row[1]
            sql_single = 'INSERT INTO sat(state,score) VALUES (' + '\''+state+'\'' + ','+str(score)+');\n'
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