import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "BEGIN BATCH \n"
with open('law.csv') as tsvfile:
    reader = csv.reader(tsvfile)
    header = False
    counter = 0
    for row in reader:
        if not header:
            header = True
        else:
            state = row[2]
            year = row[1]
            law_rate = row[4]
            sql_single = 'INSERT INTO law(state,year,law_rate) VALUES (' + '\''+state+'\'' + ','+str(year)+',' + str(law_rate)+');\n'
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