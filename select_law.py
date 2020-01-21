import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "select * from state;"

states = session.execute(sql)

event_csv = open('law_output.csv', 'w')
event_csv_writer = csv.writer(event_csv)
event_csv_writer.writerow(["state", "law_score"])

for state in states:
    shorthand = state[0]
    sql = "select * from law where state = "+"\'"+shorthand+"\';"
    res = session.execute(sql)
    score = 0
    c = 0
    for year in res:
        score += year[2]
        c += 1
    if c!=0:
        score = score / c
        event_csv_writer.writerow([shorthand,score])



    
