import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "select * from state;"

states = session.execute(sql)
event_csv = open('police_spending_output.csv', 'w')
event_csv_writer = csv.writer(event_csv)
event_csv_writer.writerow(["state", "police_spending","police_spending_rate"])
for state in states:
    shorthand = state[0]
    sql = "select police_spending,rate from police where state = "+"\'"+shorthand+"\';"
    count = session.execute(sql)
    try:
        police_spending= count[0][0]
        rate = count[0][1]
        #print(shorthand,count)
        event_csv_writer.writerow([shorthand,police_spending,rate])
    except:
        pass


    
