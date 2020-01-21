import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "select * from state;"

states = session.execute(sql)
event_csv = open('event_output.csv', 'w')
event_csv_writer = csv.writer(event_csv)
event_csv_writer.writerow(["state", "event_count"])
for state in states:
    shorthand = state[1]
    sql = "select count(*) from events where state = "+"\'"+shorthand+"\';"
    count = session.execute(sql)
    count = count[0][0]
    print(shorthand,count)
    event_csv_writer.writerow([shorthand,count])


    
