import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "select * from state;"

states = session.execute(sql)
event_csv = open('revenue_output.csv', 'w')
event_csv_writer = csv.writer(event_csv)
event_csv_writer.writerow(["state", "cost per student"])
for state in states:
    shorthand = state[0]
    sql = "select cost_per_pupil from revenue where state = "+"\'"+shorthand+"\';"
    count = session.execute(sql)
    print(count[0][0])
    event_csv_writer.writerow([shorthand,count[0][0]])


    
