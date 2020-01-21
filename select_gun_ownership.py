import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "select * from state;"

states = session.execute(sql)
event_csv = open('gun_ownership_output.csv', 'w')
event_csv_writer = csv.writer(event_csv)
event_csv_writer.writerow(["state", "gun_owner_rate"])
for state in states:
    shorthand = state[0]
    sql = "select gun_ownership from gun_ownership where state = "+"\'"+shorthand+"\';"
    count = session.execute(sql)
    count = count[0][0]
    print(shorthand,count)
    event_csv_writer.writerow([shorthand,count])


    
