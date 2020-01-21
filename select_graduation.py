import csv
import re
from cassandra.cluster import Cluster 
from datetime import *



cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "select * from state;"

states = session.execute(sql)
event_csv = open('graduation_output.csv', 'w')
event_csv_writer = csv.writer(event_csv)
event_csv_writer.writerow(["state", "grad_rate"])
for state in states:
    state = state[0]
    sql = "select grad_rate from graduation where state = "+"\'"+state+" \';"
    #print(sql)
    res = session.execute(sql)
    counter = 0
    grad_rate = 0
    for r in res:
        grad_rate += r[0]
        counter +=1
    if counter != 0:
        grad_rate = grad_rate / counter
        event_csv_writer.writerow([state,grad_rate])


    
