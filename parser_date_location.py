import csv
import re
from cassandra.cluster import Cluster 
from datetime import *

counter = 0
#cluster = Cluster(['172.17.0.2'])
cluster = Cluster(['ec2-52-47-198-123.eu-west-3.compute.amazonaws.com:9042'])
session = cluster.connect('test')
sql = "BEGIN BATCH \n"
with open('Events.tsv') as tsvfile:
    reader = csv.reader(tsvfile, delimiter='\t')
    id_event = 0
    add = False
    for row in reader:
        location = str(row[0])
        locations = location.split(',')
        if locations[-1] == " USA":
            id_event += 1
            counter += 1
            line = str(row[1])
            time = line.split(' ')
            date = time[0]
            if len(locations) == 3:
                city = locations[0]
                state = locations[1]
                add = True
                '''
                print("City",locations[0])
                print("State",locations[1])
                print("Country",locations[2])
                '''
            elif len(locations) == 4:
                city = locations[1]
                state = locations[2]
                add = True
                '''
                print("Address",locations[0])
                print("City",locations[1])
                print("State",locations[2])
                print("Country",locations[3])
                '''
            elif len(locations) == 5:
                city = locations[2]
                state = locations[3]
                add = True
                '''
                print("Address",locations[0],location[1])
                print("City",locations[2])
                print("State",locations[3])
                print("Country",locations[4])
                '''
            else:
                print("Unknown")
                print(locations)
                print("------------------")

        if add:
                sql_single = 'INSERT INTO events(id,date,city,state) VALUES (' + '\''+str(id_event)+'\'' + ','+'\''+date+'\''+',' +'\''+city.lstrip()+'\''+',' +'\''+ state.lstrip()[0:2]+'\''+');\n'
                print(sql_single)
                sql += sql_single
        if counter % 100 == 0:
                sql += "APPLY BATCH;"
                session.execute(sql)
                sql = "BEGIN BATCH \n"

    print(counter)
    cluster.shutdown()