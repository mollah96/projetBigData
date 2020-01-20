import csv
import re
from cassandra.cluster import Cluster 
from datetime import *

def convert24(str1):
    if(str1 is not ""):
        if str1[-2:] == "AM" and str1[:2] == "12":
            return "00" + str1[2:-2]

            # remove the AM
        elif str1[-2:] == "AM":
            return str1[:-2]

            # Checking if last two elements of time
        # is PM and first two elements are 12
        elif str1[-2:] == "PM" and str1[:2] == "12":
            return str1[:-2]

        else:

            # add 12 to hours and remove PM
            return str(int(str1[:2]) + 12) + str1[2:8]
    else:
        return ""
id = 0
cluster = Cluster(['172.17.0.2'])
session = cluster.connect('test')
sql = "BEGIN BATCH \n"
with open('gvdb-aggregated-db/Events.tsv') as tsvfile:
    reader = csv.reader(tsvfile, delimiter='\t')
    id_event = 0
    for row in reader:
        location = str(row[0])
        locations = location.split(',')
        if locations[-1] == " USA":
            id += 1
            add =False
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
                sql += 'INSERT INTO events_location(id,city,state) VALUES (' + '\''+str(id)+'\'' + ','+'\''+city.lstrip()+'\''+',' +'\''+ state.lstrip()[0:2]+'\''+');\n'
            if id % 100 == 0:
                sql += "APPLY BATCH;"
                session.execute(sql)
                sql = "BEGIN BATCH \n"

    if sql != "BEGIN BATCH \n":
        sql += 'APPLY BATCH;'
        ##print(sql)
        session.execute(sql)
    cluster.shutdown()
            
            
        
