import csv
import re

from datetime import *

# event_csv_file = open("EventCSV.csv", "w")
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


with open('gvdb-aggregated-db/Events.tsv') as tsvfile:
    reader = csv.reader(tsvfile, delimiter='\t')
    id_event = 0

    event_csv = open('eventCSV.csv', 'w', newline='')
    event_csv_writer = csv.writer(event_csv)
    event_csv_writer.writerow(["id_event", "adsress", "date"])

    details_csv = open('details.csv', 'w', newline='')
    details_csv_writer = csv.writer(details_csv)
    details_csv_writer.writerow(
        ["id_event", "clock_time", "details", "number_of_shots_fired", "time_day", "type_of_gun"])

    participants_csv = open('participants.csv', 'w', newline='')
    participants_csv_writer = csv.writer(participants_csv)
    participants_csv_writer.writerow(["id_event", "name", "is_victim", "gender", "race", "hospitalized", "killed"])

    for row in reader:
        event_csv_writer.writerow([id_event, row[0], row[1]])

        # print(row[2])
        regexDetails = re.compile(r".*"
                                  r"\"value\":\"(?P<clock_time>.*)\""
                                  r"\}.*"
                                  r"\"value\":\"(?P<details>.*)\""
                                  r"\}.*"
                                  r"\"value\":\"(?P<number_of_shots_fired>.*)\""
                                  r"\}.*"
                                  r"\"value\":\"(?P<time_day>.*)\""
                                  r"\}.*"
                                  r"\"value\":\"(?P<type_of_gun>.*)\".*")
        resultDetails = regexDetails.search(row[2])
        if resultDetails is not None:
            clock_time = resultDetails.group("clock_time").replace("a.m.","AM").replace("p.m.","PM").replace("p.m","PM").replace("\'","")
            details = resultDetails.group("details")
            number_of_shots_fired = resultDetails.group("number_of_shots_fired")
            time_day = resultDetails.group("time_day")
            type_of_gun = resultDetails.group("type_of_gun")
            print(clock_time+"--> ")
            print(convert24(clock_time))

            """
        print("clock-time: " + clock_time)
        print("details: " + details)
        print("number_of_shots_fired: " + number_of_shots_fired)
        print("time_day :" + time_day)
        print("type_of_gun :"+ type_of_gun )
        """

            details_csv_writer.writerow([id_event, clock_time, details, number_of_shots_fired, time_day, type_of_gun])

        # participants_csv_writer.writerow([])
        regexParticipants = re.compile(r".*u'injured': (?P<injured>\w*), "
                                       r"u'name': u(?P<name>.*), "
                                       r"u'hospitalized': (?P<hospitalized>\w*), "
                                       r"u'gender': u'(?P<gender>\w*)', "
                                       r"u'age': u'(?P<age>\w*)', "
                                       r"u'race': u'(?P<race>\w*)', "
                                       r"u'killed': (?P<killed>\w*), "
                                       r"u'is_victim': (?P<is_victim>\w*).*"
                                       )

        #print("row :" + row[3])
        for participant in row[3].split("}, {"):
            print("part :" + participant)

            resultParticipants = regexParticipants.search(participant)
            # rint(row[3])
            # resultParticipants2 = regexParticipants2.search(row[3])
            if resultParticipants is not None:
                injured = resultParticipants.group("injured")
                name = resultParticipants.group("name").replace("\"", "").replace("\'", "")
                hospitalized = resultParticipants.group("hospitalized")
                gender = resultParticipants.group("gender")
                age = resultParticipants.group("age")
                race = resultParticipants.group("race")
                killed = resultParticipants.group("killed")
                is_victim = resultParticipants.group("is_victim")

                '''print("injured: " + injured)
                print("name : " + name)
                print("hospitalized: " + hospitalized)
                print("gender :" + gender)
                print("age :" + age)
                print("race: " + race)
                print("killed: " + killed)
                print("is_victim: " + is_victim)
'''
                participants_csv_writer.writerow(
                    [id_event, is_victim, name, gender, race, hospitalized, killed, injured])
            '''
            elif resultParticipants2 is not None:
                injured = resultParticipants2.group("injured")
                name = resultParticipants2.group("name")
                hospitalized = resultParticipants2.group("hospitalized")
                gender = resultParticipants2.group("gender")
                age = resultParticipants2.group("age")
                race = resultParticipants2.group("race")
                killed = resultParticipants2.group("killed")
                is_victim = resultParticipants2.group("is_victim")

                print("injured: " + injured)
                print("name : " + name)
                print("hospitalized: " + hospitalized)
                print("gender :" + gender)
                print("age :" + age)

                participants_csv_writer.writerow(
                    [id_event, name, is_victim,gender, race, hospitalized, killed, injured])'''

        id_event += 1

print("parsing ok!")
