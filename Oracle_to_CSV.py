# -*- coding: utf-8 -*-
"""
Created on Thursday Jan 30 15:10:09 2020
@author: Devesh Waingankar
Python Version: 3.6 or above
"""
import csv
import cx_Oracle

connection = cx_Oracle.connect(dsn=r"<CONNECTION_NAME>", user=r"<USERNAME>", password=r"<PASSWORD>")
cursor = connection.cursor()

schema="<SCHEM_ANAME>."
tablename=("TABLE1","TABLE2")

for i in range (0,len(tablename)):
    query="Select * from "  + schema + tablename[i]
    csvfile=tablename[i]
    r = cursor.execute(query)
    path=r"C:\\Users\\path\\Desktop\\"
    csv_link= path + csvfile + ".csv"

    csv_file = open(csv_link, "w")
    writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)

    for row in cursor:
        writer.writerow(row)

    csv_file.close()

cursor.close()
connection.close()
