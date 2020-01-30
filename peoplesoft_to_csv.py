# -*- coding: utf-8 -*-
"""
Created on Thursday Jan 30 02:05:16 2020
@author: Devesh Waingankar
Python Version: 3.6 or above
"""
import csv
import cx_Oracle

connection = cx_Oracle.connect(dsn=r"<DATABASE>", user=r"<USERNAME>", password=r"<PASSWORD>")
cursor = connection.cursor()

tablename='''<XYZ TABLE>'''
query='''Select * from ''' + tablename
r = cursor.execute(query)

csv_file = open(r"<PATH TO CREATE CSV FILE>\testfile.csv", "w")
writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)

for row in cursor:
    writer.writerow(row)

cursor.close()
connection.close()
csv_file.close()
