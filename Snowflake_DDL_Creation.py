#!/usr/bin/env python
# coding: utf-8
# Developer: Devesh Waingankar

# In[5]:


import snowflake.connector
import csv
import cx_Oracle
import pandas as pd
from functools import reduce
import operator
import os


# In[25]:


Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')
sc=Snow_Con.cursor()
sc.execute("USE WAREHOUSE DEMO_WH")
sc.execute("USE DATABASE S_DW")

connection = cx_Oracle.connect(dsn=r"DATABASE_NAME", user=r"USERNAME", password=r"PASSWORD")
cursor = connection.cursor()


# In[16]:


table_list=[]
staging_area = "test_staging_area_new"
col_list = ""
for root, dirs, files in os.walk("/apps/python/data/snowflake_merge/S_DW"):
    for f in files: # all files in the path
        table_list.append(f[:-11])
tablename = list(dict.fromkeys(table_list)) #remove duplicates


# In[40]:


try:
    for i in range (0,len(tablename)):
        metadata_query = """SELECT  column_name, data_type FROM all_tab_columns where table_name = '""" + tablename[i]+   """' """
        cursor.execute(metadata_query)
        column_result = cursor.fetchall()
        total_columns = len(column_result) #8
        #print (result)
        l = 0
        for each in column_result:
            cola = list(each)
            if cola[1] == "VARCHAR2":
                cola[1] = 'VARCHAR (16777216)'
            each = tuple(cola)
            #print(each)
            l = l + 1
            if l != total_columns:
                col_list = col_list + each[0] + " " + each[1] + ","
            else:
                col_list = col_list + each[0] + " " + each[1]


        #print (col_list)
        create_query = """Create or replace TABLE """ + tablename[i]+ """ (""" + col_list + """)"""
#        print(create_query)
        snowflake_cmd = """select get_ddl('table','""" + tablename[i] + """')"""
#        print(snowflake_cmd)
        snowflake_ddl = sc.execute(snowflake_cmd).fetchall()
#        print(snowflake_ddl)
        print("\n")


        for each in snowflake_ddl:
            each_str = (str(each))
            each_str = each_str.replace("\\n\\t", "")
            each_str = each_str.replace("\\n", "")
            print(each_str)
            #re.search(r'\\n\\t', each_str).group(1).strip()





        col_list = ""

except Exception as e:
    print ("\n",e)


cursor.close()
connection.close()
Snow_Con.close()
