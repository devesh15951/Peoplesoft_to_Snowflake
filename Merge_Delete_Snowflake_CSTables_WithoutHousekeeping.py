#!/usr/bin/env python
# coding: utf-8
# Developer: Devesh Waingankar

# In[59]:


import snowflake.connector
import csv
import cx_Oracle
from functools import reduce
import operator
import time
import datetime
from datetime import date
import os
import re
import logging
import smtplib


# In[60]:


def csv_staging():
    # Open connections.
    Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')

    sc=Snow_Con.cursor()
    sc.execute("USE WAREHOUSE DEMO_WH")
    sc.execute("USE DATABASE S_CS")
    #os.system("exit")

    #tablename = ('''PS_ADDRESSESS''','''PS_CAMPUS_TBL''')
    staging_area = '''staging_area_CS'''
    path="""//apps//python//data//snowflake_merge//S_CS_NoHousekeeping//"""

    cmd1='''create or replace stage ''' + staging_area + '''
    copy_options= (on_error='skip_file')
    file_format= (type = 'CSV' field_delimiter = '0x009' FIELD_OPTIONALLY_ENCLOSED_BY='"' encoding='ISO2022CN')'''
    # sc.execute(cmd1)


    for root, dirs, files in os.walk(path):
        for f in files: # all files in the path
            put_cmd='''PUT file:/''' + path + f + ''' @''' + staging_area +''' auto_compress=true overwrite=true'''
            sc.execute(put_cmd)
            print("Moved "+f+" to Snowflake Staging Area")
            csv_out = '''Moved ''' + f +''' to Snowflake Staging Area'''

    # Close connections.
    Snow_Con.close()


# In[61]:


def merge():
    Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')
    sc=Snow_Con.cursor()
    sc.execute("USE WAREHOUSE DEMO_WH")
    sc.execute("USE DATABASE S_CS")

    connection = cx_Oracle.connect(dsn=r"DATABASE_NAME", user=r"USERNAME", password=r"PASSWORD")
    cursor = connection.cursor()
    table_list=[]

    for root, dirs, files in os.walk("/apps/python/data/snowflake_merge/S_CS_NoHousekeeping/"):
        for f in files: # all files in the path
            table_list.append(f[:-11])
    tablename = list(dict.fromkeys(table_list)) #remove duplicates
    # tablename=['PS_SCC_AFL_PERSON']

    staging_area = "staging_area_CS"

    get_ipython().system('echo \'Merged-\' >> "/apps/python/data/snowflake_logs/S_CS_NoHK_load_$(date +%F)"')
    #try:
    for i in range(len(tablename)):
#         print(tablename[i],"\n")
        lit = []
        lit_before = []
        qu=" "  #query
        qu2=" " #query2
        qu3=" " #query3
        all_col= [] #all fields from the table
        all_col_file= " " #all fields from the file
        col_list_del= " " #all key fields to delete from file
        col_list_del2= " " #all key fields to delete from file
        col_list_del3= " " #all key fields to delete from file
        col_list_del4= " " #all key fields to delete from file

        query_key = """select a.column_name from all_ind_columns a, all_indexes b where a.index_name=b.index_name
        and a.table_name ='""" +tablename[i]+ """' and b.uniqueness='UNIQUE' order by a.table_name, a.index_name, a.column_position"""

        cursor.execute(query_key)
        result = cursor.fetchall()
        lit=list(reduce(operator.concat,result))

    #     for each in lit_before
    #         if re.search("SYS_NC0",each):
    #             pass #SYS_NC00029$
    #         else:
    #             lit.append(each)

        unique_list=[]
        for each in lit:
            if each not in unique_list:
                unique_list.append(each)
        lit = unique_list
#         print("Keys are " + lit)

        print("merging " + tablename[i])
        merge_out = '''\\--''' + tablename[i] + '''--\\'''
        get_ipython().system('echo {merge_out} >> "/apps/python/data/snowflake_logs/S_CS_NoHK_load_$(date +%F)"')
        for j in range(0,len(lit)):
            if (j != len(lit)-1):
                qu = qu + "t1." + lit[j] + "= t2.$"+str(j+1)+" AND "
                col_list_del = col_list_del + lit[j] + ","
                col_list_del2 = col_list_del2 + "t2.$" + str(j+1) + ","
                col_list_del3 = col_list_del3 + "t1." + lit[j] + "= t2.$"+str(j+1)+" AND "
                col_list_del4 = col_list_del4 + "t2.$" + str(j+1) + " is null AND "
                all_col_file= all_col_file + "t2.$"+ str(j+1)+", "
            else:
                qu = qu + "t1." + lit[j] + "= t2.$"+str(j+1)
                col_list_del = col_list_del + lit[j]
                col_list_del2 = col_list_del2 + "t2.$"  + str(j+1)
                col_list_del3 = col_list_del3 + "t1." + lit[j] + "= t2.$"+str(j+1)
                col_list_del4 = col_list_del4 + "t2.$" + str(j+1) + " is null"
                all_col_file= all_col_file + "t2.$"+ str(j+1)



        query_nonkey = """select column_name from all_tab_columns where table_name ='""" +tablename[i]+ """' and owner='SPSOFT'
        and column_name not in(select a.column_name from all_ind_columns a, all_indexes b where a.index_name=b.index_name
        and a.table_name ='""" +tablename[i]+ """' and b.uniqueness='UNIQUE'  and a.index_owner='SPSOFT') order by column_id """

        cursor.execute(query_nonkey)
        result2 = cursor.fetchall()
        lit2=list(reduce(operator.concat,result2))
        all_col = lit + lit2
        all_col = ', '.join(all_col)
        all_col_file= all_col_file + ", "

        for k in range(0,len(lit2)):
            if (k != len(lit2)-1):
                qu2 = qu2 + lit2[k] + " <> t2.$"+str(k+1+len(lit))+" or "
                qu3 = qu3 + lit2[k] + "= t2.$"+str(k+1+len(lit))+" , "
                all_col_file= all_col_file + "t2.$"+ str(k+1+len(lit)) + ", "
            else:
                qu2 = qu2 + lit2[k] + "<> t2.$"+str(k+1+len(lit))
                qu3 = qu3 + lit2[k] + "= t2.$"+str(k+1+len(lit))
                all_col_file= all_col_file + "t2.$"+ str(k+1+len(lit))

        session_query='''alter session set ERROR_ON_NONDETERMINISTIC_MERGE = FALSE'''
        sc.execute(session_query)
        merge_query = '''MERGE INTO ''' +tablename[i]+ ''' t1 USING @''' +staging_area+ '''/''' +tablename[i]+ '''_Update.csv.gz t2 ON ''' + qu + '''
        WHEN MATCHED AND (''' + qu2 + ''') THEN UPDATE SET ''' + qu3 + ''' WHEN NOT MATCHED THEN INSERT ( ''' + all_col + ''') VALUES(''' + all_col_file + ''')'''
    #     print(merge_query,"\n")
        sc.execute(merge_query)
        delete_query='''Delete from ''' +tablename[i]+ ''' where (''' + col_list_del + ''') in (Select ''' + col_list_del2 + '''
         from @''' +staging_area+ '''/''' +tablename[i]+'''_Delete.csv.gz t2 left outer join '''  + tablename[i] +''' t1 ON ''' + col_list_del3 + ''')'''
    #     print (delete_query,"\n")
        sc.execute(delete_query)

        print("\n")
        qu=" "
        qu2=" "
        qu3=" "
        all_col_file=" "
        col_list_del=" "
        col_list_del2=" "
        col_list_del3=" "
        col_list_del4=" "

    cursor.close()
    connection.close()
    Snow_Con.close()

    print("Execution Time: %s seconds" % (time.time() - start_time))


# In[62]:


if __name__ == "__main__":
    try:
        start_time = time.time()
        todays_dt=str(date.today())
        os.system('rm -rf /apps/python/data/snowflake_logs/S_CS_NoHK_load_' + todays_dt)
        csv_staging()
        merge()
        os.system('rm -rf /apps/python/data/snowflake_merge/S_CS_NoHousekeeping/*')
        execution_time = str(time.time() - start_time)[0:4]
#         print("Total Execution Time: %s seconds" % (time.time() - start_time))


        log_path = """/apps/python/data/snowflake_logs/"""
        temp_list = []
        log_dir = log_path + 'S_CS_NoHK_load_' + todays_dt
        log_text= ""
        open_log_file = open(log_dir)
        for each in open_log_file:
            log_text+=each

        SERVER = ""
        FROM = ""
        TO = ["example1@email.com","example2@email.com"] #  list of email ids receiving the notification
        SUBJECT = "SUCCESS"
        TEXT = '''Success.
        Total Execution Time - ''' + execution_time + ''' seconds


        '''+log_text

#         Prepare actual message
        message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n
        %s
        """ % (FROM, ", ".join(TO), SUBJECT, TEXT)

#         Send the mail
        server = smtplib.SMTP(SERVER)
        server.sendmail(FROM, TO, message)
        server.quit()

    except Exception as e:
        SERVER = ""
        FROM = ""
        TO = ["example1@email.com","example2@email.com"] # list of email ids receiving the notification
        SUBJECT = "Merge CS_NoHousekeeping status"
#         SUBJECT = "CAS TESTING"
        TEXT = """Error :
        """ + e

#         Prepare actual message
        message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n
        %s
        """ % (FROM, ", ".join(TO), SUBJECT, TEXT)

#         Send the mail
        server = smtplib.SMTP(SERVER)
        server.sendmail(FROM, TO, message)
        server.quit()
