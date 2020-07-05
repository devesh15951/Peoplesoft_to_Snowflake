#!/usr/bin/env python
# coding: utf-8
# Developer: Devesh Waingankar

# In[1]:


import snowflake.connector
import csv
import cx_Oracle
from functools import reduce
import operator
import time
import datetime
from datetime import date
import os
import sys
import re
import logging
import smtplib


# In[ ]:


def csv_staging(table_parameter):
    # Open connections.
    Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')

    sc=Snow_Con.cursor()
    sc.execute("USE WAREHOUSE DEMO_WH")
    sc.execute("USE DATABASE S_CS")

    tablename = table_parameter
    staging_area = '''staging_area_CS'''
    upd_filename="""//apps//python//data//snowflake_merge//S_CS//""" + tablename + """_Update.csv"""
    del_filename="""//apps//python//data//snowflake_merge//S_CS//""" + tablename + """_Delete.csv"""
    put_upd_cmd='''PUT file:/''' + upd_filename + ''' @''' + staging_area +''' auto_compress=true overwrite=true'''
    put_del_cmd='''PUT file:/''' + del_filename + ''' @''' + staging_area +''' auto_compress=true overwrite=true'''
    sc.execute(put_upd_cmd)
    sc.execute(put_del_cmd)
    print("Moved "+tablename+" to Snowflake Staging Area")

    # Close connections.
    Snow_Con.close()


# In[ ]:


def merge(table_parameter):
    Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')
    sc=Snow_Con.cursor()
    sc.execute("USE WAREHOUSE DEMO_WH")
    sc.execute("USE DATABASE S_CS")

    connection = cx_Oracle.connect(dsn=r"DATABASE_NAME", user=r"USERNAME", password=r"PASSWORD")
    cursor = connection.cursor()
    tablename = []
    t = table_parameter.strip()

    tablename+= [t]
#     tablename = ['STDNT_TEST_COMP']

    staging_area = "staging_area_CS"

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

        query_key = """select a.column_name from all_ind_columns a, all_indexes b where a.index_name=b.index_name and
        a.index_owner='SPSDW' and a.table_name ='""" +tablename[i]+ """' and b.uniqueness='UNIQUE' order by a.table_name,
        a.index_name, a.column_position"""

        cursor.execute(query_key)
        result = cursor.fetchall()
        lit=list(reduce(operator.concat,result))
    #     lit_before=list(reduce(operator.concat,result))
    #     for each in lit_before:
    #         if re.search("SYS_NC0",each):
    #             pass #SYS_NC00029$
    #         else:
    #             lit.append(each)

        unique_list=[]
        for each in lit:
            if each not in unique_list:
                unique_list.append(each)
        lit = unique_list
    #     print("Keys are :" + str(lit))
    #     for each in lit:
    #         print(each + " ")
        print("merging " + tablename[i])

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



        query_nonkey = """select column_name from all_tab_columns where table_name ='""" +tablename[i]+ """'and owner='SPSDW'
        and column_name not in(select a.column_name from all_ind_columns a, all_indexes b where a.index_name=b.index_name
        and a.table_name ='""" +tablename[i]+ """' and b.uniqueness='UNIQUE') order by column_id """

        cursor.execute(query_nonkey)
        result2 = cursor.fetchall()
        lit2=list(reduce(operator.concat,result2))
        unique_list=[]
        for each in lit2:
            if each not in unique_list:
                unique_list.append(each)
        lit2 = unique_list

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
    #     error if row is duplicate
        session_query='''alter session set ERROR_ON_NONDETERMINISTIC_MERGE = FALSE'''
        sc.execute(session_query)

        merge_query = '''MERGE INTO ''' +tablename[i]+ ''' t1 USING @''' +staging_area+ '''/''' +tablename[i]+ '''_Update.csv.gz t2
        ON ''' + qu + ''' WHEN MATCHED AND (''' + qu2 + ''') THEN UPDATE SET ''' + qu3 + ''' WHEN NOT MATCHED
        THEN INSERT ( ''' + all_col + ''') VALUES(''' + all_col_file + ''')'''

    #     print(merge_query,"\n")
        sc.execute(merge_query)

        delete_query='''Delete from ''' +tablename[i]+ ''' where (''' + col_list_del + ''') in (Select
         ''' + col_list_del2 + ''' from @''' +staging_area+ '''/''' +tablename[i]+'''_Delete.csv.gz t2 )'''

    #     print (delete_query,"\n")
        sc.execute(delete_query)
        current_time=str(datetime.datetime.today())[0:19]

        alter_cmd ='''ALTER TABLE ''' + tablename[i] + ''' SET COMMENT = 'Updated at ''' + current_time +'''.' '''
        sc.execute(alter_cmd)
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


# In[ ]:


if __name__ == "__main__":
    try:
        start_time = time.time()
        table_parameter = str(sys.argv).split(',')[-1][:-1].strip()[1:-1]
        csv_staging(table_parameter)
        merge(table_parameter)
        execution_time = str(time.time() - start_time)[0:4]
        todays_dt=str(date.today())


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
