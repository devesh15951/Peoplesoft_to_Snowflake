#!/usr/bin/env python
# coding: utf-8
# Developer: Devesh Waingankar

# In[147]:


import snowflake.connector
import csv
import cx_Oracle
from functools import reduce
import operator
import time
import os
import logging


# In[ ]:


def csv_staging():
    # Open connections.
    Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')

    sc=Snow_Con.cursor()
    sc.execute("USE WAREHOUSE DEMO_WH")
    sc.execute("USE DATABASE S_DW")
    #os.system("exit")

    #tablename = ('''PS_ADDRESSESS''','''PS_CAMPUS_TBL''')
    staging_area = '''staging_area_DW'''
    path="""//apps//python//data//snowflake_merge//S_DW//"""

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
            get_ipython().system('echo {csv_out} >> "/apps/python/data/snowflake_logs/S_DW_load_$(date +%F)"')

    # Close connections.
    Snow_Con.close()
    execution_time = str(time.time() - start_time)[0:4]
    print("Execution Time : "+execution_time)


# In[151]:


def merge():
    #executing oracle to csv to staging_area

    Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')
    sc=Snow_Con.cursor()
    sc.execute("USE WAREHOUSE DEMO_WH")
    sc.execute("USE DATABASE S_DW")

    connection = cx_Oracle.connect(dsn=r"DATABASE_NAME", user=r"USERNAME", password=r"PASSWORD")
    cursor = connection.cursor()

    table_list=[]

    for root, dirs, files in os.walk("/apps/python/data/snowflake_merge/S_DW/"):
        for f in files: # all files in the path
            table_list.append(f[:-11])
    tablename = list(dict.fromkeys(table_list)) #remove duplicates
#     tablename = ['PS_D_PERSON']

    merge_out = '''merging '''+tablename[i]
    get_ipython().system('echo {csv_out} >> "/apps/python/data/snowflake_logs/S_CS_NoHK_load_$(date +%F)"')
    staging_area = "staging_area_dw"
    #try:
    for i in range(len(tablename)):
#         print(tablename[i],"\n")
        lit = []
        lit_bef = []
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
        and a.table_name ='""" +tablename[i]+ """' and b.uniqueness='UNIQUE' and a.index_owner='SPSDW' order by a.table_name,
        a.index_name, a.column_position"""

        cursor.execute(query_key)
        result = cursor.fetchall()
        lit=list(reduce(operator.concat,result))
        unique_list=[]
        for each in lit:
            if each not in unique_list:
                unique_list.append(each)
        lit = unique_list
#         print("Keys are " + lit)

        print("Merging ",tablename[i])
        for j in range(0,len(lit)):
            if (j != len(lit)-1):
                qu = qu + "t1." + lit[j] + "= t2.$"+str(j+1)+" AND "
                col_list_del = col_list_del + lit[j] + ","
                col_list_del2 = col_list_del2 + "t2.$" + str(j+1)
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

        query_nonkey = """select column_name from all_tab_columns where table_name ='""" +tablename[i]+ """'
        and owner='SPSDW' and column_name not in(select a.column_name from all_ind_columns a, all_indexes b
        where a.index_name=b.index_name and a.table_name ='""" +tablename[i]+ """' and b.uniqueness='UNIQUE'
        and a.index_owner='SPSDW') order by column_id """

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

        merge_query = '''MERGE INTO ''' +tablename[i]+ ''' t1 USING @''' +staging_area+ '''/''' +tablename[i]+ '''_Update.csv.gz t2
        ON ''' + qu + ''' WHEN MATCHED AND (''' + qu2 + ''') THEN UPDATE SET ''' + qu3 + ''' WHEN NOT MATCHED
        THEN INSERT ( ''' + all_col + ''') VALUES(''' + all_col_file + ''')'''

        print(merge_query,"\n")
        sc.execute(merge_query)

        delete_query='''Delete from ''' +tablename[i]+ ''' where (''' + col_list_del + ''') in (Select ''' + col_list_del2 + ''' from
        @''' +staging_area+ '''/''' +tablename[i]+'''_Delete.csv.gz t2 left outer join '''  + tablename[i] +''' t1 ON ''' + col_list_del3 + ''')'''

        print (delete_query,"\n")
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


# In[149]:


if __name__ == "__main__":
    start_time = time.time()
    csv_staging()
    merge()
    print("Total Execution Time: %s seconds" % (time.time() - start_time))
