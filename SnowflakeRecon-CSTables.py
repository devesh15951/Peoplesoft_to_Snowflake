#!/usr/bin/env python
# coding: utf-8
# Developer: Devesh Waingankar

# In[1]:


import snowflake.connector
import cx_Oracle
import time
import datetime
from datetime import date
import os
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP


# In[2]:


Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')

sc=Snow_Con.cursor()
sc.execute("USE WAREHOUSE DEMO_WH")
sc.execute("USE DATABASE S_CS")

connection = cx_Oracle.connect(dsn=r"DATABASE_NAME", user=r"USERNAME", password=r"PASSWORD")
cursor = connection.cursor()


# In[3]:


snow_count = '''select t.table_name as "table_name",
t.row_count as "rows"
from information_schema.tables t
where t.table_type = 'BASE TABLE'
order by t.row_count desc'''

df_snowflake_all_tables = pd.DataFrame(sc.execute(snow_count))
df_snowflake_all_tables.columns=["TABLE","SNOWFLAKE_ROWS"]
zero_row_count = df_snowflake_all_tables.loc[df_snowflake_all_tables['SNOWFLAKE_ROWS'] == 0]


# In[4]:


merge_tables_list = ['TABLE1','TABLE2']
df_snowflake_merge_tables = df_snowflake_all_tables.loc[df_snowflake_all_tables['TABLE'].isin(merge_tables_list)]


# In[5]:


first_col_table = []
second_col_row = []
try:
    for each in df_snowflake_merge_tables['TABLE']:
        count_query = '''Select count(*) from SPSDW.''' + each + ''' where DATA_ORIGIN <> 'D' and SRC_SYS_ID = 'CS' '''
        table_count_dw = cursor.execute(count_query)
        z = int(str(cursor.fetchone()).split(',')[0][1:])
    #     print(type(table_count_dw))
        first_col_table.append(each)
        second_col_row.append(z)
except Exception as e:
    pass
dw_data = {'DW_TABLE':  first_col_table,'DW_ROWS': second_col_row}
df_dw_all_tables = pd.DataFrame (dw_data, columns = ['DW_TABLE','DW_ROWS'])


# In[6]:


horizontal_stack = pd.concat([df_snowflake_merge_tables, df_dw_all_tables], axis=1)
merged_inner = pd.merge(left=df_dw_all_tables, right=df_snowflake_merge_tables, left_on='DW_TABLE', right_on='TABLE')
merged_inner = merged_inner[['DW_TABLE','DW_ROWS','SNOWFLAKE_ROWS']]
merged_inner['Difference'] = merged_inner['DW_ROWS'].sub(merged_inner['SNOWFLAKE_ROWS'], axis = 0)


# In[7]:


recipients = ["example1@email.com","example2@email.com"] #  list of email ids receiving the notification
emaillist = [elem.strip().split(',') for elem in recipients]
msg = MIMEMultipart()
msg['Subject'] = "Snowflake-Recon CS Tables"
msg['From'] = "EMAIL"

html = """<html>
  <head></head>
  <body>
    {0}
  </body>
</html>
""".format(merged_inner.to_html())

html2 = """<html>
  <head></head>
  <body>
    {0}
  </body>
</html>
""".format(zero_row_count.to_html())

part1 = MIMEText(html, 'html')
part2 = MIMEText(html2, 'html')
msg.attach(part1)
msg.attach(part2)

server = smtplib.SMTP('SERVER')
server.sendmail(msg['From'],recipients, msg.as_string())


# In[8]:


cursor.close()
connection.close()
Snow_Con.close()
server.quit()
