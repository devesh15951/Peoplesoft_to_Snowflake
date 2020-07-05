#!/usr/bin/env python
# coding: utf-8
# Developer: Devesh Waingankar

# In[36]:


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


# In[37]:


Snow_Con = snowflake.connector.connect(user='USERNAME',password='PASSWORD',account='ACCT')

sc=Snow_Con.cursor()
sc.execute("USE WAREHOUSE DEMO_WH")
sc.execute("USE DATABASE S_CS")

connection = cx_Oracle.connect(dsn=r"DATABASE_NAME", user=r"USERNAME", password=r"PASSWORD")
cursor = connection.cursor()


# In[38]:


snow_count = '''select t.table_name as "table_name",
t.row_count as "rows"
from information_schema.tables t
where t.table_type = 'BASE TABLE'
order by t.row_count desc'''

df_snowflake_all_tables = pd.DataFrame(sc.execute(snow_count))
df_snowflake_all_tables.columns=["TABLE","SNOWFLAKE_ROWS"]
zero_row_count = df_snowflake_all_tables.loc[df_snowflake_all_tables['SNOWFLAKE_ROWS'] == 0]


# In[39]:


merge_tables_list = ['TABLE1','TABLE2']
df_snowflake_merge_tables = df_snowflake_all_tables.loc[df_snowflake_all_tables['TABLE'].isin(merge_tables_list)]


# In[40]:


first_col_table = []
second_col_row = []
try:
    for each in df_snowflake_merge_tables['TABLE']:
        count_query = '''Select count(*) from SPSOFT.''' + each
        table_count_cs = cursor.execute(count_query)
        z = int(str(cursor.fetchone()).split(',')[0][1:])
    #     print(type(table_count_dw))
        if table_count_cs:
            first_col_table.append(each)
            second_col_row.append(z)
except Exception as e:
    pass
cs_data = {'CS_TABLE':  first_col_table,'CS_ROWS': second_col_row}
df_cs_all_tables = pd.DataFrame (cs_data, columns = ['CS_TABLE','CS_ROWS'])


# In[41]:


horizontal_stack = pd.concat([df_snowflake_merge_tables, df_cs_all_tables], axis=1)
merged_inner = pd.merge(left=df_cs_all_tables, right=df_snowflake_merge_tables, left_on='CS_TABLE', right_on='TABLE')
merged_inner = merged_inner[['CS_TABLE','CS_ROWS','SNOWFLAKE_ROWS']]
merged_inner['Difference'] = merged_inner['CS_ROWS'].sub(merged_inner['SNOWFLAKE_ROWS'], axis = 0)


# In[42]:


recipients = ["example1@email.com","example2@email.com"] # list of email ids receiving the notification
emaillist = [elem.strip().split(',') for elem in recipients]
msg = MIMEMultipart()
msg['Subject'] = "Snowflake-Recon CS Tables- No HK"
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


# In[43]:


cursor.close()
connection.close()
Snow_Con.close()
server.quit()
