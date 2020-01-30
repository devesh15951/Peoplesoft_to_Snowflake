# -*- coding: utf-8 -*-
"""
Created on Thursday Jan 30 02:05:16 2020
@author: Devesh Waingankar
Python Version: 3.6 or above
"""
import snowflake.connector

# Open connections.
Snow_Con = snowflake.connector.connect(user='<USERNAME>',password='<PASSWORD>',account='URL.us-east-1')

# update comments in snowflake.
sc=Snow_Con.cursor()
#replace DEMO_WH with your warehouse and DB_DEMO with your database name.
sc.execute("USE WAREHOUSE <DEMO_WH>")
sc.execute("USE DATABASE <DB_DEMO>")

#These are variables for different tables.
tablename = "<TABLENAME>"
filename = "<FILENAME>.csv"

cmd1='''create or replace stage <STAGING_AREA_NAME>
copy_options= (on_error='skip_file')
file_format= (type = 'CSV' field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"')'''
cmd2='''PUT file://C:\\Users\\drw16c\\''' + filename + '''@<STAGING_AREA_NAME> '''
cmd3='''copy into ''' + tablename + '''from <STAGING_AREA_NAME>'''

#executing commands on snowflake using snowflake cursor(sc).
rc1=sc.execute(cmd1)
rc2=sc.execute(cmd2)
rc3=sc.execute(cmd3)

# Close connections.

Snow_Con.close()
Oracle_Con.close()
