# -*- coding: utf-8 -*-
"""
Created on Mon Jun  9 12:48:51 2025

@author: Logmo
"""

#%% connect to mysql db
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

# Connect to the MySQL database
conn = MySQLdb.connect(
    host="localhost",
    user="logmo",
    password="Logmonster02!",
    database="cbb_data"
)

# Create a cursor
cursor = conn.cursor()

# Test a simple query
cursor.execute("SELECT DATABASE()")
current_db = cursor.fetchone()
print("Connected to database:", current_db)

# Clean up
cursor.close()
conn.close()


#%% save into mysql database
def toSQL(df, databaseName):
    engine = create_engine('mysql+mysqlconnector://logmo:Logmonster02!@127.0.0.1/cbb_data')
    return df.to_sql(name=databaseName, con=engine, if_exists='append', index=False)