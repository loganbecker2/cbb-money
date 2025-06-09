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
