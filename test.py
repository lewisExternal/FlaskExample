import sqlite3
from flask import Flask
from flask import request, jsonify
import pandas as np 
import db_lib
import unittest

database = r"pythonsqlite.db"
conn = db_lib.create_connection(database)
rows = db_lib.sql_report(conn,'2019-09-22')

class TestEndpoint(unittest.TestCase):

    def test_items(self):
        self.assertEqual(rows[1],1524)
    
    def test_customers(self):
        self.assertEqual(rows[2],5)

if __name__ == '__main__':
    unittest.main()
