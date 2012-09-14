import unittest
import psycopg2
import MySQLdb
import sqlite3

from upsert import *

def create_table(connection):
    cur = connection.cursor()
    cur.execute("""
    DROP TABLE IF EXISTS pets;
""")
    cur.execute("""
    CREATE TABLE pets ( "name" CHARACTER VARYING(255) PRIMARY KEY, color CHARACTER VARYING(255), license INTEGER, weight FLOAT );
""")

class Single:
    def test_single(self):
        upsert = Upsert(self.connection, 'pets')
        upsert.row({'name': 'Jerry'})

        cur = self.connection.cursor()
        cur.execute("select name from pets;")
        self.assertEqual('Jerry', cur.fetchone()[0])


class TestUpsertPostgresql(unittest.TestCase, Single):
    def setUp(self):
        self.connection = psycopg2.connect('dbname=test_py_upsert')
        create_table(self.connection)

    def tearDown(self):
        self.connection.close()

class TestUpsertMysql(unittest.TestCase, Single):
    def setUp(self):
        self.connection = MySQLdb.connect(user="root",passwd="password",db="test_py_upsert")
        create_table(self.connection)

    def tearDown(self):
        self.connection.close()

class TestUpsertSqlite(unittest.TestCase, Single):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        create_table(self.connection)

    def tearDown(self):
        self.connection.close()

# class TestUpsertMysql()

# class TestUpsertMysql(unittest.TestCase):
#     def setUp(self):
#         self.connection = MySQLdb.connect(user="root",passwd="password",db="test_py_upsert")
#         cur = self.connection.cursor()
#         cur.execute("""
#         DROP TABLE IF EXISTS pets;
# """)
#         cur.execute("""
#         CREATE TABLE pets ( "name" CHARACTER VARYING(255) PRIMARY KEY, color CHARACTER VARYING(255), license INTEGER, weight FLOAT );
# """)

#     def tearDown(self):
#         self.connection.close()

#     def test_single(self):
#         upsert = Upsert(self.connection, 'pets')
#         upsert.row({'name': 'Pierre'})

#         cur = self.connection.cursor()
#         cur.execute("select name from pets;")
#         self.assertEqual('Pierre', cur.fetchone()[0])

# class TestUpsertMysql(unittest.TestCase):
#     def setUp(self):
#         self.connection = MySQLdb.connect(user="root",passwd="password",db="test_py_upsert")
#         cur = self.connection.cursor()
#         cur.execute("""
#         DROP TABLE IF EXISTS pets;
# """)
#         cur.execute("""
#         CREATE TABLE pets ( "name" CHARACTER VARYING(255) PRIMARY KEY, color CHARACTER VARYING(255), license INTEGER, weight FLOAT );
# """)

#     def tearDown(self):
#         self.connection.close()

#     def test_single(self):
#         upsert = Upsert(self.connection, 'pets')
#         upsert.row({'name': 'Pierre'})

#         cur = self.connection.cursor()
#         cur.execute("select name from pets;")
#         self.assertEqual('Pierre', cur.fetchone()[0])
