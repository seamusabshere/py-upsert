import unittest
import sys
import os

import psycopg2
import MySQLdb
import sqlite3

from upsert import Upsert

class OneByOne:
    def test_insert_1_0(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'})
        self.execute_sql("select name from pets")
        self.assertEqual('Jerry', self.cursor.fetchone()[0])

    def test_insert_1_1(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'}, {'color': 'brown1'})
        self.execute_sql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown1', res[1])

    def test_insert_2_0(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'brown2'})
        self.execute_sql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown2', res[1])

    def test_insert_1_2(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'}, {'color': 'brown3', 'weight': 256.78})
        self.execute_sql("select name, color, weight from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown3', res[1])
        self.assertEqual(256.78, res[2])

    def test_insert_2_2(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'license': 888}, {'color': 'brown4', 'weight': 256.78})
        self.execute_sql("select name, color, weight, license from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown4', res[1])
        self.assertEqual(256.78, res[2])
        self.assertEqual(888, res[3])

    # so basically a NOOP
    def test_update_1_0(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name) VALUES (%s)', ('Jerry',))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'})
        self.execute_sql("select name from pets")
        self.assertEqual('Jerry', self.cursor.fetchone()[0])

    def test_update_1_1(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name, color) VALUES (%s, %s)', ('Jerry', 'red'))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'}, {'color': 'brown5'})
        self.execute_sql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown5', res[1])

    def test_update_2_0_hit(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name, color) VALUES (%s, %s)', ('Jerry', 'red'))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'red'})
        self.execute_sql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])

    def test_update_1_2(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name, color, weight) VALUES (%s, %s, %s)', ('Jerry', 'red', 123.456))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'}, {'color': 'brown6', 'weight': 256.78})
        self.execute_sql("select name, color, weight from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown6', res[1])
        self.assertEqual(256.78, res[2])

    def test_update_2_0_miss(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name, color) VALUES (%s, %s)', ('Jerry', 'red'))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'brown7'})
        self.execute_sql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])

    def test_update_2_1_hit(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name, color, weight) VALUES (%s, %s, %s)', ('Jerry', 'red', 123.456))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'red'}, {'weight': 256.78})
        self.execute_sql("select name, color, weight from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])
        self.assertEqual(256.78, res[2])

    def test_update_2_1_selfmod(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name, color) VALUES (%s, %s)', ('Jerry', 'red'))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'red'}, {'color': 'brown8'})
        self.execute_sql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown8', res[1])

    def test_update_2_2_hit(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name, color, weight, license) VALUES (%s, %s, %s, %s)', ('Jerry', 'red', 123.456, 555))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'red'}, {'weight': 256.78, 'license': 888})
        self.execute_sql("select name, color, weight, license from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])
        self.assertEqual(256.78, res[2]) 
        self.assertEqual(888, res[3])

    def test_update_2_2_miss(self):
        #print(sys._getframe(0).f_code.co_name)
        self.execute_sql('INSERT INTO pets (name, color, weight, license) VALUES (%s, %s, %s, %s)', ('Jerry', 'red', 123.456, 555))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'brown9'}, {'weight': 256.78, 'license': 888})
        self.execute_sql("select name, color, weight, license from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])
        self.assertEqual(123.456, res[2]) 
        self.assertEqual(555, res[3])

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = self.get_connection()
        self.cursor = self.connection.cursor()
        self.create_table('pets')

    def tearDown(self):
        self.cursor.close()
        self.connection.close()

    def create_table(self, t):
        self.execute_sql("""
            DROP TABLE IF EXISTS %s;
        """ % t)
        self.execute_sql("""
            CREATE TABLE %s (
                "name" CHARACTER VARYING(255) PRIMARY KEY,
                color CHARACTER VARYING(255) UNIQUE,
                license INTEGER,
                weight FLOAT
            );
        """ % t)

class TestUpsertSqlite(MyTestCase, OneByOne):
    def get_connection(self):
        return sqlite3.connect(':memory:')

    def execute_sql(self, template, values = ()):
        template = template.replace('%s', '?')
        self.cursor.execute(template, values)

class TestUpsertMysql(MyTestCase, OneByOne):
    def get_connection(self):
        return MySQLdb.connect(user="root",passwd="password",db="test_py_upsert")

    def execute_sql(self, template, values = ()):
        self.cursor.execute(template, values)

class TestUpsertPostgresql(MyTestCase, OneByOne):
    def get_connection(self):
        return psycopg2.connect('dbname=test_py_upsert')

    def execute_sql(self, template, values = ()):
        self.cursor.execute(template, values)
