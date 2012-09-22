import unittest
import sys
import os

import psycopg2
import MySQLdb
import sqlite3

from upsert import Upsert

def create_table(cursor, t):
    cursor.execute("""
    DROP TABLE IF EXISTS %s;
""" % t)
    cursor.execute("""
    CREATE TABLE %s (
        "name" CHARACTER VARYING(255) PRIMARY KEY,
        color CHARACTER VARYING(255),
        license INTEGER,
        weight FLOAT
    );
""" % t)

class OneByOne:
    def test_insert_1_0(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'})
        self.executeSql("select name from pets")
        self.assertEqual('Jerry', self.cursor.fetchone()[0])

    def test_insert_1_1(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'}, {'color': 'brown1'})
        self.executeSql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown1', res[1])

    def test_insert_2_0(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'brown2'})
        self.executeSql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown2', res[1])

    def test_insert_1_2(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'}, {'color': 'brown3', 'weight': 256.78})
        self.executeSql("select name, color, weight from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown3', res[1])
        self.assertEqual(256.78, res[2])

    def test_insert_2_2(self):
        #print(sys._getframe(0).f_code.co_name)
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'license': 888}, {'color': 'brown4', 'weight': 256.78})
        self.executeSql("select name, color, weight, license from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown4', res[1])
        self.assertEqual(256.78, res[2])
        self.assertEqual(888, res[3])

    # so basically a NOOP
    def test_update_1_0(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name) VALUES (%s)', ('Jerry',))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'})
        self.executeSql("select name from pets")
        self.assertEqual('Jerry', self.cursor.fetchone()[0])

    def test_update_1_1(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name, color) VALUES (%s, %s)', ('Jerry', 'red'))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'}, {'color': 'brown5'})
        self.executeSql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown5', res[1])

    def test_update_2_0_hit(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name, color) VALUES (%s, %s)', ('Jerry', 'red'))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'red'})
        self.executeSql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])

    def test_update_1_2(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name, color, weight) VALUES (%s, %s, %s)', ('Jerry', 'red', 123.456))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry'}, {'color': 'brown6', 'weight': 256.78})
        self.executeSql("select name, color, weight from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown6', res[1])
        self.assertEqual(256.78, res[2])

    def test_update_2_0_miss(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name, color) VALUES (%s, %s)', ('Jerry', 'red'))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'brown7'})
        self.executeSql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])

    def test_update_2_1_hit(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name, color, weight) VALUES (%s, %s, %s)', ('Jerry', 'red', 123.456))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'red'}, {'weight': 256.78})
        self.executeSql("select name, color, weight from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])
        self.assertEqual(256.78, res[2])

    def test_update_2_1_selfmod(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name, color) VALUES (%s, %s)', ('Jerry', 'red'))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'red'}, {'color': 'brown8'})
        self.executeSql("select name, color from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('brown8', res[1])

    def test_update_2_2_hit(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name, color, weight, license) VALUES (%s, %s, %s, %s)', ('Jerry', 'red', 123.456, 555))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'red'}, {'weight': 256.78, 'license': 888})
        self.executeSql("select name, color, weight, license from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])
        self.assertEqual(256.78, res[2]) 
        self.assertEqual(888, res[3])

    def test_update_2_2_miss(self):
        #print(sys._getframe(0).f_code.co_name)
        self.executeSql('INSERT INTO pets (name, color, weight, license) VALUES (%s, %s, %s, %s)', ('Jerry', 'red', 123.456, 555))
        upsert = Upsert(self.cursor, 'pets')
        upsert.row({'name': 'Jerry', 'color': 'brown9'}, {'weight': 256.78, 'license': 888})
        self.executeSql("select name, color, weight, license from pets")
        res = self.cursor.fetchone()
        self.assertEqual('Jerry', res[0])
        self.assertEqual('red', res[1])
        self.assertEqual(123.456, res[2]) 
        self.assertEqual(555, res[3])

# class TestUpsertPostgresql(unittest.TestCase, OneByOne):
#     def setUp(self):
#         self.connection = psycopg2.connect('dbname=test_py_upsert')
#         self.cursor = self.connection.cursor()
#         create_table(self.cursor)

#     def tearDown(self):
#         self.connection.close()

#     def executeSql(self, template, values = ()):
#         self.cursor.execute(template, values)


# class TestUpsertMysql(unittest.TestCase, OneByOne):
#     def setUp(self):
#         self.connection = MySQLdb.connect(user="root",passwd="password",db="test_py_upsert")
#         self.cursor = self.connection.cursor()
#         create_table(self.cursor)

#     def tearDown(self):
#         self.connection.close()

#     def executeSql(self, template, values = ()):
#         self.cursor.execute(template, values)


class TestUpsertSqlite(unittest.TestCase, OneByOne):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.cursor = self.connection.cursor()
        create_table(self.cursor, 'pets')

    def tearDown(self):
        self.cursor.close()
        self.connection.close()

    def executeSql(self, template, values = ()):
        template = template.replace('%s', '?')
        self.cursor.execute(template, values)
