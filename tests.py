import unittest
import psycopg2

from upsert import *

class TestUpsertPostgresql(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg2.connect('dbname=test_py_upsert')
        cur = self.connection.cursor()
        cur.execute("""
        DROP TABLE IF EXISTS pets;
""")
        cur.execute("""
        CREATE TABLE pets ( "name" CHARACTER VARYING(255) PRIMARY KEY, color CHARACTER VARYING(255), license INTEGER, weight FLOAT );
""")
#        self.connection.commit()

    def tearDown(self):
        self.connection.close()

    def test_single(self):
        upsert = Upsert(self.connection, 'pets')
        upsert.row({'name': 'Jerry'})

        cur = self.connection.cursor()
        cur.execute("select name from pets;")
        self.assertEqual('Jerry', cur.fetchone()[0])
