import os
db = os.environ.get('DB', 'postgresql')

import copy

# http://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
import random
from datetime import date, datetime, timedelta
def random_datetime(start, end):
    r = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
    if db == 'sqlite3' and r.__class__ == datetime:
        r = r.replace(microsecond=5000)
    return r

# this is python-faker not faker
import faker

lotsa_records = []
selectors = []
for _ in xrange(150):
    selectors.append({ 'name': faker.name.name(), 'tag_number': random.randint(0, 1e8)})
for _ in xrange(500):
    selector = random.choice(selectors)
    setter = {
        'lovability':           random.random() * 1e11,
        'spiel':                ''.join(faker.lorem.sentences()),
        'good':                 True,
        'birthday':             random_datetime(date(2001, 7, 14), date.today()),
        'morning_walk_time':    random_datetime(datetime(2001, 7, 14, 12, 30), datetime.now()),
        'home_address':         ''.join(faker.lorem.paragraphs()),
    }
    lotsa_records.append([selector, setter])

from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
 
Base = declarative_base()

class Pet(Base):
    __tablename__ = 'pets'
    name = Column(String(255), primary_key=True)
    gender = Column(String(255))
    spiel = Column(String(255))
    good = Column(Boolean)
    lovability = Column(Float)
    morning_walk_time = Column(DateTime)
    zipped_biography = Column(LargeBinary)
    tag_number = Column(Integer)
    birthday = Column(Date)
    home_address = Column(Text)

Index('foobar', Pet.name, Pet.tag_number, unique=True)

from upsert import Upsert

if db == 'mysql':
    import MySQLdb
    os.system('mysql -u root -ppassword -e "drop database test_py_upsert"')
    os.system('mysql -u root -ppassword -e "create database test_py_upsert charset utf8"')
    engine = create_engine('mysql+mysqldb://root:password@localhost:3306/test_py_upsert', echo=False)
    conn1 = MySQLdb.connect(user="root",passwd="password",db="test_py_upsert")
elif db == 'sqlite3':
    import sqlite3
    if os.path.exists('foobar.sqlite3'):
        os.unlink('foobar.sqlite3')
    engine = create_engine('sqlite:///foobar.sqlite3', echo=False)
    conn1 = sqlite3.connect('foobar.sqlite3')
elif db == 'postgresql':
    os.system("dropdb test_py_upsert")
    os.system("createdb test_py_upsert")
    import psycopg2
    conn1 = psycopg2.connect('dbname=test_py_upsert')
    engine = create_engine('postgresql+psycopg2://localhost:5432/test_py_upsert', echo=False)
else:
    raise("DB={0} not recognized".format(db))

print "** Running benchmarks on {0} **".format(db)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

def run_sqlalchemy():
    session = Session()
    session.execute('delete from pets')
    for (selector, setter) in lotsa_records:
        full_setter = copy.copy(selector)
        full_setter.update(setter)
        pet = session.query(Pet).filter_by(**selector).first()
        if pet is None:
            pet = Pet(**full_setter)
            session.add(pet)
        else:
            for (k, v) in full_setter.items():
                pet.__setattr__(k, v)
            session.merge(pet)
        session.commit() # otherwise it's cheating by keeping everything in memory
    session.close()

def run_naive():
    cur = conn1.cursor()
    cur.execute('delete from pets')
    for (selector, setter) in lotsa_records:
        full_setter = copy.copy(selector)
        full_setter.update(setter)
        where_sql = ' AND '.join("{0} = %s".format(k) for k in selector.keys())
        template = "SELECT COUNT(*) FROM pets WHERE {}".format(where_sql)
        if db == 'sqlite3':
            template = template.replace('%s', '?')
        cur.execute(template, selector.values())
        if cur.fetchone()[0] > 0:
            set_sql = ','.join("{0} = %s".format(k) for k in full_setter.keys())
            template = "UPDATE pets SET {} WHERE {}".format(set_sql, where_sql)
            if db == 'sqlite3':
                template = template.replace('%s', '?')
            cur.execute(template, full_setter.values()+selector.values())
        else:
            column_names = ','.join(full_setter.keys())
            values_sql = ','.join(['%s']*len(full_setter))
            template = "INSERT INTO pets ({}) VALUES ({})".format(column_names, values_sql)
            if db == 'sqlite3':
                template = template.replace('%s', '?')
            cur.execute(template, full_setter.values())
        cur.connection.commit()
    cur.close()

def run_upsert():
    cur = conn1.cursor()
    cur.execute('delete from pets')
    upsert = Upsert(cur, 'pets')
    for (selector, setter) in lotsa_records:
        upsert.row(selector, setter)
    cur.close()

import unittest

class Correctness(unittest.TestCase):
    def test_sqlalchemy(self):
        run_sqlalchemy()
        cur = conn1.cursor()
        cur.execute('select * from pets order by name')
        correct = cur.fetchall()
        cur.close()
        self.assertGreater(len(correct), 10)

        run_upsert()
        cur = conn1.cursor()
        cur.execute('select * from pets order by name')
        mine = cur.fetchall()
        cur.close()
        self.assertEqual(correct, mine)

    def test_naive(self):
        run_naive()
        cur = conn1.cursor()
        cur.execute('select * from pets order by name')
        correct = cur.fetchall()
        cur.close()
        self.assertGreater(len(correct), 10)

        run_upsert()
        cur = conn1.cursor()
        cur.execute('select * from pets order by name')
        mine = cur.fetchall()
        cur.close()
        self.assertEqual(correct, mine)

import benchmark

class Speed(benchmark.Benchmark):
    def test_upsert(self):
        run_upsert()

    def test_sqlalchemy(self):
        run_sqlalchemy()

    def test_naive(self):
        run_naive()

if __name__ == '__main__':
    benchmark.main(format="markdown", numberFormat="%.4g")
    unittest.main()