#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import MySQLdb.cursors
import time
import datetime

import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

database = {
    'host': '172.16.3.103',
    'user': 'shiksha',
    'password': 'shiKm7Iv80l',
    'db': 'bluespider',
    'port': 3306,
    'socket': '/tmp/mysql_06.sock',
    }


class Db:

    db = None
    cursorObject = None

    def __init__(self):
        try:
            self.db = MySQLdb.connect(
                host=database['host'],
                user=database['user'],
                passwd=database['password'],
                db=database['db'],
                port=database['port'],
                cursorclass=MySQLdb.cursors.DictCursor,
                use_unicode=True,
                charset='utf8',
                autocommit=True,
                unix_socket=database['socket'],
                )
            self.db.autocommit(True)

        # self.db.autocommit(True)

            self.cursorObject = self.db.cursor()
        except Exception as e:
            print( e)

    def insert(self, liveValues, action=None):  
        sql = None
        sql = \
                "INSERT INTO eventDiff (url, oldVal, newVal, diffDate, notify) VALUES (%s, %s ,%s ,%s ,%s)"

        try:
            self.query_executemany(sql, liveValues)
        except Exception as err:
            print( err)
            return False
        return True

    def getNewData(self,url):

        query = \
            "SELECT * from eventDiff where url like (%s) and status='live' limit 1000"  ## otify wud be 0

        try:
            self.query_execute(query, ["%"+url+"%"])
            return self.cursorObject.fetchall()
        except Exception as e:
            print( e)
            return list()


    def query_execute(self, query, args=list()):
        try:
            self.cursorObject.execute(query, args)
        except MySQLdb.OperationalError as error:

        # self.db.commit()

            print( 'Reconnecting BlueSpider DB..')
            self.db.ping(True)
            self.cursorObject.execute(query, args)

        # self.db.commit()

    def query_executemany(self, query, args=list()):
        try:
            self.cursorObject.executemany(query, args)
        except MySQLdb.OperationalError as error:

        # self.db.commit()

            print( 'Reconnecting BlueSpider DB..')
            self.db.ping(True)
            self.cursorObject.executemany(query, args)

        # self.db.commit()

    def __del__(self):
        if self.db != None:
            self.db.close()
