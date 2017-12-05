#coding=utf-8
import MySQLdb as mydatabase
import user_agents
import random
import time
import config
import spidermethod
import dbmethod
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 
import db

def main():
    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()
    file_name = config.keyword_db
    comment_list = dbmethod.combination(file_name)
    pool = Pool()
    comment_list = pool.map(dbmethod.set_color,comment_list)
    print comment_list[2]
    conn = mydatabase.connect(host='117.25.155.149', port=3306, user='gelinroot', passwd='glt#789A', db='db_dolphin', charset='utf8')
    cursor = conn.cursor()
    print len(comment_list)
    dbmethod.write_into_database(comment_list,cursor,conn)
    conn.commit()

