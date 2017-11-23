#coding=utf-8
import time
import os
import MySQLdb as mydatabase
import db

open('result/result_comment.txt','w')
os.system('python autorun.py')
os.system('python washdb.py')
os.system('python resetnew.py')
os.system('python word_key.py')
os.system('python set_AZCM.py')
os.system('python word_count_key.py')
conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()
# cursor.execute('TRUNCATE TABLE py_product_comments_tmp')
conn.commit()

