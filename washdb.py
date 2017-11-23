#coding=utf-8
import MySQLdb as mydatabase
import db
conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()

#cursor.execute('DELETE FROM py_product_comments_tmp')
#不能删掉tmp表
cursor.execute('TRUNCATE TABLE py_keyword_main')
cursor.execute('TRUNCATE TABLE py_keyword_main_tmp')
cursor.execute('TRUNCATE TABLE py_keyword_word_count')
cursor.execute('TRUNCATE TABLE py_keyword_count')

conn.commit()