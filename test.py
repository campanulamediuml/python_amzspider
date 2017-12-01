#coding=utf-8
import MySQLdb as mydatabase
import json
import db
conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()

# cursor.execute('SELECT * FROM py_product_comments')#从数据库中提取全部数据
# pro_info = cursor.fetchall()
# for line in pro_info[:1000]:
#     cursor.execute('INSERT INTO py_product_comments_tmp(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,vote,good_type)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line[1:-1])
# conn.commit() 

cursor.execute('UPDATE py_product_comments_tmp SET vote = 9999')
conn.commit()