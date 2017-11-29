#coding=utf-8
import sys
sys.path.append("..")
import MySQLdb as mydatabase
import db

conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()

cursor.execute('SELECT * FROM user_product WHERE website = "AZCM" AND user_id > 82')#从数据库中提取全部数据
asid_list = cursor.fetchall()


result = []
for i in asid_list:
    asid = i[3]
    try:
        if str(asid)[0] == 'B':
            result.append(asid)
        else:
            continue
    except:
        continue

result = list(set(result))

fh = open('asid_list.txt','w')
for i in result:
    fh.write(str(i.strip()))
    fh.write('\n')

fh.close()


