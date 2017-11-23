#coding=utf-8
import MySQLdb as mydatabase
import config
from multiprocessing import Pool
import db

#***********************
#  ┏┓     ┏┓
# ┏┛┻━━━━━┛┻┓
# ┃         ┃
# ┃    ━    ┃
# ┃ ┳┛   ┗┳ ┃
# ┃         ┃
# ┃    ┻    ┃
# ┃         ┃
# ┗━┓     ┏━┛
#   ┃     ┃ 神兽保佑
#   ┃     ┃ 代码永无BUG！
#   ┃     ┗━━━┓
#   ┃         ┣┓
#   ┃         ┏┛
#   ┗┓┓┏━━━┳┓┏┛
#    ┃┫┫   ┃┫┫
#    ┗┻┛   ┗┻┛
#*************************


conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()
print 'connecting successful...'

# sql_1 = 'CREATE TABLE py_keyword_dictionary_main_tmp (id INT(11)primary key auto_increment ,express VARCHAR(200),score TEXT(100),comment_id INT(10),comment TEXT(100000),pos_or_neg VARCHAR(10),good_type VARCHAR(100), express_id INT(20),asin VARCHAR(20))'
# try:
#     cursor.execute(sql_1)
# except:
#     print 'table exists'

cursor.execute('SELECT * FROM py_product_comments WHERE good_type = "'+config.good_type+'"')#从数据库中提取全部数据
pro_info = cursor.fetchall()
print len(pro_info)

cursor.execute('SELECT * FROM py_keyword_main_tmp WHERE good_type = "'+config.good_type+'"')#从数据库中提取全部数据
key_words = cursor.fetchall()
print len(key_words)


key_list = []
for i in key_words:
    tmp = []
    for j in i:
        tmp.append(j)
    key_list.append(tmp[1:])

result = []

def func_judge(key):
    pro_index = key[2]
    for i in pro_info:
        if int(i[0]) == int(pro_index):
            key.append(i[1])
            key.append(i[6])
            key.append(i[5])            
            break
    return key


pool = Pool()
result = pool.map(func_judge,key_list)


print 'writing into database...'

conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()
print 'connecting successful...'
cursor.execute('SELECT * FROM py_keyword_main WHERE good_type = "'+config.good_type+'"')#从数据库中提取全部数据
key_words = cursor.fetchall()
key_index = []
for i in key_words:
    tmp = []
    for j in i[1:-1]:
        tmp.append(j)
    key_index.append(tmp)




print result[4]
count = 0
for line in result:
    if line not in key_index:
    #print line
        cursor.execute('INSERT INTO  py_keyword_main(express,score,comment_id,comment,pos_or_neg,good_type,express_id,prod_asin,son_asin,attribute)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line) 
        count += 1
        if count % 10000 == 0:
            conn.commit()
    else:
        continue

conn.commit() 