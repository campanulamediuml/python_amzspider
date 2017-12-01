#coding=utf-8
import MySQLdb as mydatabase
import config
from multiprocessing import Pool
import db
import time

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

cursor.execute('SELECT * FROM py_product_comments')#从数据库中提取全部数据
pro_info = cursor.fetchall()

cursor.execute('SELECT * FROM py_keyword_main_tmp')#从数据库中提取全部数据
key_words = cursor.fetchall()

word_dict = {}
info_dict = {}

for i in pro_info:
    info_dict[i[0]] = i[1:-1]
for i in key_words:
    word_dict[i[0]] = i[1:]

result = []
for line in word_dict:
    tmp = list(word_dict[line])
    info_list = [info_dict[tmp[2]][0],info_dict[tmp[2]][5],info_dict[tmp[2]][4]]
    tmp = tmp+info_list
    result.append(tmp)
print result[0]

print 'commiting'
count = 0
for line in result:
    #print line
    cursor.execute('INSERT INTO  py_keyword_main(express,score,comment_id,comment,pos_or_neg,good_type,express_id,prod_asin,son_asin,attribute)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line) 
    count += 1
    if count % 10000 == 0:
        conn.commit()


conn.commit() 


