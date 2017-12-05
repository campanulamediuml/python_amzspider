#coding=utf-8
import time
import os
import MySQLdb as mydatabase
import db
import word_key
import washdb
import resetnew
import autorun
import config

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
#自动运行该脚本即可

print open('shenshou.py').read()

def main():
    time_1 = time.time()
    open('result/result_comment.txt','w')
    #autorun.main()
    washdb.main()
    time_2 = time.time()
    print '数据爬取花费',str(time_2-time_1),'秒'
    resetnew.main()
    time_3 = time.time()
    print '数据去重花费',str(time_3-time_2),'秒'
    word_key.main()
    time_4 = time.time()
    print '关键词分析取花费',str(time_4-time_3),'秒'
    print 'analyze finish...'
    print 'washing tmp database...'
    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()
    cursor.execute('TRUNCATE TABLE py_product_comments_tmp')
    print 'wash successfully'
    cursor.execute('UPDATE py_product_comments SET syn_status = 0')
    print time.localtime()
    conn.commit()

while 1:
    localtime = time.asctime(time.localtime(time.time()))
    #print time.localtime(time.time())
    if localtime.split()[3].split(':')[0] == str(config.run_time):
        time_1 = time.time()
        print 'running...'
        main()
        time_2 = time.time()
        print time_2-time_1
        print 'other ring begin..waiting...'
        print open('shenshou.py').read()
    time.sleep(60)

