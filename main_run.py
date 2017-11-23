#coding=utf-8
import time
import os
import MySQLdb as mydatabase
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
#自动运行该脚本即可

print open('shenshou.py').read()

def main():
    open('result/result_comment.txt','w')
    os.system('python autorun.py')
    os.system('python washdb.py')
    os.system('python resetnew.py')
    os.system('python word_key.py')
    os.system('python set_AZCM.py')
    os.system('python word_count_key.py')
    print 'analyze finish...'
    print 'washing tmp database...'
    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()
    cursor.execute('TRUNCATE TABLE py_product_comments_tmp')
    print 'wash successfully'
    print time.localtime()
    conn.commit()

while 1:
    localtime = time.asctime(time.localtime(time.time()))
    #print time.localtime(time.time())
    if localtime.split()[3].split(':')[0] == '20':
        print 'running...'
        main()
        print 'other ring begin..waiting...'
        print open('shenshou.py').read()
    time.sleep(60)

