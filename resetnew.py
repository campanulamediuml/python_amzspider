#coding=utf-8
import MySQLdb as mydatabase
from multiprocessing import Pool
import random
import time
import cal_sell
import db

conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()

def get_data():
    cursor.execute('SELECT * FROM py_product_comments')
    result = cursor.fetchall()
    rst = []
    for i in result:
        rst.append(i[1:-1])
    print len(rst)

    cursor.execute('SELECT * FROM py_product_comments_tmp')
    new = cursor.fetchall()
    nw = []
    for i in new:
        nw.append(i[1:-1])
    print len(nw)

    long_list = list(set(rst+nw))
    return long_list

def get_index_vote(comment_list):
    index_vote = {}
    for line in comment_list:
        if line[:-1] not in index_vote or index_vote[line[:-1]] < line[-1]:
            index_vote[line[:-1]] = line[-1]
    result_list = []
    for key in index_vote:
        tmp = []
        for i in key:
            tmp.append(i)
        tmp.append(index_vote[key])
        result_list.append(tmp)
    return result_list
#使用哈希法去重，哈希相同的选择更高的投票数

def commit_into_database(result_list):
    cursor.execute('TRUNCATE TABLE py_product_comments_tmp')
    conn.commit()

    count = 0
    for line in result_list:
        cursor.execute('INSERT INTO py_product_comments_tmp(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,vote,good_type)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line) 
            #cursor.execute('INSERT INTO fashion_shoe_comment_tmp (prod_asin,title,content,user_name,color,type_call,user_address,vote,prod_star,create_date)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line)
            #写入数据库
        count += 1
        if count % 10000==0:
            conn.commit()
    conn.commit()

    cursor.execute('TRUNCATE TABLE py_product_comments')
    conn.commit()

    count = 0
    for line in result_list:
        cursor.execute('INSERT INTO py_product_comments(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,vote,good_type)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line) 
            #cursor.execute('INSERT INTO fashion_shoe_comment_tmp (prod_asin,title,content,user_name,color,type_call,user_address,vote,prod_star,create_date)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line)
            #写入数据库
        count += 1
        if count % 10000==0:
            conn.commit()
    conn.commit()  

def main():
    long_list = get_data()
    result_list=get_index_vote(long_list)
    commit_into_database(result_list)
    conn.commit()  

main()

