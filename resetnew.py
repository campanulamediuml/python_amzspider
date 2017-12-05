#coding=utf-8
import MySQLdb as mydatabase
from multiprocessing import Pool
import random
import time
import db


def get_data():
    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()
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
    print len(result_list)
    return result_list
#使用哈希法去重，哈希相同的选择更高的投票数

def commit_into_database(result_list):
    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()
    cursor.execute('TRUNCATE TABLE py_product_comments_tmp')
    conn.commit()
    print 'writing into tmp table'

    count = 0
    sql = 'INSERT INTO py_product_comments_tmp(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,good_type,vote)  values'
    inser_list = []
    for line in result_list:
        sql += '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),'
        inser_list.extend(list(line))
        count += 1
        if count % 10000==0:
            cursor.execute(sql[:-1],inser_list)
            sql = 'INSERT INTO py_product_comments_tmp(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,good_type,vote)  values'
            inser_list = []
            conn.commit()
    try:
        cursor.execute(sql[:-1],inser_list)
    except:
        pass
    conn.commit()

    cursor.execute('TRUNCATE TABLE py_product_comments')
    conn.commit()

    print 'writing into real table'

    count = 0
    sql = 'INSERT INTO py_product_comments(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,good_type,vote)  values'
    inser_list = []
    for line in result_list:
        sql += '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),'
        inser_list.extend(list(line))
        count += 1
        if count % 10000==0:
            cursor.execute(sql[:-1],inser_list)
            sql = 'INSERT INTO py_product_comments(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,good_type,vote)  values'
            inser_list = []
            conn.commit()
    try:
        cursor.execute(sql[:-1],inser_list)
    except:
        pass
    conn.commit()

def main():
    long_list = get_data()
    result_list=get_index_vote(long_list)
    commit_into_database(result_list) 



