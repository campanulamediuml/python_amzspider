#coding=utf-8
import MySQLdb as mydatabase
from multiprocessing import Pool
import random
import time
import cal_sell
import db

conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()

cursor.execute('SELECT * FROM py_product_comments_tmp')
new = cursor.fetchall()
nw = []
nw_without_vote = []
for i in new:
    nw.append(i[1:-1])
    #把不包含主键和同步状态的行内容取下来，做成list，list的每个元素为数据库内的一行
    #nw_without_vote.append(i[1:-2])


def get_vote_index_dict(nw):
#你们也熟悉我的代码风格，看函数名差不多就知道是干啥的了
    vote_index = {}
    for line in nw:
        #逐个遍历表的每一行
        if line[:-1] not in vote_index or int(vote_index[line[:-1]]) < int(line[-1]):
            #每一行的前面部分作为key的名称，value是投票数量，当出现某一条的投票数大于字典内的对应键值，那就把这条更新掉就行了
            vote_index[line[:-1]] = line[-1]
    return vote_index
    #通本方法返回一个字典，这条字典的每一个value都是同一条评论中vote数量最高的
    #之前搞php的后端强行表示这么复杂的数据结构不可能在O(n)内实现，我觉得要就是他自己的问题

def get_result_list(vote_index):
    result_list = []
    #把整个字典内的所有数据一条一条拿出来，把key和value拼接一下就行了
    for key in vote_index:
        #要注意的是，key是个元组的数据结构，所以我们需要的是,先把key转成列表结构，然后把value转化成一个单元素的列表，两个列表相加
        #最后再把这个最终得到的list转化回元组
        result_list.append(tuple(list(key)+[vote_index[key]]))
    return result_list
    #最终返回值是一个包含若干tuple的list，每个tuple可以正好插入数据库

def write_into_database(result_list):
    print 'committing'
    count = 0
    for line in result_list:
        cursor.execute('INSERT INTO py_product_comments(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,good_type,vote)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line) 
        count+= 1
        if count % 10000 == 0:
            conn.commit()
    conn.commit()

def main():
    vote_index = get_vote_index_dict(nw)
    result_list = get_result_list(vote_index)
    print len(result_list)
    result_list = list(set(result_list))
    print len(result_list)
    write_into_database(result_list)

main()



# def write_in_database(write_in):
#     count = 0
#     print 'committing'
#     for line in write_in:
#         if line != None :
#             cursor.execute('SELECT * FROM py_product_comments WHERE prod_asin = "%s" AND content = "%s" AND user_name = "%s" AND user_address = "%s"',[line[0],line[2],line[3],line[6]])
#             if len(cursor.fetchall()) != 0:

#                 cursor.execute('UPDATE py_product_comments SET vote = "'+str(line[-1])+'" WHERE prod_asin = "'+str(line[0])+'" AND content = "'+line[2]+'" AND user_name = "'+line[3]+'" AND user_address = "'+str(line[6])+'"') 
#                 cursor.execute('UPDATE py_product_comments SET syn_status = 2 WHERE prod_asin = "'+str(line[0])+'" AND content = "'+line[2]+'" AND user_name = "'+line[3]+'" AND user_address = "'+str(line[6])+'"') 
#                 count += 1
#                 if count%10000 == 0:
#                     conn.commit()
#             else:
#                 cursor.execute('INSERT INTO py_product_comments(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,good_type,vote)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line) 
#                 count+= 1
#                 if count%10000 == 0:
#                     conn.commit()
#                     print 'update'
                        

# pool = Pool()
# print 'reseting'
# # write_in = pool.map(reset_list,nw)
# write_in = reset_list(nw,rst)
# print 'reset finished'
# write_in = list(set(write_in))
# print len(write_in)
# index_dict = count_increase(write_in)
# print 'writing into database'
# # write_increase(index_dict)
# print len
# write_in_database(write_in)
# conn.commit()
# print 'committing'
