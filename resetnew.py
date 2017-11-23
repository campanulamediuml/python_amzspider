import MySQLdb as mydatabase
from multiprocessing import Pool
import random
import time
import cal_sell
import db

conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()

cursor.execute('SELECT * FROM py_product_comments')
result = cursor.fetchall()
rst = []
for i in result:
    rst.append(i[1:-1])

cursor.execute('SELECT * FROM py_product_comments_tmp')
new = cursor.fetchall()
nw = []
for i in new:
    nw.append(i[1:-1])

nw = list(set(nw))

def reset_list(line):
    if line not in rst:
        return line

    

# def count_increase(write_in):
#     index_dict = {}
#     for i in write_in:
#         if i[0] in index_dict:
#             index_dict[i[0]] += 1
#         else:
#             index_dict[i[0]] = 1

#     return index_dict


def write_in_database(write_in):
    count = 0
    print 'committing'
    for line in write_in:
        if line != None :
            #print line
            try:
                cursor.execute('SELECT * FROM py_product_comments WHERE prod_asin = "%s" AND content = "%s" AND user_name = "%s" AND user_address = "%s"'%(line[0],line[2],line[3],line[6]))
            except Exception,e:
                print e

                print line[2]
            tmp = cursor.fetchall()
            if len(tmp) != 0:
                #print len(tmp)
                #print tmp
                content_tuple = (str(line[-1]),str(line[0]),line[2],line[3],str(line[6]))
                #print content_tuple

                cursor.execute('UPDATE py_product_comments SET vote = "%s" WHERE prod_asin = "%s" AND content = "%s" AND user_name = "%s" AND user_address = "%s"'%content_tuple) 
                content_tuple = (str(line[0]),line[2],line[3],str(line[6]))
                #print content_tuple
                cursor.execute('UPDATE py_product_comments SET syn_status = 2 WHERE prod_asin = "%s" AND content = "%s" AND user_name = "%s" AND user_address = "%s"'%content_tuple) 
                count += 1
                # print 'update'
                #
                    #conn.commit()
            else:
                cursor.execute('INSERT INTO py_product_comments(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,good_type,vote)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line) 
                count+= 1
                # if count%10000 == 0:
                #     #conn.commit()
                #     print 'update'
                        
                # except Exception,e:
                #     print e 
                #     print line
                #     continue
        


# def write_increase(index_dict):
#     for key in index_dict:
#         try:
#             sql_1 = "UPDATE py_productDynamic SET pro_commentincrement = "+str(index_dict[key])+" WHERE pro_asin = '"+str(key)+"'AND pro_ctime LIKE '%"+str(time.strftime('%Y-%m-%d',time.localtime(time.time())))+"%'"
#             #print str(key),index_dict[key]
#             sql_2 = "UPDATE py_productDynamic SET pro_salesvolume = "+str(cal_sell.cal_sell(index_dict[key]))+" WHERE pro_asin = '"+str(key)+"'AND pro_ctime LIKE '%"+str(time.strftime('%Y-%m-%d',time.localtime(time.time())))+"%'"
            
#             cursor.execute(sql_1)
#             cursor.execute(sql_2)
#         except Exception,e:
#             print str(e)
#             continue
#     conn.commit()

pool = Pool()
print 'reseting'
write_in = pool.map(reset_list,nw)
#write_in = reset_list(nw,rst)
print 'reset finished'
write_in = list(set(write_in))
print len(write_in)
# index_dict = count_increase(write_in)
print 'writing into database'
# write_increase(index_dict)
write_in_database(write_in)
conn.commit()
print 'committing'
