#coding=utf-8
import MySQLdb as mydatabase
from multiprocessing import Pool
import config
import db

conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()


print 'connecting successful...'

cursor.execute('SELECT * FROM py_keyword_main')#从数据库中提取全部数据
key_words = cursor.fetchall()

# cursor.execute('SELECT * FROM py_shoes_comment_raw_data')
# raw_data = cursor.fetchall()

cursor.execute('SELECT * FROM py_keyword_word_count')
key_count = cursor.fetchall()

def get_pos_count(data_tuple):
    result = {} #统计关键词中属于正面评论的数量
    for line in data_tuple:
        key_word = line[1]+','+str(line[2])
        if str(line[5]) == '1':
            if key_word not in result:
                result[key_word] = 1
            else:
                result[key_word] += 1

    return result

def get_neg_count(data_tuple):
    result = {}
    for line in data_tuple:     #制作出一个字符串，内容是关键词和评分
        key_word = line[1]+','+str(line[2]) #这个字典只保存负面态度的内容
        if str(line[5]) == '-1':            
            if key_word not in result:      #做出一个字典，字典的key是关键词带分数，value是出现次数
                result[key_word] = 1
            else:
                result[key_word] += 1

    return result

def get_mid_count(data_tuple):
    result = {}
    for line in data_tuple:
        key_word = line[1]+','+str(line[2]) #制作出一个字符串，内容是关键词和评分
        if str(line[5]) == '0':         #这个字典只保存中立态度的内容
            if key_word not in result: #做出一个字典，字典的key是关键词带分数，value是出现次数
                result[key_word] = 1
            else:
                result[key_word] += 1

    return result



def get_count_all(data_tuple):#我其实也不是很想写注释
    result = {}
    for line in data_tuple:
        key_word = line[1]+','+str(line[2])#好吧，还是写一下吧，这条注释是这样的
        if key_word not in result: 
            result[key_word] = 1 #把所有信息全部储存进一个字典里，字典的key是关键词带分数，value是出现次数
        else:
            result[key_word] += 1
    return result

def get_express_id(key_word):
    key = (key_word.split(','))[0]
    for i in key_count:
        if i[1] == key:
            return str(i[0])



all_count_dict = get_count_all(key_words) #这个是用来获取所有词的出现次数
#print all_count_dict
mid_count_dict = get_mid_count(key_words) #获取每个词在中评中出现的次数
# print mid_count_dict
pos_count_dict = get_pos_count(key_words) #获取每个词在好评中出现的次数

neg_count_dict = get_neg_count(key_words) #获取每个词在差评中出现的次数
result_list = []

print 'analyzing..'

key_word_index = []
for key_word in all_count_dict:
    key_word_index.append(key_word)

def get_result_tuple(key_word):
    #print key_word
    word_with_score = key_word
    try:
        all_count = all_count_dict[key_word]
        key_word
        all_count
    except:
        all_count = '0'

    try:
        mid_count = mid_count_dict[key_word]
    except:
        mid_count = '0'

    try:
        pos_count = pos_count_dict[key_word]
    except:
        pos_count = '0'

    try:
        neg_count = neg_count_dict[key_word]
    except:
        neg_count = '0'

    try:
        express_id = get_express_id(key_word)
    except:
        express_id = 'N/A'

    result_tuple = (word_with_score,str(all_count),str(pos_count),str(neg_count),str(mid_count),config.good_type,express_id)
    #print result_tuple
    #通过哈希值查找，依次列出各个统计数量

    return result_tuple


pool = Pool(5)
result_list = pool.map(get_result_tuple,key_word_index)
# for key in key_word_index:
#     get_result_tuple(key)

conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()
cursor.execute('SELECT * FROM py_keyword_count')
key_count = cursor.fetchall()
key_index = []
for i in key_count:
    key_index.append(i[1])


print 'writing into database...'

count = 0
for line in result_list:
    if line[0] not in key_index:
        try:
            cursor.execute('INSERT INTO  py_keyword_count (express_with_score,count_all,count_pos,count_neg,count_mid,good_type,express_id)  values(%s,%s,%s,%s,%s,%s,%s)',line) 
            count = count+1
            if count % 5000 == 0:
                conn.commit()
        except:
            continue
    else:
        cursor.execute('UPDATE py_keyword_count SET count_all = "'+str(line[1])+'" WHERE express_with_score = "'+str(line[0])+'" and good_type = "'+config.good_type+'"')
        cursor.execute('UPDATE py_keyword_count SET count_pos = "'+str(line[2])+'" WHERE express_with_score = "'+str(line[0])+'" and good_type = "'+config.good_type+'"')
        cursor.execute('UPDATE py_keyword_count SET count_neg = "'+str(line[3])+'" WHERE express_with_score = "'+str(line[0])+'" and good_type = "'+config.good_type+'"')
        cursor.execute('UPDATE py_keyword_count SET count_mid = "'+str(line[4])+'" WHERE express_with_score = "'+str(line[0])+'" and good_type = "'+config.good_type+'"')
        if count % 5000 == 0:
            conn.commit()

conn.commit()


















