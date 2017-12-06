#coding=utf-8
import MySQLdb as mydatabase
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import rake
import nltk
import emotionanalysis
import config
import db
import time

def get_key_word(comment_line):
    nltk_keyword = emotionanalysis.get_keyword(comment_line[3])#用nltk提取出评论正文中的关键词
    nltk_keyword = nltk_keyword.lower()#统一处理成小写
    nltk_keyword = nltk_keyword.split(',')#以逗号分割成列表
    
    judge = comment_line[8]
    if int(judge) > 3:
        judge = '1'
    elif int(judge) < 3:
        judge = '-1'
    else:
        judge = '0'
    #此处添加判定，是好评还是差评，用-1，0，1进行标记

    tmp = rake.select_kw(comment_line[3])
    #通过rake提取关键词和关键词评分

    rst_tmp = []
    for j in tmp:
        if j[0] not in nltk_keyword:
            #排除掉rake中分数过低的关键词
            rst_tmp.append([j[0],j[1],comment_line[0],comment_line[3],judge,config.good_type])
            #格式化输出结果
#这块代码用来获取关键词       
    return rst_tmp

def count_all(analys):
    count_dict_all = {}
    #采用字典的方法获取全部评论的关键词统计结果
    for i in analys:
        if i[0] in count_dict_all:
            count_dict_all[i[0]] += 1
        else:
            count_dict_all[i[0]] = 1
    return count_dict_all
    #返回一个字典，key是关键词，value是出现次数，该字典包含全部评论


def count_pos(analys):
    count_dict_pos = {}
    #采用字典的方法获取好评的关键词统计结果
    for i in analys:
        if str(i[4]) == '1':
            if i[0] in count_dict_pos:
                count_dict_pos[i[0]] += 1
            else:
                count_dict_pos[i[0]] = 1
        else:
            continue
    return count_dict_pos
    #返回一个字典，key是关键词，value是出现次数，该字典仅包含好评

def count_neg(analys):
    count_dict_neg = {}
    #采用字典的方法获取差评的关键词统计结果
    for i in analys:
        if str(i[4]) == '-1':
            if i[0] in count_dict_neg:
                count_dict_neg[i[0]] += 1
            else:
                count_dict_neg[i[0]] = 1
        else:
            continue
    return count_dict_neg
    #返回一个字典，key是关键词，value是出现次数，该字典仅包含差评

def count_mid(analys):
    count_dict_mid = {}
    for i in analys:
        if str(i[4]) == '0':
            if i[0] in count_dict_mid:
                count_dict_mid[i[0]] += 1
            else:
                count_dict_mid[i[0]] = 1
        else:
            continue
    return count_dict_mid
    #获取中评，方法同上，这条字典的内容是中评

def get_word_type(analys):
    word_type_dict = {}
    for i in analys:
        if i[0] not in word_type_dict:
            word_type_dict[i[0]] = nltk.pos_tag([i[0]])
    return word_type_dict
    #获取词语的类型

def get_word_count(analys):

    print 'counting...'
    count_dict_all = count_all(analys)
    count_dict_pos = count_pos(analys)
    count_dict_mid = count_mid(analys)
    count_dict_neg = count_neg(analys)
    #获取到好评差评中评的关键词字典
    word_type_dict = get_word_type(analys)
    #分析关键词的词性

    result_list = []
    for word in count_dict_all:
        try:
            neg = str(count_dict_neg[word])
        except:
            neg = '0'
        try:
            pos = str(count_dict_pos[word])
        except:
            pos = '0'
        try:
            mid = str(count_dict_mid[word])
        except:
            mid = '0'

        result_tuple = (word,((word_type_dict[word])[0])[1],count_dict_all[word],pos,neg,mid,config.good_type)
        result_list.append(result_tuple)

    result_list = list(set(result_list))
    
    cursor.execute('SELECT * FROM py_keyword_word_count')
    key_words_list = cursor.fetchall()
    key_words_index = []
    for key_word_line in key_words_list:
        key_words_index.append(key_word_line[1])

    print 'writing into database...'
    count = 0

    sql = 'INSERT INTO  py_keyword_word_count (express_without_score,word_type,count_all,count_pos,count_neg,count_mid,good_type)  values'
    inser_list = []
    for line in result_list:
        sql += '(%s,%s,%s,%s,%s,%s,%s),'
        inser_list.extend(list(line))
        count = count+1
        if count % 10000 == 0:
            cursor.execute(sql[:-1],inser_list)
            sql = 'INSERT INTO  py_keyword_word_count (express_without_score,word_type,count_all,count_pos,count_neg,count_mid,good_type)  values'
            inser_list = []
            conn.commit()#五千条提交一次 
    try:
        cursor.execute(sql[:-1],inser_list)
    except:
        pass
    conn.commit()


def get_analysis():
    print 'connecting successful...'
    cursor.execute('SELECT * FROM py_product_comments')
    results = cursor.fetchall()
    comment_list = []
    for i in results:
        comment_list.append(i)

    print 'analyzing...'

    pool = Pool(4)
    #抢占资源，运行时电脑有多少个逻辑核心就抢多少个
    result = pool.map(get_key_word,comment_list)
    pool.close()
    #返回一个list列表，列表内容也是一个list
    analys = []
    for i in result:
        analys.extend(i)
    #扩展成一条单独的list
    #把处理结果储存起来，然后清空列表
    return analys

# def get_key_words(line): 
#     return tuple([final_result.index(line)+1]+list(line))

def get_keyword_table(line):
    tmp = list(word_dict[line])
    info_list = [info_dict[tmp[2]][0],info_dict[tmp[2]][5],info_dict[tmp[2]][4]]
    tmp = tmp+info_list
    return tmp

def get_final_result(item):
    return tuple(item+[index_dict[item[0]]])

def main():
    global final_result
    global index_dict
    global word_dict
    global info_dict
    global conn
    global cursor

    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()
    analys = get_analysis()
    get_word_count(analys)

    cursor.execute('SELECT * FROM py_keyword_word_count')
    results = cursor.fetchall()

    print 'getting main table...'

    time_start = time.time()

    index_dict = {}
    for i in results:
        index_dict[i[1]] = i[0]

    pool = Pool(4)
    final_result = map(get_final_result,analys)
    pool.close()
    time_mid = time.time()

    key_words = []
    for i in range(1,len(final_result)+1):
        line = [i]
        line.extend(list(final_result[i-1]))
        key_words.append(tuple(line))

    print 'get main table successful'

    word_dict = {}
    info_dict = {}
    time_end = time.time()

    print time_end-time_start

    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM py_product_comments')#从数据库中提取全部数据
    pro_info = cursor.fetchall()

    for i in pro_info:
        info_dict[i[0]] = i[1:-1]
    for i in key_words:
        word_dict[i[0]] = i[1:]

    pool=Pool(4)
    result = pool.map(get_keyword_table,word_dict)
    pool.close()
    # print len(result)

    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()

    print 'commiting'
    count = 0

    sql = 'INSERT INTO  py_keyword_main(express,score,comment_id,comment,pos_or_neg,good_type,express_id,prod_asin,son_asin,attribute)  values'
    inser_list = []

    for line in result:
        sql+='(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),'
        inser_list.extend(list(line))

        count += 1
        if count % 10000 == 0:
            cursor.execute(sql[:-1],inser_list) 
            sql = 'INSERT INTO  py_keyword_main(express,score,comment_id,comment,pos_or_neg,good_type,express_id,prod_asin,son_asin,attribute)  values'
            inser_list = []
            conn.commit()#五千条提交一次 

    try:
        cursor.execute(sql[:-1],inser_list)
    except:
        pass
    conn.commit()



