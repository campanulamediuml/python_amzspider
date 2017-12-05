#coding=utf-8
import MySQLdb as mydatabase
import config
import json
import exchangetype

def combination(keywords):
    headers = ['prod_asin','title','content','user_name','color','type_call','user_address','prod_star','create_date','prod_website','prod_group_number','vote','good_type']
    rst = []
    for i in keywords:
        fh = open('result/'+i+'result_comment.txt')
        comment_list = fh.readlines()
        rst.extend(comment_list)

    rst = list(set(rst))
    
    result = []
    for item in rst:
        line_dict = json.loads(item)
        line = []
        for word in headers:
            line.append(line_dict[word])
        result.append(line)

    return result

    #把几个评论列表连接起来，变成一个巨大的列表

def set_color(line):
    result_data = line
    try:  
        for i in result_data:
            result_data[result_data.index(i)] = i.replace('"','\\\"')
            result_data[result_data.index(i)] = i.replace("'","\\\'")
        #print result_data
        tmp = result_data[4]
        #color_json = crm_method.
        
        result_data[4] = exchangetype.stringtojson(tmp)
        #result.append(result_data)
    except:
        result_data = line
    
    return result_data
    # 重设商品信息格式，储存成json

def write_into_database(comment_list,cursor,conn):
    count = 0 
    sql = 'INSERT INTO py_product_comments_tmp(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,vote,good_type)  values'
    inser_list = []
    for line in comment_list:
        sql+='(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),'
        inser_list.extend(list(line))
        count += 1
        if count%10000 == 0:
            cursor.execute(sql[:-1],inser_list)
            sql = 'INSERT INTO py_product_comments_tmp(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,vote,good_type)  values'
            inser_list = [] 
            conn.commit()
    try:
        cursor.execute(sql[:-1],inser_list)
    except:
        pass
    conn.commit()