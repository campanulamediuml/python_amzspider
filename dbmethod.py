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

    print result[3]
    print len(result[3])

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
    for line in comment_list:
        try:
            cursor.execute('INSERT INTO py_product_comments_tmp(prod_asin,title,content,user_name,attribute,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,vote,good_type)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line) 
        #cursor.execute('INSERT INTO fashion_shoe_comment_tmp (prod_asin,title,content,user_name,color,type_call,user_address,vote,prod_star,create_date)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line)
        #写入数据库
            count = count + 1           
            # except:
            #     print line
            #     continue
            #写入数据库
            if count%50 == 0:
                conn.commit()
        except Exception,e:
            print e
            print line
            continue

    conn.commit()