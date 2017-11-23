#coding=utf-8
import MySQLdb as mydatabase
import json
import db
conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
cursor = conn.cursor()


def stringtojson(string):
    string_list = string.split('|')
    result_dict = {}
    option_dict = {}
    try:
        for i in string_list:
            option_dict[str(string_list.index(i))] = i.split(':')[0]
            result_dict[i.split(':')[0]] = i.split(':')[1]
        result_content = {'option_name':option_dict,'option_value':result_dict}
    except:
        result_content = {'option_name':{'m0':'attribute'},'option_value':{'attribute':string}}
   
    return_json = json.dumps(result_content)
    print type(return_json)
    return return_json

def set_line_attribute(line):
    tmp_line = []
    for i in line[1:-1]:
        tmp_line.append(i)
    attribute_json = line[5]
    attribute_dict = json.loads(attribute_json)

    #print attribute_dict
    for key in attribute_dict['option_name']:
        tmp_line.append(attribute_dict['option_name'][key])
        tmp_line.append(attribute_dict['option_value'][attribute_dict['option_name'][key]])
    while len(tmp_line)<21:
        tmp_line += ['N/A']
    return tmp_line



def refresh():
    cursor.execute('SELECT * FROM py_product_comments')
    all_results = cursor.fetchall()

    result_dict={}
    for ident in all_results:
        result_dict[str(ident[0])] = stringtojson(ident[5])
    #print result_dict
    count = 0
    for i in result_dict:
        write_tuple = (str(result_dict[i]),str(i).strip())
        print write_tuple[0]
        cursor.execute('UPDATE py_product_comments SET color = %s  WHERE id = %s',write_tuple)
        cursor.execute('UPDATE py_product_comments SET syn_status = 2  WHERE id = %s',(write_tuple[1],))

        # count+=1
        # if count== 10000:
        #     conn.commit()
    conn.commit()

def copy_database():
    count = 0
    cursor.execute('SELECT * FROM py_product_comments')
    all_results = cursor.fetchall()
    for line in all_results[:1000]:
        print line[1:-1]
        cursor.execute('INSERT INTO py_product_comments_tmp(prod_asin,title,content,user_name,color,type_call,user_address,prod_star,create_date,prod_website,prod_group_number,vote,good_type)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line[1:-1]) 
        #cursor.execute('INSERT INTO fashion_shoe_comment_tmp (prod_asin,title,content,user_name,color,type_call,user_address,vote,prod_star,create_date)  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',line)
        #写入数据库
        count = count + 1           
        # except:
        #     print line
        #     continue
        #写入数据库
        if count%50 == 0:
            conn.commit()
    conn.commit()

def refresh_database():
    cursor.execute('SELECT * FROM py_product_comments_tmp')
    all_results = cursor.fetchall()
    index_dict = {}
    for line in all_results:
        index_string = json.dumps({'prod_asin':line[1],'title':line[2],'content':line[3],'user_address':line[7]})
        index_dict[index_string] = line[5]

    for index_string in index_dict:
        index_json_dict = json.loads(index_string)
        content_line = (index_dict[index_string],index_json_dict['prod_asin'],index_json_dict['title'],index_json_dict['content'],index_json_dict['user_address'])
        cursor.execute('UPDATE py_product_comments SET color = %s  WHERE prod_asin = %s AND title = %s AND content = %s AND user_address = %s',content_line)
    conn.commit()

def refresh_real_database():
    cursor.execute('SELECT * FROM py_product_comments')
    all_results = cursor.fetchall()


print refresh,id(refresh)
print stringtojson,id(stringtojson)
print copy_database,id(copy_database)
print set_line_attribute,id(set_line_attribute)
print refresh_database,id(refresh_database)
print type(refresh_real_database),id(refresh_real_database)
print type(json)
print id(mydatabase)

# refresh()
# refresh_database()


    