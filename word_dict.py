import MySQLdb as mydatabase

conn = mydatabase.connect(host='117.25.155.149', port=3306, user='gelinroot', passwd='glt#789A', db='db_dolphin', charset='utf8')
cursor = conn.cursor()

print 'connect successful'
cursor.execute('DELETE FROM py_keyword_main_tmp')
print 'delete successful'
cursor.execute('DELETE FROM py_keyword_main')
print 'delete successful'
cursor.execute('DELETE FROM py_keyword_word_count')
print 'successful'




# sql_1 = 'CREATE TABLE py_keyword_main_tmp_JP (id INT(11)primary key auto_increment ,express VARCHAR(200),score TEXT(100),comment_id INT(10),comment TEXT(100000),pos_or_neg VARCHAR(10),good_type VARCHAR(100),express_id INT(10))'
# sql_2 = 'CREATE TABLE py_keyword_count_JP (id INT(11)primary key auto_increment ,express_with_score VARCHAR(200),count_all INT(100),count_pos INT(100),count_neg INT(100),count_mid INT(100),good_type VARCHAR(100),express_id INT(100))'
# sql_3 = 'CREATE TABLE py_keyword_word_count_JP (id INT(11)primary key auto_increment ,express_without_score VARCHAR(200),word_type VARCHAR(10),count_all INT(100),count_pos INT(100),count_neg INT(100),count_mid INT(100),good_type VARCHAR(100),express_id INT(100))'
# sql_4 = 'CREATE TABLE py_keyword_main_JP (id INT(11)primary key auto_increment ,express VARCHAR(200),score TEXT(100),comment_id INT(10),comment TEXT(100000),pos_or_neg VARCHAR(10),good_type VARCHAR(100),express_id INT(10),prod_asin VARCHAR(20))'


# try:
#     cursor.execute(sql_1)
   
# except:
#     print 'table is alredy exist' 

# try: 
#     cursor.execute(sql_2)

# except:
#     print 'table is alredy exist' 

# try:   
#     cursor.execute(sql_3)
   
# except:
#     print 'table is alredy exist' 

# try:   
#     cursor.execute(sql_4)
   
# except:
#     print 'table is alredy exist' 

# conn.commit()