import MySQLdb as mydatabase
import db

def main():
    conn = mydatabase.connect(host=db.host, port=db.port, user=db.user, passwd=db.passwd, db=db.db, charset=db.charset)
    cursor = conn.cursor()

    cursor.execute('SELECT prod_asin,prod_star FROM py_product_comments')
    comment_list = cursor.fetchall()

    comment_neg_dict = {}

    for line in comment_list:
        if int(line[1]) < 4:
            if line[0] not in comment_neg_dict:
                comment_neg_dict[line[0]] = 1
            else:
                comment_neg_dict[line[0]] += 1

    sql_1 = 'UPDATE py_product_main SET pro_negativeNumber = %s WHERE pro_asin = %s' 
    sql_2 = 'UPDATE py_product_main SET syn_status = 2 WHERE pro_asin = %s' 
    for key in comment_neg_dict:
        cursor.execute(sql_1,(comment_neg_dict[key],key))
        cursor.execute(sql_2,(key,))
    conn.commit()

