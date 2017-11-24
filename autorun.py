#coding=utf-8
import time
import os
import time
import config
import sys
sys.path.append("..")

try:
    fh = open('result/result_comment.txt','r')
    fh.close()
except Exception,e:
    print e

count = 0
while True:
    fh = open('asid_list.txt')
    answer = fh.readline()
    if len(answer) == 0:
        os.system('python get_asin_list.py')
        count+=1
        if count == 10:
            break
    else:
        os.system('python commentspider.py')

os.system('python intodb.py')



