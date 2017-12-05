#coding=utf-8
import time
import os
import time
import config
import get_asin_list
import commentspider
import intodb

def main():
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
            get_asin_list.main()
            count+=1
            if count == config.page_count:
                break
        else:
            commentspider.main()
            print 'ringfinished...'

    #intodb.main()



