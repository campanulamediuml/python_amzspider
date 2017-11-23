#coding=utf-8
import urllib2
from bs4 import BeautifulSoup
import spidermethod
import time
import random
import sys, time
import config
import os
import config
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
#import dbmethod
#import MySQLdb as database

def run_spider(filename):
    dir_name = config.dir_name
    try:
        os.mkdir(dir_name)
    except:
        print 'dir exsists'
    #以当天日期为文件夹名称，创建文件夹
    fh = open(str(filename)+'asid_list.txt')
    asinlist = fh.readlines()

    print '获取目录完毕，开始获取评论...'
    comment_count = 0
    count_down = 0
    tmp_list = asinlist 
    #在这里写一个缓存的列表
    
    for asid in asinlist:
        try:
            asin_comment = spidermethod.spider_page_logic(asid)
            #调用文件名为spidermethod的对象，通过其中的方法获取asin的评论列表
            spidermethod.write_result_by_asid(asid,dir_name,filename,asin_comment)
            #把上一步获取到的结果写进文件夹中
            count_down = count_down + 1
            #这是一个计算次数的设备，通过计数统计成功写入了多少条信息
            spidermethod.refresh(asinlist,count_down,filename)
            #更新原始目录的asid表格
            time.sleep(random.uniform(1,3))
        except Exception,e:
            print e
            count_down = count_down + 1
            #这是一个计算次数的设备，通过计数统计成功写入了多少条信息
            spidermethod.refresh(asinlist,count_down,filename)
            continue
    #更新asid列表

    
    count = 0
    fh.close()
    # rst=open(str(filename)+'asid_list.txt','w')
    # rst.write('')
    # rst.close()
    print '总共抓取到'+str(comment_count)+'条'

def main():
    # pool_lenth = len(config.keyword)
    # pool = ThreadPool(pool_lenth)
    filename = config.keyword 
    for i in filename:
        run_spider(i)
    #单线程处理
    #创建多线程一起开始抓，每个线程就是每个文件目录，但是具体到每个目录下的项目依旧是单线程
    # results = pool.map(run_spider, filename)
    # pool.close() 
    # pool.join() 

main()
