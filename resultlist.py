#coding=utf-8
import urllib2
from bs4 import BeautifulSoup
import random
import user_agents
import datetime 
import spidermethod
import config

def getasid(url,page):
    while True:
        page_html = spidermethod.get_htmlsoup(url)
        asid_list = []
        for result_ in range(((page-1)*48)-10,(page*48)+10):
            try:
                page = page_html.find(id="result_"+str(result_))
                asid = str(page.get('data-asin'))
                asid_list.append(asid)
            except:
                continue
        if len(asid_list) == 51:
            break
    return asid_list


keyword = raw_input('input a keyword: ')
asids = []
page_range = 1000


print '开始获取搜索页信息。。。'
for mainpage in range(1,page_range+1):
    url = 'https://www.amazon.com/s/ref=sr_pg_2?fst=p90x%3A1%2Cas%3Aon&rh=n%3A7141123011%2Ck%3A'+str(keyword.strip())+'&page='+str(mainpage)
    
    product_list = getasid(url,mainpage)
    asids.extend(product_list)
    print 'get page '+str(mainpage)
    print len(asids)

fh=open(keyword+'.txt','w')
fh.write('')

fh=open(keyword+'.txt','a')
for i in asids:
    fh.write(i+'\n')

fh.close()

print  'search main page finished, get '+str(len(asids))+' items.'

