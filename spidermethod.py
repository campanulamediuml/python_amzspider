#coding=utf-8
import urllib2
from bs4 import BeautifulSoup
import time
import user_agents
import random
import sys
import os
import config
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 
import csv
import json


def get_htmlsoup(site):
    randomarry = random.choice(user_agents.user_agent_list)
#随机挑选一个user_agent文件头
    headers = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.6',
    'X-Requested-With':'XMLHttpRequest',
    'User-Agent':randomarry
    }
#手动添加完整一套文件头，假装是不同的浏览器进行访问
    data = None
    requests = urllib2.Request(site,data,headers)
    try:
        response = urllib2.urlopen(requests,timeout=30)
        site_page = response.read()
        soup = BeautifulSoup(site_page, 'html.parser')
    except urllib2.HTTPError,e:
        if e.code == 404:
            soup = 0
    return soup
#获取经过beautifusoup处理过后的结果
def monthexchange(string):
    month = 0
    if string == 'January':
        month = '01'
    elif string == 'February':
        month = '02'
    elif string == 'March':
        month = '03'
    elif string == 'April':
        month = '04'
    elif string == 'May':
        month = '05'
    elif string == 'June':
        month = '06'
    elif string == 'July':
        month = '07'
    elif string == 'August':
        month = '08'
    elif string == 'September':
        month = '09'
    elif string == 'October':
        month = '10'
    elif string == 'November':
        month = '11'
    elif string == 'December':
        month = '12'
    return month

def timeexchange(time):
    try:
        time = time.split()
        time = time[1:] #去掉第一个单词on
        year = time[-1] 
        time = time[0:2] 
        time = [year] + time #把年份移到最前面
        time[2] = time[2].replace(',','')#替换掉逗号
        if len(time[2]) == 1:
            time[2] = '0' + time[2]
        time[1] = monthexchange(str(time[1])) #把月份变成数字
        time = '-'.join(time)#组成格式
    except:
        time = 'N/A'
    return time
#对时间进行格式化处理
def get_author(html):
    try:
        comment_author = html.find(class_="a-size-base a-link-normal author")
        comment_author = comment_author.get_text()
    except:
        comment_author = 'N/A'
    return comment_author.encode('utf-8')
#获取评论作者的信息

def get_title(html):
    try:
        comment_title = html.find(class_="a-size-base a-link-normal review-title a-color-base a-text-bold")
        comment_title = comment_title.get_text()
    except:
        comment_title = 'N/A'
    return comment_title.encode('utf-8')
#获取评论标题

def get_content(html):
    try:
        comment_text = html.find(class_="a-size-base review-text")
        comment_text = comment_text.get_text()
    except:
        comment_text = 'N/A'
    return comment_text.encode('utf-8')
#获取评论内容
def get_userid(html):
    try:
        user_id = html.find(class_="a-size-base a-link-normal author")
        user_id = user_id['href']
    except:
        user_id = 'N/A'
    return user_id.encode('utf-8')
#获取评论作者的对应id主页
def get_stars(html):
    try:
        icon_alt = html.find(class_="a-icon-alt")
        icon_alt = icon_alt.get_text()
        icon_alt = str(icon_alt)[0]
    except:
        icon_alt = 'N/A' 
    return icon_alt.encode('utf-8')
#获取评论星级
def get_comment_date(html):
    try:
        comment_date = html.find(class_="a-size-base a-color-secondary review-date")
        comment_date = comment_date.get_text()
        comment_date = timeexchange(comment_date)
    except:
        comment_date = 'N/A'
    return comment_date.encode('utf-8')
#获取评论发布日期并进行格式化
def get_item_type(html):
    try:
        item_type = html.find(class_="a-size-mini a-link-normal a-color-secondary")
        item_type = item_type.get_text()
    except:
        item_type = 'N/A'
    return item_type
#获取该商品的规格型号
def get_item_number(html):
    try:
        item_number = html.find(class_="a-size-mini a-link-normal a-color-secondary")
        #通过class项目抓取到项目的细分asin码
        item_number = item_number['href']
        #从带有asin码的标签中，抓取href这个网址
        item_number = item_number.split('/')
        #对网址进行根据"/"的切片，获取真实的细分asin内容
        item_number = item_number[3]
    except:
        item_number = 'N/A'
    return item_number
#获取细分下的编号
def get_vote(html):
    try:
        vote = html.find(class_="review-votes")
        vote = vote.get_text()
        vote = (vote.split())[0]
        #对投票的字符串进行切片，留下第一个元素（投票数量）
        if 'One' in vote:
            vote = str(1)
        vote = filter(str.isdigit,str(vote))
        #如果是投票
    except:
        vote = 'N/A' 
    return vote.encode('utf-8')
#获取评论票数
def get_comment_data(html):
    comment = []
    comment.append(get_title(html))
    
    comment.append(get_content(html))
    
    comment.append(get_author(html))
   
    try:
        comment.append(get_item_type(html))
    except:
        comment.append('N/A')
  
    try:   
        comment.append(get_item_number(html))
    except:
        comment.append('N/A')
   
    comment.append(get_userid(html))

    try:
        comment.append(get_vote(html))
    except:
        comment.append(str(0))
 
    comment.append(get_stars(html))
   
    comment.append(get_comment_date(html))
    
    return comment
#连接成一整条完整的评论列表 
def refresh(count_list,list_array,filename):
    tmp_list = count_list[list_array:]
    config=open(str(filename)+'asid_list.txt','w')
    config.write('')
    config=open(str(filename)+'asid_list.txt','a')
    for i in tmp_list:
        config.write(i)
#写入缓存文本文件
def reset_list(comment_list):
    count = 0
    result =[]
    for comment in comment_list:
        count = count+1
        if comment not in result:
            result.append(comment)
        else:
            continue
    return result
#去除重复项
def get_page_range(asid):
    print 'try to get page range...'
    url = 'https://www.amazon.com/product-reviews/'+(str(asid).strip())+'/ref=cm_cr_arp_d_paging_btm_1?&sortBy=recent&pageNumber=1'
    page_content = get_htmlsoup(url)
    if page_content == 0:
        page_range = 0

    else:
        print 'connect successful'
        try:
            page_content = page_content.find(class_="a-pagination")
            page_range = page_content.find_all(class_="page-button")#通过按钮上的数字查找到整个评论究竟有多少页 
            
            page_range = page_range[-1]
            page_range = str(page_range.get_text())
            page_range = filter(str.isdigit,page_range)

            print page_range
        except:
            page_range = 1
    print asid.strip(),page_range
    return page_range
#获取页面链接

def cycle_get_page_range(asid):
    count = 0
    while 1:
        page_range = get_page_range(asid)
        try:
            page_range = int(page_range)
            break
        except:
            count = count+1
        if count == 10:
            page_range = 1
            break
    #反复获取总页面数量，获取成功就跳出，获取失败就重新获得，十次失败以后就视为只有一页
    print 'get page_range successful'
    print 'page = '+str(page_range)
    return page_range

def spider_page_logic(asid):
    comment_count = 0
    asin_comment = []
    page_range = cycle_get_page_range(asid)
    page_url = []
    for page in range(1,page_range+1):
        url = 'https://www.amazon.com/product-reviews/'+str(asid.strip())+'/ref=cm_cr_arp_d_paging_btm_'+str(page)+'?&sortBy=recent&pageNumber='+str(page)
        page_url.append(url)
    
    pool = ThreadPool(80)
    results = pool.map(get_item_attribute, page_url)
    # pool.join()
    pool.close()
    print 'get '+asid.strip()+'finish'
    asin_comment = []
    for i in results:
        if i == []:
            continue
        else:
            asin_comment.extend(i)

    #print asin_comment[0]
    return asin_comment
    #每当抓取完一件商品（按照asin区分）后，就立刻写入缓存的txt中    

def write_result_by_asid(asid,address,filename,asin_comment):
    headers = ['prod_asin','title','content','user_name','color','type_call','user_address','vote','prod_star','create_date','prod_website','prod_group_number','good_type']

    fh = open(address+'/'+str(filename)+'result_comment.txt','a')
    
    count = 0

    for line in asin_comment:
        rows = {}

        line.extend([config.pro_website,asid,config.good_type])
        for name in headers:
            rows[name] = (line[headers.index(name)]).strip()
            
        json_content = json.dumps(rows)
        fh.write(json_content)
        count+=1
        fh.write('\n')

    print '抓取完'+str(asid.strip())+'抓取到'+str(count)+'条,开始抓取下一件商品'
    #每当抓取完一件商品，就把内容写入到txt文件中


def get_item_attribute(url):
    asin_comment = []
    page_html = get_htmlsoup(url)
    if page_html == 0:
        asin_comment = [[]]
    else:
        url = url.split('/')
        asid = url[4]
        page = (url[-1]).split('=')
        page = page[-1]
        review_list = page_html.find_all(class_="a-section review")
        #从html代码中获得所有评论的html块，储存成一个列表
        for comment in review_list:
            comment_data = [asid]
            comment_data.extend(get_comment_data(comment))
            
        #对列表中的每一块，依次获取到全部评论细节，组成列表储存起来
            asin_comment.append(comment_data)
        print '     '+asid+' '+str(page)
        #time.sleep(random.uniform(1,3))
    #print asin_comment[3]
    return asin_comment
    







