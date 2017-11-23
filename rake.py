#coding=utf-8
from __future__ import absolute_import
import re
import operator
import six
from six.moves import range

#***********************
#  ┏┓     ┏┓
# ┏┛┻━━━━━┛┻┓
# ┃         ┃
# ┃    ━    ┃
# ┃ ┳┛   ┗┳ ┃
# ┃         ┃
# ┃    ┻    ┃
# ┃         ┃
# ┗━┓     ┏━┛
#   ┃     ┃ 神兽保佑
#   ┃     ┃ 代码永无BUG！
#   ┃     ┗━━━┓
#   ┃         ┣┓
#   ┃         ┏┛
#   ┗┓┓┏━━━┳┓┏┛
#    ┃┫┫   ┃┫┫
#    ┗┻┛   ┗┻┛
#*************************

# 这个文件是rake算法，用语快速获取英文句子中的关键词
# 请配合停止词列表一同食用
# 需要修改算法可以把debug变量改为True进行测试


debug = False
test = False

def is_number(s):
    try:
        float(s) if '.' in s else int(s)
        return True
    except ValueError:
        return False
#这个方法用来处理句子中的数字，判断浮点数和整数

def load_stop_words(stop_word_file):
    #传入停止词列表，获取没意义的词语
    stop_words = []
    for line in open(stop_word_file):
        if line.strip()[0:1] != "#":
            for word in line.split():  
            #对每一行进行拆分
                stop_words.append(word)
                #读取停止词，把停止词变成一套列表结构
    return stop_words

def separate_words(text, min_word_return_size):
    
    #这个函数返回一个列表，列表内是进行分词以后的内容
    splitter = re.compile('[^a-zA-Z0-9_\\+\\-/]')
    #用正则去匹配内容
    words = []
    for single_word in splitter.split(text):
        current_word = single_word.strip().lower()
        #注意，这里要先把内容转化成小写的
        #转化的目的是大小写什么的嘛……很讨厌……
        if len(current_word) > min_word_return_size and current_word != '' and not is_number(current_word):
            #空值就不要返回了……
            words.append(current_word)
    return words
    #这里返回的是一个list结构，每个元素分别对应一个关键词或短语


def split_sentences(text):
    sentence_delimiters = re.compile(u'[\\[\\]\n.!?,;:\t\\-\\"\\(\\)\\\'\u2019\u2013]')
    sentences = sentence_delimiters.split(text)
    return sentences


def build_stop_word_regex(stop_word_file_path):
    stop_word_list = load_stop_words(stop_word_file_path)
    stop_word_regex_list = []
    for word in stop_word_list:
        word_regex = '\\b' + word + '\\b'
        stop_word_regex_list.append(word_regex)
    stop_word_pattern = re.compile('|'.join(stop_word_regex_list), re.IGNORECASE)
    return stop_word_pattern


def generate_candidate_keywords(sentence_list, stopword_pattern, min_char_length=1, max_words_length=5):
    phrase_list = []
    for s in sentence_list:
        tmp = re.sub(stopword_pattern, '|', s.strip())
        phrases = tmp.split("|")
        for phrase in phrases:
            phrase = phrase.strip().lower()
            if phrase != "" and is_acceptable(phrase, min_char_length, max_words_length):
                phrase_list.append(phrase)
    return phrase_list


def is_acceptable(phrase, min_char_length, max_words_length):

    if len(phrase) < min_char_length:
        return 0


    words = phrase.split()
    if len(words) > max_words_length:
        return 0

    digits = 0
    alpha = 0
    for i in range(0, len(phrase)):
        if phrase[i].isdigit():
            digits += 1
        elif phrase[i].isalpha():
            alpha += 1

    
    if alpha == 0:
        return 0

    
    if digits > alpha:
        return 0
    return 1


def calculate_word_scores(phraseList):
    word_frequency = {}
    word_degree = {}
    for phrase in phraseList:
        word_list = separate_words(phrase, 0)
        word_list_length = len(word_list)
        word_list_degree = word_list_length - 1
       
        for word in word_list:
            word_frequency.setdefault(word, 0)
            word_frequency[word] += 1
            word_degree.setdefault(word, 0)
            word_degree[word] += word_list_degree  
            
    for item in word_frequency:
        word_degree[item] = word_degree[item] + word_frequency[item]

    
    word_score = {}
    for item in word_frequency:
        word_score.setdefault(item, 0)
        word_score[item] = word_degree[item] / (word_frequency[item] * 1.0)  
    
    return word_score


def generate_candidate_keyword_scores(phrase_list, word_score, min_keyword_frequency=1):
    keyword_candidates = {}

    for phrase in phrase_list:
        if min_keyword_frequency > 1:
            if phrase_list.count(phrase) < min_keyword_frequency:
                continue
        keyword_candidates.setdefault(phrase, 0)
        word_list = separate_words(phrase, 0)
        candidate_score = 0
        for word in word_list:
            candidate_score += word_score[word]
        keyword_candidates[phrase] = candidate_score
    return keyword_candidates


class Rake(object):
    def __init__(self, stop_words_path, min_char_length=1, max_words_length=5, min_keyword_frequency=1):
        self.__stop_words_path = stop_words_path
        self.__stop_words_pattern = build_stop_word_regex(stop_words_path)
        self.__min_char_length = min_char_length
        self.__max_words_length = max_words_length
        self.__min_keyword_frequency = min_keyword_frequency

    def run(self, text):
        sentence_list = split_sentences(text)

        phrase_list = generate_candidate_keywords(sentence_list, self.__stop_words_pattern, self.__min_char_length, self.__max_words_length)

        word_scores = calculate_word_scores(phrase_list)

        keyword_candidates = generate_candidate_keyword_scores(phrase_list, word_scores, self.__min_keyword_frequency)

        sorted_keywords = sorted(six.iteritems(keyword_candidates), key=operator.itemgetter(1), reverse=True)
        return sorted_keywords


def select_kw(text):
    
    sentenceList = split_sentences(text)
    
    stoppath = "SmartStoplist.txt"  
    stopwordpattern = build_stop_word_regex(stoppath)
   
    phraseList = generate_candidate_keywords(sentenceList, stopwordpattern)
   
    wordscores = calculate_word_scores(phraseList)
    
    keywordcandidates = generate_candidate_keyword_scores(phraseList, wordscores)
 
    sortedKeywords = sorted(six.iteritems(keywordcandidates), key=operator.itemgetter(1), reverse=True)
    
    totalKeywords = len(sortedKeywords)
    
    #print sortedKeywords[0:(totalKeywords // 3)]

    rake = Rake("SmartStoplist.txt")
    keywords = rake.run(text)
    #print keywords
    
    return keywords
