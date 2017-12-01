import random


def cal_sell(comment_count):

    tmp = comment_count
    count = 0
    while 1:
        comment_count = comment_count/10
        if comment_count == 0:
            break
        count+=1
    express = 10 ** count
    fix = random.randint((tmp/(-10**count)),(tmp/(10**count)))
    #express_2 = comment/express
    result = (((tmp**2)/(express))+fix)*3

    return result

# print cal_sell(0)
