#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
author :_seed
date : 20171102
crawler the all result from the net 
# 20171120 应该改造一下，判断期数，不用每次都爬全部
"""
import urllib2
import time
from bs4 import BeautifulSoup
import sys
import os
import random
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
reload(sys)
sys.setdefaultencoding('utf-8')

    
hostname = 'http://kaijiang.zhcw.com'
filename = 'double-color-all-results.txt'
newfilename = 'new-double-color-all-results.txt'
lastrecord = 'last-double-color.txt'
totalnums = 0
firstline = ''
currentline = ''
for x in open(filename):
    totalnums += 1
print('totlanums is : %d' %(totalnums))
totalPage = 0
_pwd  = sys.argv[1]     #邮件密码以参数形式读取进来
def get_total_pages(url):       # 获取总页数
    page = urllib2.urlopen(url)      #设置超时时间5s
    soup = BeautifulSoup(page,"html.parser")
    soup = soup.find('p','pg').find_all('strong')
#    print(soup[0].get_text())
#    print(soup[1].get_text())
    # 获取原文件行数，如果文件行数与结果记录数一致，将会退出爬取
    totalRecords = soup[1].get_text()
    print('totalRecords is : %s ' %(totalRecords))
    if int(totalnums) == int(totalRecords):     #记录数相同，不进行爬取，总页数不再进行爬取
        print("...................equals ...........")
        sys.exit(0)

    return soup[0].get_text()


def get_total_double_colors(totalPage):     #爬取所有数据下来
    base_uri = '/zhcw/html/ssq/list_'
    if os.path.exists(filename):        # 文件存在，则删除这个文件
        for line in open(filename):
            print("the  current line is : %s" %(line))
            firstline = line.split('|')[1]
            print("the firstline is : %s" %(firstline))
            break
    print("....................  萌萌的分割线  ......................")
    totalPage =  int(totalPage) + 1
    apd_data = []
#    for i in range(1,totalPage):
    for i in range(1,2):        # 开始爬取页数
        url = hostname +  base_uri +  str(i) + '.html'
        print("the crawler url is : %s " %(url))
        page = urllib2.urlopen(url)
        soup = BeautifulSoup(page,"html.parser")
        data = soup.find_all('tr')
        flag = 'f';
        for x in range(2,len(data)-1):
            y = data[x].find_all('td')
            res = y[0].get_text() + '|'
            res += y[1].get_text()
            res += y[2].get_text().replace('\n','|') + '|'
            res += y[3].get_text() + '|'
            res += y[4].get_text().split('\n')[0]
            currentline = res.replace('\r','').replace('\n','') + '\n'
            print("current line is : %s " %(currentline))
            nowline = y[1].get_text()
            print("the firstline is : %s , and the now line is : %s ; and the flag is : %s " %(firstline,nowline,flag))
            if firstline == nowline:
                flag = 't'
                print("the flag is true ? :  %s " %(flag))
                break;
            apd_data.append(currentline)
        print("once into the url .....")
        if flag == 't':
            break
    f = open(newfilename,'w+')
    for x in apd_data:
        print("data[x] is : %s" %(x))
        f.write(x)
    for y in open(filename):
        print(y)
        f.write(y)
    f.close()       # 文件一定要关闭
    os.remove(filename)     # 移除旧有的数据文件,将新文件再复制到旧文件上
    old_f = open(filename,'w+')
    for x in open(newfilename):
        old_f.write(x)
    old_f.close()
    print("xxxxxxxxxxxxxxxxxxxxx the end of the crawler ....................................")

# 取出top5的数字出来,并按排序取出6个数字出来
def count_top5():
    words = {}
    for line in open(filename):
        data1 = []
        data = line.split('|')
        print(data)
        data1.append(data[2])
        data1.append(data[3])
        data1.append(data[4])
        data1.append(data[5])
        data1.append(data[6])
        data1.append(data[7])
        for x in data1:
            words[x] = words.get(x,0) + 1
    list1 = []
    for word in sorted(words.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):  #对结果集进行按value排序，倒序
#        print(word)
        list1.append(word)

    res = []
    # the red ball
    x = random.randint(0,4)     # 随机从前4中取出1位数出来
    res.append(list1[x][0])
    x = random.randint(5,9)     # 随机从前4中取出1位数出来
    res.append(list1[x][0])
    x = random.randint(10,14)     # 随机从前4中取出1位数出来
    res.append(list1[x][0])
    x = random.randint(15,20)     # 随机从前4中取出1位数出来
    res.append(list1[x][0])
    x = random.randint(21,26)     # 随机从前4中取出1位数出来
    res.append(list1[x][0])
    x = random.randint(27,32)     # 随机从前4中取出1位数出来
    res.append(list1[x][0])

    res = sorted(res)  # 按数字大小进行排序
    # the blue ball
    x = random.randint(1,16)     # 随机从前4中取出1位数出来
    res.append(str(x))
    print(res)
    f = open(lastrecord,'w')        #记录随机出来的号码
    record = str('|'.join(res))
    f.write('|'.join(res))
    f.close()
    return res

def is_match_nums():        #判断有几个数字匹配
    print(".........")
    occurred = []
    last = []
    for x in open(filename):
        occurred = x.replace('\r','').replace('\n','').split('|')
        break
    for x in open(lastrecord):
        last = x.split('|')
        break
    print(len(last))
    content = '上一期的选的号码是 ：\n' + ','.join(last) + '\n'
    stage_1 = occurred[0:2]
    stage_2 = occurred[2:9]
    content = content + '官方的期数是 ：\n ' + ','.join(stage_1) + '\n'
    content = content + '上一期的官方的号码是 ： \n' + ','.join(stage_2) + '\n'

    blue = 0

    if last[6] == occurred[8]:
        print("the blue ball is one ball")
        content = content + '所选的蓝球中了 : ' + last[6] + '\n'
        blue = 1
    else:
        content = content + '所选的蓝球未中 : ' + last[6] + '\n'
    nums = 0
    matched = []
    for j in range(0,6):
#        print("..................................................")
        x = last[j]
#        print("x is : %d" %(int(x)))
        for k in range(2,8):
            y = occurred[k]
#            print("y is : %d" %(int(y)))
            if x == y:
#                print(x)
                nums += 1
                matched.append(x)
    print("nums is : %d " %(nums))
    content = content + '红球中了 ' + str(nums) + ' 个，分别是 ： ' + ','.join(matched) + '\n'
    now = count_top5()        # 按出现次数进行排序，随机抽出一组数据出来,并记录该组数据进行是否匹配下一期出现的
    content = content + '下一期的选的号码是 ：\n ' + '|'.join(now)
    print("..................................................")
    print(content)
    title = '没有中奖'
    if blue == 1 or nums > 3:
        title = '中奖了'

    SendEmail("250239675@qq.com",'',content,title)    # 将计算结果发送出来
    
# 使用ssl验证方式开通手机认证之后可以使用第三方登录 -seed
# 使用email将结果发送出来
def SendEmail(fromAdd, toAdd,content,title):
    print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 进入邮件发送 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    toAdd = ','.join(['250239675@qq.com'])
    # 如名字所示： Multipart就是多个部分
    msg = MIMEMultipart()
    msg["Subject"] = title
    msg["From"]    = fromAdd
    msg["To"]      = toAdd

    # 下面是文字部分，也就是纯文本
    puretext = MIMEText(content)
    msg.attach(puretext)

    # 下面是附件部分 ，这里分为了好几个类型

    # 首先是xlsx类型的附件
    xlsxpart = MIMEApplication(open('beautifulsoup-doublecolor.py', 'rb').read())
    xlsxpart.add_header('Content-Disposition', 'attachment', filename='beautifulsoup-doublecolor.py')
    msg.attach(xlsxpart)

    xlsxpart = MIMEApplication(open('double-color-all-results.txt', 'rb').read())
    xlsxpart.add_header('Content-Disposition', 'attachment', filename='double-color-all-results.txt')
    msg.attach(xlsxpart)

    try:
        s = smtplib.SMTP_SSL("smtp.qq.com", 465)
        s.login(fromAdd, _pwd)
        s.sendmail(fromAdd,toAdd, msg.as_string())
        s.quit()
        print "Success!"

    except smtplib.Exception,e:
        print e.message

if __name__ == "__main__":
    print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 程序开始 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    time.clock()
    #获得当前时间时间戳
    now = int(time.time())  # 这是时间戳
    #转换为其他日期格式,如:"%Y-%m-%d %H:%M:%S"
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    print(otherStyleTime)
    uri = "/zhcw/html/ssq/list.html"
    url = hostname + uri

    totalPage = get_total_pages(url)    # 获取总页数
    get_total_double_colors(totalPage)  # 爬取所有记录
    is_match_nums()         #计算结果是否匹配
    print('程序执行结束，耗时： %s ' %(str(time.clock())))

