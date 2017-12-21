#!/usr/bin/env python
# -*- coding:utf8 -*-

"""
@version: ??
@author: lwd
@license: Apache Licence 
@contact: scuseedman@163.com
@site: http://www.alaxigaodi.com
@software: PyCharm
@file: BeautifulBlog.py
@time: 2017/12/12 11:39

# code is far away from bugs with the god animal protecting
    I love animals. They taste delicious.
              ┏┓      ┏┓
            ┏┛┻━━━┛┻┓
            ┃      ☃      ┃
            ┃  ┳┛  ┗┳  ┃
            ┃      ┻      ┃
            ┗━┓      ┏━┛
                ┃      ┗━━━┓
                ┃  神兽无影    ┣┓
                ┃　BUG无踪！   ┏┛
                ┗┓┓┏━┳┓┏┛
                  ┃┫┫  ┃┫┫
                  ┗┻┛  ┗┻┛
"""
import urllib
import urllib2
import time
import webbrowser as web
import re


def beautifulsoupblog(url):
    print("...................................")
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'  # 将user_agent写入头信息
    values = {'Content-Encoding': 'gzip',
              'Content-Type': 'text/html',
              'Pragma': 'no-cache',
              'Transfer-Encoding':'chunked',
              'Accept-Language':'zh-CN,zh;q=0.8',
              'Referer':'http://blog.sina.com.cn/s/blog_6dd718930102x8pz.htm',
              'Upgrade-Insecure-Requests':'1',
              'Connection':'keep-alive',
              'Set-Cookie':'mblog_userinfo=uid%3D1842813075%26nick%3D; expires=Wed, 13-Dec-2017 06:15:14 GMT; path=/; domain=.blog.sina.com.cn',
              'Set-Cookie':'mblog_userinfo=uid%3D1842813075%26nick%3D%E6%92%AD%E7%A7%8D%E8%80%85; expires=Wed, 13-Dec-2017 06:15:12 GMT; path=/; domain=.blog.sina.com.cn',
              'SINA-LB':'aGwuOTAuc2c1LmphLmxiLnNpbmFub2RlLmNvbQ=='}
    headers = {'User-Agent': user_agent}
    data = urllib.urlencode(values)
    req = urllib2.Request(url, data, headers)
    response = urllib2.urlopen(req)
    the_page = response.read()


    return the_page


if __name__ == '__main__':
    url = "http://blog.sina.com.cn/s/blog_6dd718930102x8pz.htm"
    nums = 0
    while nums < 100:
        html_doc = beautifulsoupblog(url)
        print(html_doc)
        nums += 1
        time.sleep(3)
        print(time.localtime())
        print("sleep over ......")
