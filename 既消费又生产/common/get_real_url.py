
'''
获得搜索引擎搜出来的结果的真实URL
'''

import re
import requests
from urllib.parse import unquote
from abc import ABCMeta, abstractmethod
class_name_list = []


def set_class_name_list(cls_name):
    class_name_list.append(cls_name)
    return class_name_list


class SearchResultBase(metaclass=ABCMeta):

    def __init__(self, name):
        '''
        :param name:搜索引擎的名字
        '''
        self.name = name

    @abstractmethod
    def process(self, url):
        return


@set_class_name_list
class Sougou(SearchResultBase):
    '''搜狗'''

    NAME = '搜狗'
    pattern2 = re.compile(r'URL=\'(.*)\'')
    pattern1 = re.compile('(http|https)://.*link\?(m|url)=.*.((s)?html)?')

    def process(self, url):
        if not url:
            return ''

        m1 = self.pattern1.search(url)
        if m1:
            response = requests.get(m1.group())
            if response.status_code == 200:
                m2 = self.pattern2.search(response.text)
                if m2:
                    new_url = m2.group(1)
                    new_url = unquote(new_url)
                    return new_url
                else:
                    return url


@set_class_name_list
class Baidu(SearchResultBase):
    '''百度'''
    NAME = '百度'
    pattern = re.compile(r'URL=\'(.*)\'')
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0"
    }

    def process(self, url):
        if not url:
            return ''
        response = requests.get(url, allow_redirects=False, headers=self.headers)
        if response.status_code == 200:
            m = self.pattern.search(response.text)
            return m.group(1) if m else url
        elif response.status_code == 302:
            url = response.headers.get('Location', '')
            return url
        else:
            return None


@set_class_name_list
class Haosou(SearchResultBase):
    '''好搜'''
    NAME = '好搜'

    pattern2 = re.compile(r'URL=\'(.*)\'')
    pattern1 = re.compile('(http|https)://.*link\?(m|url)=.*.((s)?html)?')

    def process(self, url):
        if not url:
            return ''

        m1 = self.pattern1.search(url)
        if m1:
            response = requests.get(m1.group())
            if response.status_code == 200:
                m2 = self.pattern2.search(response.text)
                if m2:
                    new_url = m2.group(1)
                    new_url = unquote(new_url)
                    return new_url
                else:
                    return url


def search_init(name):
    '''搜索引擎初始化'''
    for i in class_name_list:
        if name == i.NAME:
            return i(name)
