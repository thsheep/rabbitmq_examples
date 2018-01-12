
'''公有接口模块'''

import re
import time
import json
from datetime import date, datetime
from collections import Iterable
from json.decoder import JSONDecodeError
from urllib.parse import urljoin, urlparse, urlunparse
from posixpath import normpath
from lxml import etree
from lxml.etree import XPathEvalError


CHAR_SET = re.compile(r"<meta.+?charset=[^\w]?([-\w]+)")
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def create_tree(html):
    '''
    :param html:HTML或者XML字符串
    :return:tree树形结构对象
    '''
    try:
        root = etree.HTML(html)
    except ValueError:
        root = etree.XML(html)
    except Exception:
        return None
    tree = etree.ElementTree(root)
    return tree


def read_file(file_name):
    '''
    :param file_name:文件路径
    :return:文件内容
    :except:空字符串
    :attention:不适用于中大文件
    '''
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            lines = f.read()
    except IOError:
        return ''
    except UnicodeDecodeError:
        return ''
    return lines


def load_json(string):
    '''将Json转换为字典
    :param string:字符串
    :return:字典
    '''
    try:
        res = json.loads(string)
    except JSONDecodeError:
        return {}
    return res


def re_findall(pattern, string, flags=0):
    """
    :param pattern: 正则表达式
    :param string: 需要匹配的字符串
    :param flags:
    :return: 与结果相匹配的列表
    """
    string = re.findall(pattern, string, flags)
    return string or ''


def dump_json(dict_):
    '''将字典转换为Json
    :param dict_:字典
    :return:Json
    '''
    try:
        res = json.dumps(dict_)
    except RuntimeError:
        return {}
    return res


def url_join(base, url):
    '''拼接url
    :param base:需要被拼接的url
    :param url:拼接url
    :return:新url
    '''
    url1 = urljoin(base, url)
    arr = urlparse(url1)
    path = normpath(arr[2])
    result = urlunparse((arr.scheme, arr.netloc, path, arr.params, arr.query, arr.fragment))
    if base in result:
        return result


def date_format(date_time, str_format):
    '''
    :param date_time:时间字符串
    :param str_format:时间字符串对应的原始格式
    :return:%Y-%m-%d %H:%M:%S固定格式
    '''
    time_array = time.strptime(date_time, str_format)
    if time_array.tm_year in (1900, 1970):  # 如果时间年份包含1900 1970 则将年份改为当前年份
        tody = date.today()
        new_format = tody.replace(month=time_array.tm_mon,
                                  day=time_array.tm_mday)
        return "{} 00:00".format(new_format)
    if time.time() < time.mktime(time_array):  # 如果时间戳大于当前时间戳则改为当前时间
        time_array = time.gmtime()
    new_format = time.strftime(DATE_FORMAT, time_array)
    return new_format


def timestamp_format(time_stamp, template=DATE_FORMAT):
    '''将时间戳转换为指定格式时间
    :param time_stamp: 时间戳
    :param tempalte: 模板默认选择DATE_FORMAT
    :return: 格式化好的时间戳
    '''
    x = time.localtime(time_stamp)
    new_format = time.strftime(template, x)
    return new_format


def get_data_by_xpath(tree, xpaths):
    '''
    :param tree:DOM文档树
    :param xpaths:xpath列表
    :return:字符串结果或者空字符串
    '''
    if not isinstance(xpaths, Iterable):
        return xpaths
    for xpath in xpaths:
        try:
            result = tree.xpath(xpath)
        except (XPathEvalError, ValueError):
            continue
        result = result.extract()
        test = [i.strip() for i in result]
        test = [i for i in test if i]
        if test:
            return result
    else:
        return ''
