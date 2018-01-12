
'''公有接口模块2
为了防止互相导入而新建的一个模块
互相导入可以使用import导入解决，from...import...是是不能解决这个问题的
'''

from DLQYSpider2.cleaner import *


def clean_item(items, clean_items):
    '''清理接口
    :param items: 需要清洗的字典数据
    :param clean_items: 规则
    :return: items
    '''
    def _(k):
        yield items[k]

    for k, v in clean_items:
        if 'max_num' == k:
            continue
        stream = _(k)
        for cleaner in v['cleaner']:
            for name, attr in cleaner.items():
                cls_name = [i for i in clean_objects[1:] if eval(i).name == name]
                obj = eval(cls_name[0])()
                [setattr(obj, k2, v2) for k2, v2 in attr.items()]
                stream = obj.handle(stream)
        items[k] = next(stream)
    return items