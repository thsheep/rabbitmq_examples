
'''
爬虫自定义异常
'''


class SpiderError(Exception):

    def __str__(self):
        return '爬虫发生了未知的异常！'


class SpiderRabbitMQConnectionError(SpiderError):

    def __str__(self):
        return 'RabbitMQ连接异常！'


if __name__ == '__main__':
    raise SpiderRabbitMQConnectionError

