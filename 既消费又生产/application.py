import time
import copy
import exceptions
import logging
import setproctitle
from pika.exceptions import ConnectionClosed
from config import rabbitmq_conf
from common.rabbitmq.rabbitmq import MessageHandlerFactory, IMessageCallBack
from common.public import load_json, dump_json
from frame_choice import spider_run, status_statistics
from config import app_conf
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
setproctitle.setproctitle("Example")


class TaskHandler(IMessageCallBack):

    def __init__(self):
        '''初始化RabbitMQ'''
        self.task_handler = None
        self.data_handler = None
        self.create_task_handler()
        self.create_data_handler()

    @staticmethod
    def init_handler(conf):
        '''Handler创建细节
        :param conf: handler创建配置
        :return: handler
        '''
        def _(obj):
            try:
                return MessageHandlerFactory.get_instance(message_class, connect_params, obj)
            except ConnectionClosed:
                raise exceptions.SpiderRabbitMQConnectionError
        message_class = rabbitmq_conf['message_class']
        connect_params = rabbitmq_conf['connect_params']
        return _(conf)

    def create_data_handler(self):
        '''创建data_handler'''
        data = copy.deepcopy(rabbitmq_conf["consuming_queues"])
        data['queue_name'] = app_conf['data_name']
        self.data_handler = self.init_handler(data)

    def create_task_handler(self):
        '''创建task_handler'''
        receive = copy.deepcopy(rabbitmq_conf["consuming_queues"])
        receive['queue_name'] = app_conf['task_root_name']
        self.task_handler = self.init_handler(receive)

    def start(self):
        '''开始监听'''
        logger.warning('爬虫启动！')
        self.task_handler.start_consuming(self)

    def stop(self):
        logger.warning("停止RabbitMQ")
        self.task_handler.close_channel()
        self.task_handler.close()

    def callback(self, ch, method, properties, body):
        '''从消息队列获取任务
        :param ch:
        :param method:
        :param properties:
        :param body:任务格式
        :return:None
        '''
        logger.warning("收到任务")
        task = load_json(body.decode())
        if not isinstance(task, dict) or not task:
            logger.error('任务格式解析错误！')
            logger.error(body)
            return True
        boolean = 任务的入口函数(task, self)
        return boolean


if __name__ == '__main__':
    task = TaskHandler()
    try:
        task.start()
    except KeyboardInterrupt:
        task.stop()
