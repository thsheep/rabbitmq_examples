#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 18-1-10 下午1:37
# @Author  : 哎哟卧槽
# @Site    : 
# @File    : queuecount.py
# @Software: PyCharm


import time
import threading
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

count_time = 5


class MessagesCount(threading.Thread):
    """
    MQ的心跳线程
    """
    def __init__(self, connection):
        """
        :param connection: RabbitMQ的连接对象
        """
        super(MessagesCount, self).__init__()
        self.lock = threading.Lock()
        self.connection = connection
        self.quit_flag = False
        self.stop_flag = True
        self.setDaemon(True)

    def run(self):
        logger.warning("消息总数线程准备完毕")
        while not self.quit_flag:
            time.sleep(count_time)
            self.lock.acquire()
            if self.stop_flag:
                self.lock.release()
                continue
            try:
                logger.warning(f"获取消息总数,间隔时间为:{count_time}s")
                channel = self.connection.channel()
                messages = channel.queue_declare(queue="task_root_queue",
                                                 passive=True)
                print(messages)
            except Exception as ex:
                self.lock.release()
                raise RuntimeError("错误格式: %s" % (str(ex)))
            self.lock.release()

    def start_count(self):
        self.lock.acquire()
        if self.quit_flag:
            self.lock.release()
            return
        self.stop_flag = False
        self.lock.release()