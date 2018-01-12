#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 18-1-6 下午2:31
# @Author  : 哎哟卧槽
# @Site    : rabbitMQ心跳线程
# @File    : rabbitmqheartbeat.py
# @Software: PyCharm
import sys
import time
import threading
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

heartbeat = 50


class RabbitMQHeartbeat(threading.Thread):
    """
    MQ的心跳线程
    """
    def __init__(self, connection):
        """
        :param connection: RabbitMQ的连接对象
        """
        super(RabbitMQHeartbeat, self).__init__()
        self.lock = threading.Lock()
        self.connection = connection
        self.quit_flag = False
        self.stop_flag = True
        self.setDaemon(True)

    def run(self):
        logger.warning("心跳线程准备完毕")
        while not self.quit_flag:
            time.sleep(heartbeat)
            self.lock.acquire()
            if self.stop_flag:
                self.lock.release()
                continue
            try:
                logger.warning(f"发送心跳,间隔时间为:{heartbeat}s")
                self.connection.process_data_events()
            except Exception as ex:
                self.lock.release()
                raise RuntimeError("错误格式: %s" % (str(ex)))
            self.lock.release()

    def start_heartbeat(self):
        self.lock.acquire()
        if self.quit_flag:
            self.lock.release()
            return
        self.stop_flag = False
        self.lock.release()