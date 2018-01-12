#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 18-1-6 上午10:21
# @Author  : 哎哟卧槽
# @Site    : 
# @File    : publisher.py
# @Software: PyCharm

import logging
import pika
import json

LOG_FORMAT = ('%(levelname) -10s %(asctime)s'
                '%(message)s')
LOGGER = logging.getLogger(__name__)


class ExamplePublisher(object):
    """这是一个生产者示例，它将处理与RabbitMQ的意外交互，如通道和连接关闭。

    如果RabbitMQ关闭连接，它将重新打开它。 您应该查看输出，因为连接可能被关闭的原因有限，
    通常与权限相关的问题或套接字超时有关。

    它使用交付确认，并说明跟踪已发送的邮件以及RabbitMQ是否已确认的邮件。

    """
    EXCHANGE = 'message'
    EXCHANGE_TYPE = 'fanout'
    PUBLISH_INTERVAL = 10
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self, amqp_url):
        """设置示例发布者对象，传入我们将用于连接到RabbitMQ的URL。

        :param str amqp_url: 连接URL

        """
        self._connection = None
        self._channel = None

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False
        self._url = amqp_url

    def connect(self):
        """这个方法连接到RabbitMQ，返回连接句柄。
            连接建立后，pika会调用on_connection_open方法。
            如果要重新连接，请确保将stop_ioloop_on_close设置为False，这不是此适配器的默认行为。

        :rtype: pika.SelectConnection

        """
        LOGGER.info('连接到 %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     on_open_callback=self.on_connection_open,
                                     on_close_callback=self.on_connection_closed,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """一旦连接到RabbitMQ，这个方法就被pika调用。
        如果需要的话，它将句柄传递给连接对象，但是在这种情况下，我们将其标记为未使用。

        :type unused_connection: pika.SelectConnection

        """
        LOGGER.info('链接打开')
        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        """当与RabbitMQ的连接意外关闭时，此方法由pika调用。
        由于意外断开连接，我们将重新连接到RabbitMQ.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self._connection.ioloop.stop)

    def open_channel(self):
        """这个方法将通过发布Channel来与RabbitMQ打开一个新的频道。

            打开RPC命令。 当RabbitMQ通过发送频道确认频道是开放的。

            OpenOK RPC回复，on_channel_open方法将被调用。
        """
        LOGGER.info('创建一个新信道')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """频道打开后，此方法由pika调用。
            通道对象被传入，所以我们可以使用它。

        由于频道现在开放，我们将设置交换机。

        :param pika.channel.Channel channel: 信道对象

        """
        LOGGER.info('信道已打开')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """如果RabbitMQ意外关闭通道，这个方法告诉pika调用on_channel_closed方法。

        """
        LOGGER.info('添加信道关闭回调')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """当RabbitMQ意外关闭频道时，由pika调用。
            如果您尝试执行违反协议的操作，例如重新声明交换或使用不同参数的队列，通常会关闭通道.
             在这种情况下，我们将关闭连接来关闭对象。

        :param pika.channel.Channel channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.warning('信道已关闭: (%s) %s', reply_code, reply_text)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        """通过调用Exchange在RabbitMQ上设置交换机。
            声明RPC命令。 完成后，pika将调用on_exchange_declareok方法。

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('申明交换机 %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        """当RabbitMQ完成交换机时，由pika调用。
            声明RPC命令。

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('交换机已申明')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """通过调用Queue在RabbitMQ上设置队列。
            声明RPC命令。 完成后，pika将调用on_queue_declareok方法。

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('申明队列 %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        """当在setup_queue中完成的Queue.Declare RPC调用完成时，由pika调用的方法。
            在这个方法中，我们将通过发出Queue.Bind RPC命令将队列和交换机绑定在一起。
           当这个命令完成后，pika会调用on_bindok方法。

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info('绑定交换机: %s 到队列: %s 使用路由键: %s',
                    self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def on_bindok(self, unused_frame):
        """这个方法在接收队列时由pika调用。
            来自RabbitMQ的BindOk响应。
            既然我们知道我们现在已经安装好了，现在是开始发布的时候了。"""
        LOGGER.info('队列已绑定')
        self.start_publishing()

    def start_publishing(self):
        """此方法将启用传送确认并安排将第一条消息发送到RabbitMQ

        """
        LOGGER.info('发出消费者相关的RPC命令')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        """将Confirm.Select RPC方法发送到RabbitMQ以在通道上启用传送确认。
            关闭这个通道的唯一方法是创建一个新通道。

        当消息从RabbitMQ得到确认时，将会调用on_delivery_confirmation方法，
        传入一个来自RabbitMQ的Basic.Ack或Basic.Nack方法，这个方法将指示哪些消息确认或拒绝.

        """
        LOGGER.info('发出Confirm.Select RPC命令')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """当RabbitMQ响应Basic时，由pika调用。
            发布RPC命令，传递一个Basic.Ack或Basic.Nack帧与已发布消息的传递标记

            交付标记是一个整数计数器，指示通过Basic.Publish在通道上发送的消息编号。

            在这里，我们只是做家务，跟踪统计信息，并删除消息号码，

            我们希望从用于跟踪正在等待确认的消息的列表中获得确认。

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.info('已收到投放代码的%s：%i',
                    confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        LOGGER.info('已发布%i条消息，%i还没有被确认，“%i被查出，%i被扣留',
                    self._message_number, len(self._deliveries),
                    self._acked, self._nacked)

    def schedule_next_message(self):
        """如果我们没有关闭与RabbitMQ的连接，则可以安排另一条消息在PUBLISH_INTERVAL秒内发送.

        """
        LOGGER.info('计划下一个消息的%0.1f秒',
                    self.PUBLISH_INTERVAL)
        self._connection.add_timeout(self.PUBLISH_INTERVAL,
                                     self.publish_message)

    def publish_message(self):
        """如果没有停止，发布一条消息到RabbitMQ，附加一个发送列表和发送的消息号码。

        此列表将用于检查on_delivery_confirmations方法中的交付确认。

        一旦消息已经发送，安排另一条消息发送。

        我把调度的主要原因是通过改变类中的PUBLISH_INTERVAL常量来减慢和加快交付时间间隔，
        从而可以很好地了解流程是如何流动的。
        """
        if self._channel is None or not self._channel.is_open:
            return

        hdrs = {u'مفتاح': u' قيمة',
                u'键': u'值',
                u'キー': u'値'}
        properties = pika.BasicProperties(delivery_mode=2)

        message = u'مفتاح قيمة 键 值 キー 値'
        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                    json.dumps(message),
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.info('发布消息 # %i', self._message_number)
        self.schedule_next_message()

    def run(self):
        """通过连接然后启动IOLoop运行.

        """
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    # 完成关闭
                    self._connection.ioloop.start()

        LOGGER.info('已关闭')

    def stop(self):
        """通过关闭通道和连接来停止示例。

            在这里设置了一个标志，以便我们停止安排要发布的新消息。

            IOLoop被启动，因为当KeyboardInterrupt被捕获时，下面的Try / Catch调用这个方法。

            再次启动IOLoop将允许发布者与RabbitMQ彻底断开连接。

        """
        LOGGER.info('停止中')
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """调用此命令通过发送Channel.Close RPC命令来关闭与RabbitMQ的通道。

        """
        if self._channel:
            LOGGER.info('信道关闭中')
            self._channel.close()

    def close_connection(self):
        """这个方法关闭与RabbitMQ的连接."""
        if self._connection is not None:
            LOGGER.info('连接关闭中')
            self._connection.close()


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    example = ExamplePublisher('amqp://web:web@127.0.0.1:5672/study?connection_attempts=3&heartbeat_interval=3600')
    example.run()


if __name__ == '__main__':
    main()