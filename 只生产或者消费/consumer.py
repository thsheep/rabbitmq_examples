#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 18-1-2 上午11:37
# @Author  : 哎哟卧槽
# @Site    : 对处理耗时少的任务 效果非常出色，不适合耗时长的任务
# @File    : rabbitmq_test.py
# @Software: PyCharm
import time
import logging
import pika
from common.rabbitmq.rabbitmqheartbeat import RabbitMQHeartbeat
from config import rabbitmq_test_conf

LOG_FORMAT = ('%(levelname) -10s %(asctime)s'
              '%(message)s')
LOGGER = logging.getLogger(__name__)


class ExampleConsumer(object):
    """这是一个处理与ＭＱ连接发生意外的示例

    如果RabbitMQ关闭连接，将会重新连接（权限相关，超时等）

    如果通道关闭那么问题也会显示在输出中

    """
    EXCHANGE = 'exchange'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'study'
    ROUTING_KEY = 'study'

    def __init__(self, amqp_url):
        """创建一个消费者类的新实例，传递用于连接到RabbitMQ的AMQP URL。

        :param str amqp_url: 要连接的AMQP网址

        """
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._rabbit_config = rabbitmq_test_conf

    def connect(self):
        """这个方法连接到RabbitMQ，返回连接句柄。 连接建立后，pika会调用on_connection_open方法。

        :return: pika.SelectConnection

        """
        LOGGER.info('连接到 %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """ 一旦连接到RabbitMQ，这个方法就被pika调用。 如果需要的话，它将句柄传递给连接对象，
        但是在这种情况下，我们将其标记为未使用。
        :param unused_connection: pika.SelectConnection
        :return:

        """
        LOGGER.info('连接打开')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """这个方法增加了一个关闭的回调函数，当RabbitMQ关闭与发布者的连接时，会被pika调用。

        """
        LOGGER.info('添加连接关闭回调')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """当与RabbitMQ的连接意外关闭时，此方法由pika调用。 如果它由于意外断开连接，我们将重新连接到RabbitMQ，。

        :param pika.connection.Connection connection: 关闭的连接obj
        :param int reply_code: 服务器提供了reply_code（如果给出）
        :param str reply_text: 服务器提供reply_text给定

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('连接关闭，5秒内重新打开: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """
        如果连接关闭，将由IOLoop定时器调用。 请参阅on_connection_closed方法。
        """
        # 这是IOLoop实例的旧连接，需要停止ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # 创建一个新的连接
            self._connection = self.connect()
            # 现在有一个新的连接，需要一个新的ioloop运行
            self._connection.ioloop.start()

    def open_channel(self):
        """
        通过发出Channel.Open RPC命令，用RabbitMQ打开一个新的频道。 当RabbitMQ响应通道打开时，on_channel_open回调将被pika调用。
        """
        LOGGER.info('创建新信道')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        频道打开后，此方法由pika调用。 通道对象被传入，因此我们可以使用它。

        由于频道现在开放，我们将设置交换机。

        :param pika.channel.Channel channel: 这是频道对象

        """
        LOGGER.info('打开信道')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """
        如果RabbitMQ意外关闭通道，这个方法告诉pika调用on_channel_closed方法。
        """
        LOGGER.info('添加信道关闭回调')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """
        当RabbitMQ意外关闭频道时，由pika调用。
        如果您尝试执行违反协议的操作，例如重新声明交换或使用不同参数的队列，通常通常关闭通道。
        在这种情况下，我们将关闭连接来关闭对象。

        :param pika.channel.Channel: 需要关闭的频道
        :param int reply_code: 频道关闭的原因（错误数字代码）
        :param str reply_text: 频道关闭的原因（错误文本）

        """
        LOGGER.warning('信道 %i 已经关闭: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def setup_exchange(self, exchange_name):
        """通过调用Exchange.Declare RPC命令在RabbitMQ上设置交换。
        完成后，pika将调用on_exchange_declareok方法。

        :param str|unicode exchange_name: 需要设置的交换机的名字

        """
        LOGGER.info('声明交换机 %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        """当RabbitMQ完成Exchange.Declare RPC命令时，由pika调用。

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk的响应

        """
        LOGGER.info('交换机声明')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """
        通过调用Queue.Declare RPC命令在RabbitMQ上设置队列。 完成后，pika将调用on_queue_declareok方法。

        :param str|unicode queue_name: 队列的名字.

        """
        LOGGER.info('声明队列: %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        """当在setup_queue中完成的Queue.Declare RPC调用完成时，由pika调用的方法。
        在这个方法中，我们将通过发出Queue.Bind RPC命令将队列和交换绑定在一起。
        当这个命令完成后，pika会调用on_bindok方法。

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info('绑定 %s 到 %s 使用 %s',
                    self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def on_bindok(self, unused_frame):
        """当Queue.Bind方法完成时，由pika调用。
        在这一点上，我们将通过调用start_consuming开始消费消息，这将调用所需的RPC命令来启动进程。

        :param pika.frame.Method unused_frame: Queue.BindOk的返回

        """
        LOGGER.info('队列绑定')
        self.start_consuming()

    def start_consuming(self):
        """
        这个方法通过首先调用add_on_cancel_callback来设置消费者，以便在RabbitMQ取消消费者时通知该对象。
        然后它发出Basic.Consume RPC命令，它返回用于唯一标识RabbitMQ消费者的消费者标签。
        当我们要取消消费。 on_message方法作为回调pika将在消息完全接收时调用传入。

        """
        LOGGER.info('发出消费者相关的RPC命令')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(on_message,
                                                         self.QUEUE)

    def add_on_cancel_callback(self):
        """添加一个回调，如果RabbitMQ由于某种原因关闭了消费者，将会被调用。
        如果RabbitMQ关闭了消费者，则会由pika调用on_consumer_cancelled。

        """
        LOGGER.info('添加消费者关闭回调')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """当RabbitMQ为接收消息的消费者发送Basic.Cancel时，由pika调用。

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('消费者被远程关闭: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def publish_message(self):
        """

        :return:
        """

    def acknowledge_message(self, delivery_tag):
        """通过发送传递标签的Basic.Ack RPC方法来确认从RabbitMQ传递的消息。

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('确认消息 %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """通过发送Basic.Cancel RPC命令告诉RabbitMQ您想要停止使用。
        """
        if self._channel:
            LOGGER.info('发送一个Basic.Cancel RPC命令给RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        """当RabbitMQ确认关闭消费者时，此方法由pika调用。 此时我们将关闭该频道。
        一旦通道关闭，这将调用on_channel_closed方法，这将关闭连接。

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        LOGGER.info('RabbitMQ确认关闭消费者')
        self.close_channel()

    def close_channel(self):
        """调用通过发出Channel.Close RPC命令完全关闭与RabbitMQ通道。

        """
        LOGGER.info('关闭信道')
        self._channel.close()

    def run(self):
        """运行示例使用者，连接到RabbitMQ，然后启动IOLoop以阻塞模式允许SelectConnection进行操作。
        """
        self._connection = self.connect()

        heartbeat = RabbitMQHeartbeat(self._connection)
        heartbeat.start()
        heartbeat.start_heartbeat()
        self._connection.ioloop.start()

    def stop(self):
        """通过使用RabbitMQ来停止使用者，干净地关闭与RabbitMQ的连接。
         当RabbitMQ确认取消后，pika将调用on_cancelok，然后关闭通道和连接。
         IOLoop被再次启动，因为当按下CTRL-C引发KeyboardInterrupt异常时调用此方法。
         这个异常停止了需要运行的pico与RabbitMQ进行通信的IOLoop。
         在启动IOLoop之前发出的所有命令将被缓存但不被处理。
        """
        LOGGER.info('停止中')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        LOGGER.info('停止')

    def close_connection(self):
        """此方法关闭与RabbitMQ的连接。"""
        LOGGER.info('关闭连接')
        self._connection.close()


def on_message(unused_channel, basic_deliver, properties, body):
    """当RabbitMQ发送消息时，由pika调用。 该频道是为了您的方便而传递的。
    传入的basic_deliver对象携带消息的交换，路由密钥，交付标记和重新传递的标志。
     传入的属性是具有消息属性的BasicProperties实例，正文是发送的消息。

    :param pika.channel.Channel unused_channel: 信道对象
    :param pika.Spec.Basic.Deliver: basic_deliver方法
    :param pika.Spec.BasicProperties: properties
    :param str|unicode body: 消息内容

    """
    LOGGER.info('收到消息 # %s 从这儿来的: %s: %s',
                basic_deliver.delivery_tag, properties.app_id, body.decode())
    LOGGER.info("处理任务需要花费120S")
    time.sleep(120)
    unused_channel.basic_ack(basic_deliver.delivery_tag)





def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    example = ExampleConsumer('amqp://web:web@127.0.0.1:5672/study')
    try:
        example.run()
    except KeyboardInterrupt:
        example.stop()


if __name__ == '__main__':
    main()
