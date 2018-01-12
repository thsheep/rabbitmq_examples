'''RabbitMQ消息队列'''

import pika
from abc import ABCMeta, abstractmethod
from pika import credentials
from common.rabbitmq.rabbitmqheartbeat import RabbitMQHeartbeat
from common.rabbitmq.queuecount import MessagesCount


class IMessageConnection(metaclass=ABCMeta):
    # 消息连接接口类，所有消息连接类都必须实现以下接口
    @abstractmethod
    def get_connection(self):
        '''
        :return: 返回连接实例
        '''
        return


class IMessageCallBack(metaclass=ABCMeta):
    # 消息回调接口类，所有需要使用到消息回调的类都需要继承该类，并实现callback方法，否则无法完成消息回调
    @abstractmethod
    def callback(self):
        '''
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:消息成功返回True，否则False
        '''
        return


class IMessageHandler(metaclass=ABCMeta):
    # 消息处理接口类，所有消息类型类都必须实现以下接口

    @abstractmethod
    def close(self):
        # 关闭连接
        return

    @abstractmethod
    def start_consuming(self, callback):
        '''开始消费消息
        :param callback:回调函数
        :return:
        '''
        return


class ConnectionFactory(object):
    @staticmethod
    def get_instance(class_name, connect_params):
        '''获取消息连接实例
        :param class_name:消息连接类名
        :param connect_params:连接参数
        :return:连接实例
        '''
        if 'RabbitMQConnection' == class_name:
            return RabbitMQConnection(connect_params)


class RabbitMQConnection(IMessageConnection):
    def __new__(cls, connect_params):
        if not hasattr(cls, '_inst'):
            cls._inst = super(RabbitMQConnection, cls).__new__(cls)
        return cls._inst

    def __init__(self, connect_params):
        self.params = connect_params
        self.connection = None

    def get_connection(self):
        self.connection = self.connect()
        return self.connection

    def _credentials(self, username, password):
        '''返回一个plain credentials对象
        :param username:用户名
        :param password:密码
        :return:pika_credentials.PlainCredentials
        '''
        return credentials.PlainCredentials(username, password)

    def connect(self):
        username = self.params.get('username')
        password = self.params.get('password')
        if '' == username:
            print('RabbitMQ连接错误！用户名为空！')
        if '' == password:
            print('RabbitMQ连接错误！密码为空！')

        certificate = self._credentials(username, password)
        connect_params = pika.ConnectionParameters(host=self.params.get('host'),
                                                   port=self.params.get('port'),
                                                   virtual_host=self.params.get('virtual_host'),
                                                   credentials=certificate,
                                                   channel_max=self.params.get('channel_max'),
                                                   frame_max=self.params.get('frame_max'),
                                                   heartbeat_interval=self.params.get('heartbeat_interval'),
                                                   ssl=self.params.get('ssl'),
                                                   ssl_options=self.params.get('ssl_options'),
                                                   connection_attempts=self.params.get('connection_attempts'),
                                                   retry_delay=self.params.get('retry_delay'),
                                                   socket_timeout=self.params.get('socket_timeout'),
                                                   locale=self.params.get('locale'),
                                                   backpressure_detection=self.params.get('backpressure_detection')
                                                   )

        if not self.connection:
            return pika.BlockingConnection(connect_params)


class MessageHandlerFactory(object):
    # 负责消息处理程序的创建
    @staticmethod
    def get_instance(class_name, connect_params, message_params):
        '''获取消息处理实例
        :param class_name:消息处理类名
        :param connect_params:
        :param message_params:消息处理参数
        :return:class_name实例
        '''
        if 'RabbitMQ' == class_name:
            return RabbitMQMessageHandler(connect_params, message_params)


class RabbitMQMessageHandler(IMessageHandler):
    # RabbitMQ消息处理程序实现
    def __init__(self, connect_params, message_params):
        '''初始化函数，需要传入消费的配置参数
        :param connect_params:消费的配置参数
        :param message_params:None
        '''
        self.params = message_params
        self.channel = None
        self.connection = ConnectionFactory.get_instance('RabbitMQConnection', connect_params).get_connection()

    def start_consuming(self, callback):
        '''启动消费，消息的类型根据初始化传入的配置参数来决定启动哪种消费模式
        :param callback:回调函数
        :return:None
        '''
        heartbeat = RabbitMQHeartbeat(self.connection)
        heartbeat.start()
        heartbeat.start_heartbeat()
        self.on_bind()
        self._consuming_queues(callback)

    def publish_message(self, data):
        '''
        发布消息，消息的类型根据初始化传入的配置参数来决定发送那种消息
        :param callback:回调函数的实例
        :return:None
        '''
        self._publish_queues(data)

    def _publish_queues(self, data):
        # 竞争消费者模式： 发布
        channel = self._get_channel()
        routing_key = self.params.get('routing_key')
        delivery_mode = self.params.get("delivery_mode")
        self.channel.basic_publish(exchange=self.params.get("exchange"),
                                   routing_key=routing_key,
                                   body=data,
                                   properties=pika.BasicProperties(
                                       delivery_mode=delivery_mode)
                                   )
        channel.close()

    def _consuming_queues(self, callback_obj=None):
        # 竞争消费模式
        channel = self._get_channel()
        queue_name = self.params.get('task_queue')
        durable = self.params.get('durable')
        prefetch_count = self.params.get('prefetch_count')
        channel.queue_declare(queue=queue_name, durable=durable)

        def callback(ch, method, properties, body):
            if isinstance(callback_obj, IMessageCallBack):
                if callback_obj.callback(ch, method, properties, body):
                    ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=prefetch_count)
        channel.basic_consume(callback, queue=queue_name)
        channel.start_consuming()

    def setup_exchange(self):
        """申明交换机
        :return:
        """
        self.channel.exchange_declare(exchange=self.params.get("exchange"),
                                      exchange_type=self.params.get("exchange_type"),
                                      durable=self.params.get("durable"))

    def setup_queue(self):
        """队列申明
        :return:
        """
        for queue_name in self.params.get("publish_data_queue"):
            self.channel.queue_declare(queue=queue_name,
                                       durable=self.params.get("durable"))

    def on_bind(self):
        """绑定交换机与队列
        :return:
        """
        self._get_channel()
        self.setup_exchange()
        self.setup_queue()
        for queue_name in self.params.get("publish_data_queue"):
            self.channel.queue_bind(queue=queue_name,
                                    exchange=self.params.get("exchange"),
                                    routing_key=self.params.get("routing_key"))

    def _get_channel(self):
        """
        获取通道
        :return:
        """
        self.channel = self.connection.channel()
        return self.channel

    def close_channel(self):
        """关闭信道
        :return:
        """
        self.channel.close()

    def close(self):
        """关闭连接
        :return:
        """
        self.connection.close()
