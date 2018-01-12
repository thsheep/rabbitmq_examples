
import redis


class RedisClient(object):

    def __init__(self,
                 host='localhost',
                 port=6379,
                 db=0, password=None, **kwargs):
        self.r_con = redis.StrictRedis(host=host, port=port, password=password,
                                      db=db, decode_responses=True, **kwargs) \
            if password else redis.StrictRedis(host=host, port=port, db=db,
                                               decode_responses=True, **kwargs)

    def pusub(self, key, message):
        """将用户信息SADD进REIDS进行去重
        :param key: Key
        :param message: 信息
        :return:
        """
        try:
            # 将用户ID SADD 进redis使用sadd方法可以比表用户id重复
            if isinstance(message, list):
                for user_id in message:
                    self.r_con.sadd(key, user_id)
            elif isinstance(message, str):
                self.r_con.sadd(key, message)
            elif isinstance(message, dict):
                for k, v in message.items():
                    self.r_con.sadd(key, v)
        except Exception as e:
            print(e.args)
            raise e.args

    def sdiff(self, key):
        """返回key列表所有数据
        :param key:
        :return:
        """
        return self.r_con.sdiff(key)

    def delete(self, key):
        """删除Key
        :param key:
        :return:
        """
        return self.r_con.delete(key)
