
'''配置文件读取'''


from common.public import read_file, load_json


__all__ = ['rabbitmq_conf', 'app_conf', "rabbitmq_test_conf"]
rb_config_path = './conf/rabbitmq.conf'
app_config_path = './conf/config.conf'
rb_test_config_path = './conf/rabbitmq_test.conf'


def read(path):
    '''读取配置文件
    :param path: 配置文件路径
    :return: dict
    '''
    data = read_file(path)
    return load_json(data)

rabbitmq_conf = read(rb_config_path)
app_conf = read(app_config_path)
rabbitmq_test_conf = read_file(rb_test_config_path)
