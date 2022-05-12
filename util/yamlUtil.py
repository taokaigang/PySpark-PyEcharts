import yaml


# 读取yaml文件
class ReadYaml():
    def __init__(self, path, param=None):
        self.path = path  # 文件路径
        self.param = param  # 不传默认获取所有数据

    # 获取yaml文件中的数据
    def get_data(self, encoding='utf-8'):
        with open(self.path, encoding=encoding) as f:
            data = yaml.load(f.read(), Loader=yaml.FullLoader)

            # （有点多此一举，不要也行）
            if self.param == None:
                return data  # 返回所有数据
            else:
                return data.get(self.param)  # 获取键为param的值


# 写入yaml文件
class Writeyaml():
    def __init__(self, path, message):
        self.path = path
        self.message = message

    # 追加内容
    def add_data(self, encoding="utf-8"):
        with open(self.path, encoding=encoding, mode="a+") as f:
            yaml.dump(self.message, stream=f, allow_unicode=True)

    # 重写文件内容
    def change_data(self, encoding='utf-8'):
        with open(self.path, encoding=encoding, mode='w') as f:
            yaml.dump(self.message, stream=f, allow_unicode=True)


if __name__ == "__main__":
    filepath = r'./config.yaml'
    data = ReadYaml(filepath).get_data()
    # data = ReadYaml(filepath,'logger').get_data()
    print(data)

    # 写文件
    # message = {'dbtest': {'host': '192.168.134.1', 'user': 'test', 'pwd': 'test', 'port': 3306}}
    # Writeyaml(path=filepath, message=message).change_data()
    # Writeyaml(path=filepath, message=data).add_data()
