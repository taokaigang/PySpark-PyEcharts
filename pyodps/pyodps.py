from odps import ODPS
from odps import DataFrame
from odps.models import Schema, Column, Partition
import pandas as pd
from collections import defaultdict


o = ODPS('**your-access-id**', '**your-secret-access-key**', '**your-default-project**',
            endpoint='**your-end-point**')


# get_table方法
def get_odps_table(tb_name):
    data = DataFrame(o.get_table(tb_name))
    data['ds'] = data['ds'].astype('int')
    return data


# 通过sql获取数据
def exe_sql(sql):
    data = []
    with o.execute_sql(sql).open_reader() as reader:
        d = defaultdict(list)  # collection默认一个dict
        for record in reader:
            for res in record:
                d[res[0]].append(res[1])  # 解析record中的每一个元组，存储方式为(k,v)，以k作为key，存储每一列的内容；
        data = pd.DataFrame.from_dict(d, orient='index').T  # 转换为数据框，并转置，不转置的话是横条数据
    return data

# 在dataworks中创建的PyODPS脚本，数据会有1万条的限制，此时需要对查询数据创建临时表，再通过get_table的方式获取


def exe_dataworks_sql(sql):
    data = []
    sql_new = 'create table xisuo_tmp as %s' % (sql)
    o.execute_sql(sql_new)
    with o.get_table('xiatian_tmp').open_reader() as reader:
        d = defaultdict(list)  # collection默认一个dict
        # record一行数据，res一个元组
        for record in reader:
            for res in record:
                d[res[0]].append(res[1])  # 解析record中的每一个元组，存储方式为(k,v)，以k作为key，存储每一列的内容；
        data = pd.DataFrame.from_dict(d, orient='index').T  # 转换为数据框，并转置，不转置的话是横条数据
    o.delete_table('xisuo_tmp', if_exists=True)
    if o.exist_table('xisuo_tmp'):
        print('请删除临时表')
    else:
        print('临时表已清除')
    return data


# 通过dataframe创建表

# 创建表
def create_odps_table(tb_name, schema):
    try:
        table = o.create_table(tb_name, schema=schema, if_not_exists=True)
        return True
    except:
        return False


# 创建表
def gmt_table(tb_name, df, schema_name=None, schema_type=None, schema_comment=None):
    if schema_name is None:
        schema_name = df.columns.values.tolist()
    if schema_type is None:
        schema_type = ['string'] * len(schema_name)
    if schema_comment is None:
        schema_comment = [''] * len(schema_name)

    df_schema = pd.DataFrame({'name': schema_name, 'type': schema_type, 'comment': schema_comment})
    df_schema = df_schema[['name', 'type', 'comment']]
    df_columns = df_schema.apply(lambda x: [Column(name=x[0], type=x[1], comment=x[2])], axis=1)

    columns = []
    for col in df_columns:
        columns = columns + col

    partitions = [Partition(name='ds', type='string', comment='')]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table(tb_name, schema=schema, if_not_exists=True)
    return True



# dataframe写入表
# 写入表
def gmt_table(tb_name, df, schema_name=None, schema_type=None, schema_comment=None):
    if schema_name is None:
        schema_name = df.columns.values.tolist()
    if schema_type is None:
        schema_type = ['string'] * len(schema_name)
    if schema_comment is None:
        schema_comment = [''] * len(schema_name)

    df_schema = pd.DataFrame({'name': schema_name, 'type': schema_type, 'comment': schema_comment})
    df_schema = df_schema[['name', 'type', 'comment']]
    df_columns = df_schema.apply(lambda x: [Column(name=x[0], type=x[1], comment=x[2])], axis=1)

    columns = []
    for col in df_columns:
        columns = columns + col

    partitions = [Partition(name='ds', type='string', comment='')]
    schema = Schema(columns=columns, partitions=partitions)
    table = o.create_table(tb_name, schema=schema, if_not_exists=True)
    return True


# 删除表
def drop_odps_tabel(tb_name):
    table = o.delete_table(tb_name, if_exists=True)
    status = o.exist_table(tb_name)
    return status

# 数据集读出转换成pandas的dataframe之后，就是常规的分析过程了。


if __name__ == '__main__':
    print('hi')
