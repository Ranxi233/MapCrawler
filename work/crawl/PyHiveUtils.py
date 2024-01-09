# -*- coding: UTF-8 -*-

import os
from sys import argv
import subprocess

# from pyhive import hive
# cursor = hive.connect(host='cdhnn1', port='2181', database='default', username='hiveuser', password='sh@88861158')
# cc = cursor.cursor()
# cc.execute('show databases;')
# res = cc.fetchall()
# columns = res[0]
class HiveConnection():
    def __init__(self, beelineUrl, user='', password=''):
        self.beelineUrl = beelineUrl
        self.user = user
        self.password = password

    def excute_beeline_cmd(self, sql):
        cmd = "beeline -u \'{}\' -e \"{}\"".format(self.beelineUrl, sql).replace('\n', '')
        if self.user:
            cmd = "beeline -u \'{}\' -n {} -p {} -e \"{}\"".format(self.beelineUrl, self.user, self.password, sql).replace('\n', '')
        try:
            print(cmd)
            cmdout = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            if cmdout:
                print(cmdout)
        except subprocess.CalledProcessError as e:
            print(cmdout)
            raise
    
    def execute_hdfs_cmd(self, cmd):
        try:
            cmdout = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            if cmdout:
                print(cmdout)
        except subprocess.CalledProcessError as e:
            print(cmdout)
            raise
# 路网
create_DLYQLWSJ_sql = """
    create table if not exists wx_test.ZSK_JC_DLYQLWSJ_01 (
        md_id String,
        first_time String,
        last_time String,
        counter String,
        collect_place String,
        data_source String,
        data_status String,
        provinces String,
        city String,
        county String,
        xzqhdm String,
        device_type String,
        sxh String,
        dlmc String,
        dldm String,
        descriptions String,
        isno String,
        isno1 String,
        isno2 String,
        isno3 String
    )
    COMMENT '地理引擎_路网数据_开发调试'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      'hdfs://nameservice1/user/hive/warehouse/wx_test.db/ZSK_JC_DLYQLWSJ_01'
"""

# 区域面（行政区划/POI）
create_DLYQQYFGMSJ_sql = """
    create table if not exists wx_test.ZSK_JC_DLYQQYFGMSJ_01 (
        md_id String,
        first_time String,
        last_time String,
        counter String,
        collect_place String,
        data_source String,
        data_status String,
        provinces String,
        city String,
        county String,
        xzqhdm String,
        xlqymc String,
        dzmc String,
        place_name String,
        descriptions String,
        infor_content String
    )
    COMMENT '地理引擎_覆盖面POI'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      'hdfs://nameservice1/user/hive/warehouse/wx_test.db/ZSK_JC_DLYQQYFGMSJ_01'
"""

## 公交地铁线路
create_DLYQGJTLXLSJ_sql = """
    create table if not exists wx_test.ZSK_JC_DLYQGJTLXLSJ_01 (
        md_id String,
        first_time String,
        last_time String,
        counter String,
        collect_place String,
        data_source String,
        data_status String,
        provinces String,
        city String,
        xzqhdm String,
        dldm String,
        dlmc String,
        place_name String,
        extract_desc String,
        infor_content String,
        relafulldesc String,
        descriptions String
    )
    COMMENT '地理引擎_覆盖面POI'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      'hdfs://nameservice1/user/hive/warehouse/wx_test.db/ZSK_JC_DLYQGJTLXLSJ_01'
"""



hiveconn = HiveConnection(
    'jdbc:hive2://cdhnn1:2181,cdhnn2:2181,cdhdn4:2181/wx_test;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2_zk',
    # 'hiveuser',
    # 'sh@88861158'
)

def upload_data_tohive(data_file, data_filename, tablename):
    if tablename == 'ZSK_JC_DLYQLWSJ_01':
        hiveconn.excute_beeline_cmd(create_DLYQLWSJ_sql)
    elif tablename == 'ZSK_JC_DLYQQYFGMSJ_01':
        hiveconn.excute_beeline_cmd(create_DLYQQYFGMSJ_sql)
    elif tablename == 'ZSK_JC_DLYQGJTLXLSJ_01':
        hiveconn.excute_beeline_cmd(create_DLYQGJTLXLSJ_sql)
    # hdfs路径
    hdfs_data_file = '/tmp/wx_test/' + data_filename
    # 服务器上传hdfs
    upload_cmd = 'hdfs dfs -put {} {}'.format(data_file, hdfs_data_file)
    # hive加载数据
    load_data_sql = "load data inpath '{}' into table wx_test.{}".format(hdfs_data_file, tablename)

    hiveconn.execute_hdfs_cmd(upload_cmd)
    hiveconn.excute_beeline_cmd(load_data_sql)
