import jsonpath
import json
import requests as rq
import time
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[0]))
#print(str(Path(__file__).resolve().parents[0]))
import TransCooSys
import os
import re

## 引入各爬虫程序
# 百度地图 全国行政区划边界
from crawl import CrawlAreaBoundBaidu
# 百度地图 覆盖面、公交线路 
from crawl import CrawlRouteBaidu
# 高德地图 覆盖面、公交线路
from crawl import CrawlPoiBaidu
# 百度地图 路网数据
from crawl import CrawlHighway

from crawl import CrawlProcessCronller as cpc
from crawl import CrawlHighwayProcess as ch
import datetime

## 参数： 输入文件， 爬虫输出目录， 标准结果输出目录
## 1 读取输入文件，根据文件名调用相应的爬虫程序
## 2 执行爬虫，爬取结果存储到输出目录
## 3 读取结果文件，进行标准表结构转换，输出txt文件
## filename: bound区划, poi覆盖面, route公交地铁, highway路网（国道省道高速）
def crawl_entrance(input_path, output_path, standard_path):
    ctype = input('输入爬取类型：1 行政区划, 2 区域面, 3 公交地铁, 4 路网\n')
    cdict = {'1': '行政区划', '2': '区域面', '3': '公交地铁', '4': '路网'}
    cdict2 = {'1': 'bound', '2': 'poi', '3': 'route', '4': 'highway'}
    print('启动爬虫: ' + cdict[ctype])
    out_file = ''
    formatted_today=datetime.date.today().strftime("%y%m%d")
    coutdir = os.path.join(output_path, cdict2[ctype])
    standarddir = os.path.join(standard_path, cdict2[ctype])
    if not os.path.exists(coutdir):
        os.makedirs(coutdir)
    if not os.path.exists(standarddir):
        os.makedirs(standarddir)
    for root, ds, fs in os.walk(input_path):
        for fname in fs:
            filename = os.path.join(input_path, fname)
            standard_outfile = 'standard_'+fname.replace('.txt','')+'_'+formatted_today+'.txt'
            if ctype == '1' and 'bound' in filename:
                print('读取: '+filename)
                CrawlAreaBoundBaidu.crawl_bound(filename, output_path)
                out_file = cpc.get_all_bound_data(coutdir, standarddir, standard_outfile)
                print(out_file)
            elif ctype == '2' and 'poi' in filename:
                print('读取: '+filename)
                CrawlPoiBaidu.execute_poi(filename, output_path)
                out_file = cpc.get_all_poi_data(coutdir, standarddir, standard_outfile)
                print(out_file)
            elif ctype == '3' and 'route' in filename:
                print('读取: '+filename)
                CrawlRouteBaidu.execute_crawl(filename, output_path)
                out_file = cpc.get_all_route_data(coutdir, standarddir, standard_outfile)
                print(out_file)
            elif ctype == '4' and 'highway' in filename:
                print('读取: '+filename)
                CrawlHighway.crawl_highway(filename, output_path)
                out_file = ch.get_all_province_lines(coutdir, standarddir, standard_outfile)
                print(out_file)

# crawl_entrance('crawl_input/poi_amap_sx_zj.txt', 'crawl_output', 'standard_out')
crawl_entrance('crawl_input', 'crawl_output', 'standard_out')

