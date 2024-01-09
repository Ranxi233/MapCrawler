# -*- coding:utf-8 -*-
# 页面请求组件
from distutils.dir_util import copy_tree
import requests as rq
# json解析组件
import json
import time
import os
import pandas as pd
import hashlib



ROOT_DIR = 'D:/py_workspace/'
OUTDIR = ROOT_DIR+'crawl_output/'
RESOURECE_DIR = ROOT_DIR + 'resources/'

def MD5(str):
    md5 = hashlib.md5()
    md5.update(str.encode('utf-8'))
    return md5.hexdigest()

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Authorization': 'Basic cml2ZXJtYXBfZ3Vlc3Q6cml2ZXJtYXBfc2hhcmU=',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': '39.104.59.235:8090',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

def crawl_street():
    data = set()
    file = open(RESOURECE_DIR+'all_street_0526.txt', 'r', encoding='utf-8')
    lines = file.readlines()
    sep = '\t'
    for line in lines:
        try:
            line = line.replace('\n','').split('\t')
            province = line[1]
            city = line[3]
            district = line[5]
            ## 中山市、东莞市等没有区县，直接管辖街道
            # if city not in {'中山市','东莞市'}:
            #     continue
            townname = line[7]
            towncode = line[6]
            # townname = '330106110000'
            # towncode = '双浦镇'
            if towncode not in data:
                url = "http://39.104.59.235:8090/geoserver/rivermap_district/ows?SERVICE=WFS&OUTPUTFORMAT=json&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=rivermap_district:layer_street&STARTINDEX=0&COUNT=2000&FILTER=%3CFilter%20xmlns%3D%22http:%2F%2Fwww.opengis.net%2Ffes%2F2.0%22%20xmlns:rivermap_district%3D%22http:%2F%2Flocalhost:8100%2Frivermap_district%22%20xmlns:gml%3D%22http:%2F%2Fwww.opengis.net%2Fgml%2F3.2%22%3E%3CPropertyIsEqualTo%3E%3CValueReference%3Eadcode%3C%2FValueReference%3E%3CLiteral%3E"+towncode[0:9]+"%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FFilter%3E"
                rn = rq.get(url, timeout=(3, 9),headers=headers)
                rntext = rn.text
                if len(rntext) < 100:
                    url = "http://39.104.59.235:8090/geoserver/rivermap_district/ows?SERVICE=WFS&OUTPUTFORMAT=json&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=rivermap_district:layer_street&STARTINDEX=0&COUNT=2000&FILTER=%3CFilter%20xmlns%3D%22http:%2F%2Fwww.opengis.net%2Ffes%2F2.0%22%20xmlns:rivermap_district%3D%22http:%2F%2Flocalhost:8100%2Frivermap_district%22%20xmlns:gml%3D%22http:%2F%2Fwww.opengis.net%2Fgml%2F3.2%22%3E%3CPropertyIsEqualTo%3E%3CValueReference%3EName%3C%2FValueReference%3E%3CLiteral%3E"+townname+"%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FFilter%3E"
                    rn = rq.get(url, timeout=(3, 9),headers=headers)
                    rntext = rn.text
                jsondata = eval(rn.text)
                if jsondata["features"][0]['properties']['code'][0:6] != towncode[0:6]:
                    with open(OUTDIR+'log/street_log.txt','a', encoding='utf-8', errors='ignore') as fw:
                        fw.write(str(line)+'\n')
                    continue
                newjson = {
                    "type": "Feature",
                    "properties": {
                        "adcode": towncode,
                        "name": townname,
                        "level": 5,
                        "parent": {"adcode": towncode[0:6]}
                    },
                    "geometry": jsondata["features"][0]['geometry']
                }

                FIRST_TIME = '0'
                LAST_TIME = '0'
                COUNTER = '0'
                COLLECT_PLACE = '330000'
                DATA_SOURCE = '0'
                DATA_STATUS = ''
                PROVINCES = province
                CITY = city
                COUNTY = district
                XZQHDM = towncode
                XLQYMC = townname
                DZMC = ''
                PLACE_NAME = '行政区划'
                DESCRIPTIONS = json.dumps(newjson, ensure_ascii=False)
                INFOR_CONTENT = ''
                MD_ID = MD5(XZQHDM+XLQYMC+DZMC+PLACE_NAME)
                with open(OUTDIR+'bound/street0616.txt','a', encoding='utf-8') as fw:
                    fw.write(MD_ID + sep +
                        FIRST_TIME + sep +
                        LAST_TIME + sep +
                        COUNTER + sep +
                        COLLECT_PLACE + sep +
                        DATA_SOURCE + sep +
                        DATA_STATUS + sep +
                        PROVINCES + sep +
                        CITY + sep +
                        COUNTY + sep +
                        XZQHDM + sep +
                        XLQYMC + sep +
                        DZMC + sep +
                        PLACE_NAME + sep +
                        DESCRIPTIONS + sep +
                        INFOR_CONTENT + '\n')
                data.add(towncode)
            else:
                continue
        except Exception as e:
            print(line)
            with open(OUTDIR+'log/street_log.txt','a', encoding='utf-8', errors='ignore') as fw:
                fw.write(str(line)+'\n')

if __name__ == '__main__':
    crawl_street()
