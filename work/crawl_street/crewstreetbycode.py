# -*- coding:utf-8 -*-
# 页面请求组件
import requests as rq



if __name__ == '__main__':

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

    file = open(r'D:\Data\output\streetlayer.txt', 'r', encoding='utf-8')
    lines = file.readlines()
    sep = '\t'
    for line1 in lines:
        line = line1.split('\t')
        try:
            towncode = line[1]
            citycode = line[3][0:4]
            print(towncode)
            url = "http://39.104.59.235:8090/geoserver/rivermap_district/ows?SERVICE=WFS&OUTPUTFORMAT=json&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=rivermap_district:layer_street&STARTINDEX=0&COUNT=2000&FILTER=%3CFilter%20xmlns%3D%22http:%2F%2Fwww.opengis.net%2Ffes%2F2.0%22%20xmlns:rivermap_district%3D%22http:%2F%2Flocalhost:8100%2Frivermap_district%22%20xmlns:gml%3D%22http:%2F%2Fwww.opengis.net%2Fgml%2F3.2%22%3E%3CPropertyIsEqualTo%3E%3CValueReference%3Eadcode%3C%2FValueReference%3E%3CLiteral%3E"+towncode+"%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FFilter%3E"
            res = rq.get(url, timeout=(10, 20), headers=headers)
            restext = res.text
            with open('D:\Data\output\streetall.txt', 'a', encoding='utf-8') as fw:
                fw.write(citycode + sep + restext + sep + '\n')
        except Exception as e:
            print(line)
            with open('D:\Data\output\streetall_log.txt','a', encoding='utf-8', errors='ignore') as fw:
                fw.write(line[0] + sep + line[1] + sep + line[2] + sep + line[3][0:4]+'\n')
