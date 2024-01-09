# -*- coding:utf-8 -*-
# 页面请求组件
import requests as rq
import zhconv
# json解析组件
import json
import time
import os
import pandas as pd


if __name__ == '__main__':
    sep = '\t'
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


    file = open(r'D:\Data\input\2022年全国行政区域.txt', 'r', encoding='utf-8')
    lines = file.readlines()
    citycodes = set()
    for line in lines:
        line = line.split('\t')
        citycodes.add(line[3][0:4])
    citycodes = list(citycodes)
    citycodes.sort()
    print(citycodes)

    for citycode in citycodes:
        print(citycode)
      #if citycode=='6403':
        url = "http://39.104.59.235:8090/geoserver/sp_rivermap_district_names/ows?SERVICE=WFS&OutputFormat=json&VERSION=1.0.0&REQUEST=GetFeature&TYPENAME=sp_rivermap_district_names:layer_street&STARTINDEX=0&MAXFEATURES=2000&FILTER=%3CFilter%20xmlns%3D%22http:%2F%2Fwww.opengis.net%2Fogc%22%20xmlns:gml%3D%22http:%2F%2Fwww.opengis.net%2Fgml%22%3E%3CAnd%3E%3CAnd%3E%3CPropertyIsLike%20wildCard%3D%27%2A%27%20singleChar%3D%27_%27%20escape%3D%27%21%27%3E%3CPropertyName%3Epadcode%3C%2FPropertyName%3E%3CLiteral%3E"+citycode+"%2A%3C%2FLiteral%3E%3C%2FPropertyIsLike%3E%3CPropertyIsGreaterThan%3E%3CPropertyName%3Elevel%3C%2FPropertyName%3E%3CLiteral%3E4%3C%2FLiteral%3E%3C%2FPropertyIsGreaterThan%3E%3C%2FAnd%3E%3CPropertyIsLessThanOrEqualTo%3E%3CPropertyName%3Elevel%3C%2FPropertyName%3E%3CLiteral%3E5%3C%2FLiteral%3E%3C%2FPropertyIsLessThanOrEqualTo%3E%3C%2FAnd%3E%3C%2FFilter%3E"
        try:
            res = rq.get(url, timeout=(3, 9),headers=headers)
            restext = res.text
            #print(restext)
            resjson = eval(restext)
            num = len(resjson["features"])
            #print(num)
            for i in range(0, num):
                #print(resjson["features"][i])
                with open('D:\Data\output\streetlayer.txt', 'a', encoding='utf-8') as fw:
                    fw.write(resjson["features"][i]["properties"]["Name"] + sep +
                             resjson["features"][i]["properties"]["adcode"] + sep +
                             resjson["features"][i]["properties"]["padcode"] + sep +
                             citycode + '\n'
                             )
        except Exception as e:
            with open('D:\Data\output\streetlayer_log.txt','a', encoding='utf-8', errors='ignore') as fw:
                fw.write(citycode+'\n')
    ###台湾
    urltw = "http://39.104.59.235:8090/geoserver/sp_rivermap_district_names/ows?SERVICE=WFS&OutputFormat=json&VERSION=1.0.0&REQUEST=GetFeature&TYPENAME=sp_rivermap_district_names:layer_street&STARTINDEX=0&MAXFEATURES=2000&FILTER=%3CFilter%20xmlns%3D%22http:%2F%2Fwww.opengis.net%2Fogc%22%20xmlns:gml%3D%22http:%2F%2Fwww.opengis.net%2Fgml%22%3E%3CAnd%3E%3CAnd%3E%3CPropertyIsLike%20wildCard%3D%27%2A%27%20singleChar%3D%27_%27%20escape%3D%27%21%27%3E%3CPropertyName%3Epadcode%3C%2FPropertyName%3E%3CLiteral%3E"+str(71)+"%2A%3C%2FLiteral%3E%3C%2FPropertyIsLike%3E%3CPropertyIsGreaterThan%3E%3CPropertyName%3Elevel%3C%2FPropertyName%3E%3CLiteral%3E4%3C%2FLiteral%3E%3C%2FPropertyIsGreaterThan%3E%3C%2FAnd%3E%3CPropertyIsLessThanOrEqualTo%3E%3CPropertyName%3Elevel%3C%2FPropertyName%3E%3CLiteral%3E5%3C%2FLiteral%3E%3C%2FPropertyIsLessThanOrEqualTo%3E%3C%2FAnd%3E%3C%2FFilter%3E"
    restw = rq.get(urltw, timeout=(3, 9),headers=headers)
    restwtext = restw.text
    #print(restext)
    restwjson = eval(restwtext)
    numtw = len(restwjson["features"])
    #print(num)
    for n in range(0, numtw):
        #print(resjson["features"][i])
        with open('D:\Data\output\streetlayer.txt', 'a', encoding='utf-8') as fw:
            fw.write(zhconv.convert(restwjson["features"][n]["properties"]["Name"], 'zh-cn' )+ sep +
                     restwjson["features"][n]["properties"]["adcode"] + sep +
                     restwjson["features"][n]["properties"]["padcode"] + sep +
                     restwjson["features"][n]["properties"]["padcode"][0:4] + '\n'
                    )




