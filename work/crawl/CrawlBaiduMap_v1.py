# 经纬度数组转geometry对象
from ast import keyword
import sys
# import imp
from unittest import result
from gevent import sleep
from matplotlib.pyplot import get
from numpy import var
from shapely import geometry
# geojson读取组件
import geopandas
# 区域多边形转geohash，geohash转多边形组件
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes, geohashes_to_polygon
# 页面请求组件
import requests as rq
# json解析组件
import json
# xml、html解析组件
from lxml import etree
# geohash工具组件
import geohash
# 地图html生成组件
import folium
from folium import plugins
# jupyter notebook 嵌入html组件
from IPython.display import IFrame

from jsonpath import jsonpath
from urllib.parse import quote
from urllib3 import Retry
import sys
from pathlib import Path
#sys.path.append(str(Path(__file__).resolve().parents[1]))
import TransCooSys
import threading
import time
import queue
from fake_useragent import UserAgent
import os

bsc_city = {}
with open("resources/bsc_city.json", 'r', encoding='utf-8') as bf:
    bsc_city = json.load(bf)

headers = {
    'content-encoding': 'gzip',
    'content-type': 'application/json; charset=utf-8',
    'date': 'Thu, 30 Jun 2022 02:00:14 GMT',
    'eagleeye-traceid': '2120411616565544142367237e5dd0',
    'etag': 'W/"6b84-E80ZTyqRrvSrPeDtKzXQeSMPMs8"',
    'gsid': '033040076182165655441424000017286016656',
    'server': 'Tengine',
    'strict-transport-security': 'max-age=31536000',
    'timing-allow-origin': '*',
    'vary': 'Accept-Encoding',
    'x-powered-by': 'Express',
    # 'user-agent': 'user-agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'user-Agent': UserAgent().Chrome,
    'cookie': 'cna=yvuRGuMrU2MCASeqCfOWmGT/; _uab_collina=165641408901798700454793; xlly_s=1; passport_login=NzYzMzE3MzQ0LGFtYXBLNU80VjEwZiwzZ2wzY3RjZ3VwbDZxaWJlamlzbWFyZWpqbGN3Z3p6aywxNjU2NTUxNDI4LFpHWmtZalpoTURSbVlqWmpNRFEwWlRWbE1tRTBZMlUwWkdOaE1EZ3pZMlE9; oauth_state=f874984e25f48ef13f4ecaac8bb9d989; guid=1ad9-d4f8-f32a-0e6d; gray_auth=2; dev_help=hvfQi6DG%2F3uEKZnfOpvlNTU1Zjc5YTBiMDlmMjZlMmVjYjMyYmI5NmYzNDJmNWZlNDhhY2YxNjdjMWY4MmUyYjZmMmM5YTA3ZDU3NWU4M2LSrjQSdQrxdIPhQxGrHIAB6Qqw2dLuXryqtn9%2F%2B1tkY1dcSCtNZJzxDaZgg%2FlMU3%2BasVffu%2F%2BCOGn1zzOMP69snDQmQ5gULchwMD90BnykxVGkYprOB6krfQmkX8SQwdM%3D; x5sec=7b227761676272696467652d616c69626162612d616d61703b32223a2238663764386234393165313261303166616137636132616535663030396630394350475739705547454f7139766f616c37636a334b4444656c35757041773d3d227d; x-csrf-token=21ef69cff417f63b0dac3cfd8ec40354; tfstk=cBzlB0VYauoSEIo0fag5J1PJatCAZxOqeJy7u_W0oyDxgXaViAK2bxMCmxX1UH1..; l=eBStVEmgLlAoKOO2BOfwnurza77tIIRAguPzaNbMiOCPOLC95VYCW6bIzAYpCnGVhsGpR3PqcrtpBeYBqQAonxvT8NcZJVkmn; isg=BENDs2CEJReK6ulkui67igtg0gftuNf6Li9F5XUgmaIZNGNW_YjeSj-mqsR6lC_y'
}


## 生成纵/横向经纬度数组，用于转geometry对象
def gis_http_poiinfo(gis_data, fx=False):
    jwds = gis_data
    points = []
    for tmp in jwds:
        if fx:
            points.append([tmp[1], tmp[0]])
        else:
            points.append([tmp[0], tmp[1]])
    return points


## 获取多边形区域内geohash
def geohash6_from_json(data):
    points = gis_http_poiinfo(data, False)
    polygon = geometry.Polygon(points)
    geohash6_outer = polygon_to_geohashes(polygon, 6, False)
    return geohash6_outer


def generate_map(geohash6_outer, points2, url):
    box = geohash.bbox('wtm7mx')
    ## 生成地图html界面
    heatmap1 = folium.Map(  ##高德底图
        location=[30.182623, 120.15201],
        zoom_start=13,
        control_scale=True,
        tiles='http://webrd02.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={x}&y={y}&z={z}',
        attr='&copy; <a href="http://ditu.amap.com/" rel="external nofollow" >高德地图</a>'
    )
    # 创建中心标记
    folium.Marker([30.182623, 120.15201], popup='<i>设置中心点</i>', icon=folium.Icon(icon='cloud', color='green')).add_to(
        heatmap1)
    ## 循环读取geohash，并画网格叠加地图上
    for g6 in geohash6_outer:
        box = geohash.bbox(g6)
        xy_info = geohash.decode(g6)
        folium.PolyLine(locations=[[box['s'], box['w']], [box['s'], box['e']]
            , [box['n'], box['e']], [box['n'], box['w']]
            , [box['s'], box['w']]
                                   ],
                        color='black').add_to(heatmap1)
    ## 叠加行政区划边界
    folium.PolyLine(locations=points2, color='red').add_to(heatmap1)
    heatmap1.save("baidu/map_baidu_" + url + ".html")  ##生成网
    print("success")


## 发送http请求，获取json
def get_json_http(url):
    # url='https://ditu.amap.com/detail/get/detail?id=' + url
    try:
        r = rq.get(url, timeout=(3, 9))
        json_data = json.loads(r.text)
        return json_data
    except Exception as e:
        time.sleep(3)
        return get_json_http(url)


## request保存图片
def save_img(save_dir, uname, image_url):
    # rq_img = rq.get(image_url, timeout=(3, 9))
    # image = rq_img.content
    # if not os.path.exists(save_dir):
    #     os.makedirs(save_dir)
    # save_path = os.path.join(save_dir, uname + '.jpg')
    # with open(save_path, 'wb') as ff:
    #     ff.write(image)
    # return save_path
    return ''


## 解析geo标签获取经纬度数组，转换wgs842坐标系，
def get_json_badui(data_AOI):
    points_all = []
    try:
        if 'geo' in data_AOI['content']:
            geo_AOI = data_AOI['content']['geo']
            points_all = get_json_baidu_impl(geo_AOI)
        elif 'geo' in data_AOI['content'][0]:
            geo_AOI = data_AOI['content'][0]['geo']
            points_all = get_json_baidu_impl(geo_AOI)
    except Exception as e:
        print("经纬度解析失败", e)
        return points_all
    return points_all


def get_json_baidu_impl(geo_AOI):
    points_all = []
    geo_AOI = geo_AOI.split('|')
    point = geo_AOI[2].split(",")
    point_transform = []
    for i in range(int(len(point) / 2)):
        # 第一个点的x坐标删除‘1-’
        if i == 0 and point[0][:2] == '1-':
            point[2 * i] = point[2 * i][2:]
        # 最后的点的y坐标删除‘;’
        if i == int((len(point) / 2) - 1):
            point[2 * i + 1] = point[2 * i + 1][:-1]
        point_Mecator2BD09 = TransCooSys.mercatortobd09(float(point[2 * i]), float(point[2 * i + 1]))
        lon = str(point_Mecator2BD09[0])[:10]
        lat = str(point_Mecator2BD09[1])[:9]
        point_gcj02 = TransCooSys.bd09_to_gcj02(float(lon), float(lat))
        point_wgs84 = TransCooSys.gcj02_to_wgs84(point_gcj02[0], point_gcj02[1])
        point_transform.append(point_wgs84)
    point_str = ''
    for j in range(len(point_transform)):
        point_str = (str(point_transform[j])).replace(' ', '')[1:-1]
        points_all.append(point_str)
    return points_all


## 解析覆盖面poi数据
def parse_poi_baidu(content, save_dir, keywords):
    result = []
    profile_geo = content['profile_geo']
    ## 获取经纬度数据
    points_all = get_json_baidu_impl(profile_geo)
    if len(points_all) == 0:
        return result
    data = []
    for dd in points_all:
        lon, lat = dd.split(",")[0][:10], dd.split(",")[1][:9]
        data.append([float(lon), float(lat)])
    # 提取地名, 详细地址 ,标签信息, 图片，联系方式
    try:
        addr = content['addr']
        di_tag = content['di_tag']
        tel = content['tel'] if 'tel' in content else ''
        ## 获取省,市,区
        api_admin_info = content['api_admin_info']
        prov_name = api_admin_info['prov_name'] if 'prov_name' in api_admin_info else ''
        city_name = api_admin_info['city_name']
        area_name = api_admin_info['area_name'] if 'area_name' in api_admin_info else ''
        area_id = content['admin_info']['area_id'] if 'area_id' in content['admin_info'] else ''
        uname = prov_name + city_name + area_name + content['name']
        save_dir = os.path.join(save_dir, prov_name, city_name, area_name)
        image_url = content['ext']['detail_info']['image']
        ## 保存图片
        save_path = save_img(save_dir, uname, image_url)
        result.append(uname)
        result.append(addr)
        result.append(di_tag)
        result.append(tel)
        result.append(save_path)
        result.append(data)
        result.append(area_id)
    except Exception as e:
        print(keywords + ': 解析数据失败; ' + e)
        return result
    return result


## 解析公交线路数据
def parse_route_data(uids, keywords, c, auth, dir, prov_name, city_name):
    result = []
    for uid in uids:
        url_id = 'https://map.baidu.com/?qt=bsl&tps=&newmap=1&uid=' + uid + '&c=' + c + '&auth=' + auth
        json_data = get_json_http(url_id)
        points_all = get_json_badui(json_data)
        if len(points_all) == 0:
            print(c + ' : ' + keywords + '\t' + '未找到公交线路-url2' + '\n')
            continue
        data = []
        for dd in points_all:
            lon, lat = dd.split(",")[0][:10], dd.split(",")[1][:9]
            data.append([float(lon), float(lat)])
        ## 获取详细信息, 线路名、方向、所属公司、时刻表、发车间隔、各站点信息（站点名称；经纬度；图片）
        content = json_data['content'][0]
        bus_route = []
        try:
            line_name = content['name']
            line_direction = content['line_direction']
            company = content['company']
            timetable = content['timetable']
            workingTimeDesc = content['workingTimeDesc']
            stations = []
            ## 解析各站点信息
            for stat in content['stations']:
                stat_geo = stat['geo']
                gaode_geo = get_json_baidu_impl(stat_geo)[0].split(",")
                lon, lat = gaode_geo[0][:10], gaode_geo[1][:9]
                stat_name = stat['name']
                ## 根据经纬度请求pid，再根据pid获取图片
                # pxy = stat_geo.split('|')[2].split(';')[0].split(',')
                # url_pid = 'https://mapsv0.bdimg.com/?qt=qsdata&x=' + pxy[0] + '&y=' + pxy[1]
                # json_pid = get_json_http(url_pid)
                # if 'content' in json_pid and 'id' in json_pid['content']:
                #     pid = json_pid['content']['id']
                #     url_pano = 'https://mapsv0.bdimg.com//pr/?qt=prv&panoid=' + pid + '&width=323&height=101&quality=80&fovx=250&heading=142.23983739940792&udt=20200825&from=PC'
                #     if '/' in line_name:
                #         line_name = line_name.replace('/', '-')
                #     save_dir = os.path.join(dir, line_name)
                #     if '/' in stat_name:
                #         stat_name = stat_name.replace('/', '-')
                #     save_path = save_img(save_dir, stat_name, url_pano)
                #     stations.append(stat_name + ';' + lon + ',' + lat + ';' + save_path)
                # else:
                stations.append(stat_name + ';' + lon + ',' + lat + ';')
            if prov_name:
                bus_route.append(prov_name)
            else:
                bus_route.append(city_name)
            if city_name in bsc_city:
                city_code = bsc_city[city_name]
            bus_route.append(city_name)
            bus_route.append(city_code)
            bus_route.append(line_name)
            bus_route.append(line_direction)
            bus_route.append('公交')
            bus_route.append(company)
            bus_route.append(timetable)
            bus_route.append(''.join(workingTimeDesc))
            bus_route.append(stations)
            bus_route.append(data)
            # polygon = geometry.Polygon(data)
            # geohash6_outer=polygon_to_geohashes(polygon, 6, False)
            # generate_map(geohash6_outer, data2, line_name)
        except Exception as e:
            print(e + 'c={}  keywords = {}  city_code={}'.format(c, keywords, city_code))
        result.append(bus_route)
        time.sleep(1)
    return result


## 解析地铁线路数据
def parse_subway_data(uids, keywords, c, prov_name, city_name):
    result = []
    for uid in uids:
        url_id = 'https://map.baidu.com/?qt=bsl&tps=&newmap=1&uid=' + uid + '&c=' + c
        json_data = get_json_http(url_id)
        ## 获取经纬度数组
        points_all = get_json_badui(json_data)
        if len(points_all) == 0:
            print(keywords + '\t' + '未找到地铁线路' + '\n')
            continue
        data = []
        for dd in points_all:
            lon, lat = dd.split(",")[0][:10], dd.split(",")[1][:9]
            data.append([float(lon), float(lat)])
        ## 获取详细信息
        content = json_data['content'][0]
        subway = []
        line_name = ''
        stations = []
        try:
            line_name = content['name']
            line_direction = content['line_direction']
            company = content['company']
            timetable = content['timetable']
            workingTimeDesc = content['workingTimeDesc']
            ## 遍历所有站点信息
            for stat in content['stations']:
                stat_geo = stat['geo']
                gaode_geo = get_json_baidu_impl(stat_geo)[0]
                lon, lat = gaode_geo.split(",")[0][:10], gaode_geo.split(",")[1][:9]
                stations.append(stat['name'] + ';' + lon + ',' + lat)
        except Exception as e:
            print(keywords + ": json解析失败; ", e)
            return
        if prov_name:
            subway.append(prov_name)
        else:
            subway.append(city_name)
        if city_name in bsc_city:
            city_code = bsc_city[city_name]
        subway.append(city_name)
        subway.append(city_code)
        subway.append(line_name)
        subway.append(line_direction)
        subway.append('地铁')
        subway.append(company)
        subway.append(timetable)
        subway.append(''.join(workingTimeDesc))
        subway.append(stations)
        subway.append(data)
        result.append(subway)
        time.sleep(1)
    return result


def parse_lines_baidu(json_id_data, save_dir, keywords, c):
    result = []
    route_uids = []
    subway_uids = []
    ## 提取线路id
    try:
        for cc in json_id_data['content']:
            if cc['acc_flag'] == 1 and 'cp' in cc and cc['cp'] == 'bus':
                if '地铁' in cc['cla'][1][1] or '轻轨' in cc['cla'][1][1]:
                    subway_uids.append(cc['uid'])
                elif '公交' in cc['cla'][1][1]:
                    route_uids.append(cc['uid'])
    except Exception as e:
        print(c + ' : ' + keywords + ": 获取线路失败; ", e)
        return result
    if len(route_uids) == 0 and len(subway_uids) == 0:
        print(c + ' ： ' + keywords + '   未找到公交线路-url1')
        return result
    ## 获取省市区
    api_admin_info = json_id_data['content'][0]['api_admin_info']
    prov_name = api_admin_info['prov_name'] if 'prov_name' in api_admin_info else ''
    city_name = api_admin_info['city_name']
    area_name = api_admin_info['area_name'] if 'area_name' in api_admin_info else ''
    save_dir = os.path.join(save_dir, prov_name, city_name, area_name)
    auth = json_id_data['result']['auth']
    ## 根据uid爬取地铁或公交
    if len(subway_uids) > 0:
        result = parse_subway_data(subway_uids, keywords, c, prov_name, city_name)
    if len(route_uids) > 0:
        result = parse_route_data(route_uids, keywords, c, auth, save_dir, prov_name, city_name)
    return result


## 爬虫主方法,区域面
def crawl_poi_impl(keywords, save_dir='baidu\poi_img'):
    result = []
    url_name = 'https://map.baidu.com/?newmap=1&qt=s&da_src=searchBox.button&wd=' + keywords.replace('\n','') + '&c=268'
    json_id_data = get_json_http(url_name)
    content = json_id_data['content'][0]
    if 'profile_geo' in content:
        if content['acc_flag'] == 0 or len(content['profile_geo']) == 0:
            print(keywords + '\t' + '未找到区域面' + '\n')
            return result
        result = parse_poi_baidu(content, save_dir, keywords)
    return result


## 爬虫主方法， 公交地铁线路
def crawl_route_and_subway(keywords, c='179', save_dir=r'D:\baidu\lines_img'):
    # url_name = 'https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=baidu&pcevaname=pc4.1&qt=s&da_src=shareurl&wd=' + keywords + '&c=' + c
    url_name = 'https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=after_baidu&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=' + keywords + '&c=' + c
    json_id_data = get_json_http(url_name)
    result = []
    if 'content' in json_id_data:
        result = parse_lines_baidu(json_id_data, save_dir, keywords, c)
    return result


#result = crawl_route_and_subway('42路', '257')
#print(result)
#print(len(result))
crawl_poi_impl("杭州萧山国际机场")