from matplotlib.patches import Polygon
import requests as rq
import json
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
import TransCooSys
import threading
import time
import queue
from jsonpath import jsonpath
import os
#### 高德地图——爬取全国POI名称 ####

## 关键字API：https://restapi.amap.com/v3/place/text?parameters
## 测试： https://restapi.amap.com/v3/place/text?key=您的key&keywords=&types=010000&city=469006&children=1&offset=50&page=4&extensions=all

## 代码流程
# 1 读取key文件，每次访问计数，4000次更换key
# 2 读取城市列表，提取最小行政区划代号，生成目标文件
# 3 读取poi类别文件，开始执行爬虫
# 4 爬虫逻辑： keywords置空，传入type，每次返回50个，页数递增，当返回数不足50时，执行下一个类型

# reuest
def get_json_http(url):
    try:
        r = rq.get(url, timeout=(3, 9))
        json_data = json.loads(r.text)
        return json_data
    except Exception as e:
        time.sleep(3)
        return get_json_http(url)

## 获取区县代号
def get_city_code():
    with open('resources/admin_divisions.json', 'r', encoding='utf-8') as f:
        admin_divisions = json.load(f)
    city_list = []
    for ad in admin_divisions:
        citys = admin_divisions[ad]['citys']
        for city in citys:
            if ad in {'北京市', '天津市', '上海市', '重庆市', '香港特别行政区', '澳门特别行政区'}:
                city_list.append(city.split('_')[1]+'\t'+city.split('_')[0])
            else:
                cname = list(city.keys())[0] 
                areas = city[cname]['areas']
                if len(areas) == 0:
                    city_list.append(city[cname]['acode']+'\t'+cname)
                else:
                    city_list.extend([area.split('_')[1]+'\t'+area.split('_')[0] for area in areas])
    with open('resources/poi_citycode', 'w', encoding='utf-8') as f:
        for city in  city_list:
            f.write(city+'\n')


# 加载用户key
def get_amap_key(filepath):
    amapkey_dict = dict()
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            list = line.split('\t')
            amapkey = list[0]
            isUse = list[1]
            if isUse and isUse == '1':
                amapkey_dict[amapkey] = 1
            else:
                amapkey_dict[amapkey] = 0
    return amapkey_dict

# 更换用户key
def get_userkey(amap_keys):
    userkey = ''
    for uk in amap_keys:
        if amap_keys[uk] != 1:
           userkey =  uk
           break
    return userkey


def get_firstfield_fromfile(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        reslist = [line.replace('\n','').split('\t')[0] for line in lines]
        return reslist

## 关键字搜索API 2.0
def crawl_poiname_once2(userkey, poitype, citycode, pagenum, offset):
    url = "https://restapi.amap.com/v5/place/text?key={}&keywords=&types={}&region={}&children=1&city_limit=true&show_fields=children&page_size={}&page_num={}".format(
        userkey, poitype, citycode, offset, pagenum
    )
    json_data = get_json_http(url)
    if 'info' in json_data and 'USER_DAILY_QUERY_OVER_LIMIT' == json_data['info']:
        return [], -1
    try:
        count = int(json_data['count']) if json_data['count'] else 0
        pois = json_data['pois']
        poi_res_list = []
        for poi in pois:
            name = poi['name']
            address = str(poi['address']) if 'address' in poi else ''
            adcode = poi['adcode']
            ptype = poi['type']
            poi_res_list.append([name, address, adcode, ptype])
    except Exception as e:
        with open('crawl_output/poiname/crawl_error.log', 'a', encoding='utf-8') as f:
            f.writelines(poitype+'\t'+citycode+'\n')
        return [], 0
    return poi_res_list, count

citycode_dict = dict()
with open('resources/gaode/poi_type_all.txt', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    reslist = []
    for line in lines:
        llist = line.replace('\n','').split('\t')
        citycode_dict[llist[0]]=llist[3]

## 关键字搜索API 1.0
def crawl_poiname_once(userkey, poitype, citycode, pagenum, offset):
    url = "https://restapi.amap.com/v3/place/text?key={}&keywords=&types={}&city={}&citylimit=true&children=1&offset={}&page={}&extensions=all".format(
        userkey, poitype, citycode, offset, pagenum
    )
    json_data = get_json_http(url)
    if 'info' in json_data and 'USER_DAILY_QUERY_OVER_LIMIT' == json_data['info']:
        return [], -1
    try:
        count = int(json_data['count']) if json_data['count'] else 0
        pois = json_data['pois']
        poi_res_list = []
        for poi in pois:
            name = poi['name']
            address = str(poi['address']) if 'address' in poi else ''
            ptype = poi['type']
            adcode = poi['adcode']
            if 'children' in poi and len(poi['children']) > 0:
                children = poi['children']
                for cc in children:
                    cadd = str(cc['address']).strip()
                    cname = str(cc['name']).strip()
                    typecode = str(cc['typecode']).strip()
                    tpyeres = citycode_dict[typecode] if typecode in citycode_dict else typecode
                    poi_res_list.append([cname, cadd, adcode, tpyeres, name])
            poi_res_list.append([name, address, adcode, ptype])
    except Exception as e:
        with open('crawl_output/poiname/crawl_error.log', 'a', encoding='utf-8') as f:
            f.writelines(poitype+'\t'+citycode+'\n')
        return [], 0
    return poi_res_list, count
#crawl_poiname_once('ac23bcb00bf5ae622c25666435434203', '010000', '330108', '1')     
        

## 爬虫程序
def crawl_poiname_process(resource_dir):
    amap_keys = get_amap_key(os.path.join(resource_dir, 'gaode_key.txt'))
    type_list = get_firstfield_fromfile(os.path.join(resource_dir, 'poi_type_small.txt')) 
    citycode_list = get_firstfield_fromfile(os.path.join(resource_dir, 'poi_citycode.txt'))
    city_complete_list = set(get_firstfield_fromfile('crawl_output/poiname/city_success.txt'))
    offset = 50
    keymax = 4000
    crawl_result = []
    userkey = get_userkey(amap_keys)
    key_num = 0
    totalcount = 0
    for citycode in citycode_list:
        # 跳过已经爬取的城市
        if citycode in city_complete_list:
            continue
        for poitype in type_list:
            isContinue = True
            pagenum = 1
            count = 0
            while isContinue:
                poi_res_list, countonce = crawl_poiname_once(userkey, poitype, citycode, str(pagenum), str(offset))
                crawl_result.extend(poi_res_list)
                if key_num >= keymax or count == -1:
                    amap_keys[userkey] = 1
                    userkey = get_userkey(amap_keys)
                    print(userkey)
                    key_num = 0
                    if not userkey or userkey == '':
                        print('缺少可用的key')
                        return
                    continue
                if len(poi_res_list) == 0:
                    isContinue = False
                if countonce != 0:
                    count = countonce
                pagenum += 1
                key_num += 1
            totalcount += count
        print(totalcount)
        with open('crawl_output/poiname/all_poiname_sub.txt', 'a', encoding='utf-8') as tf:
            for cr in crawl_result:
                tf.writelines('\t'.join(cr)+'\n')
            crawl_result = []
        with open('crawl_output/poiname/city_success.txt', 'a', encoding='utf-8') as cf:
            cf.writelines(citycode+'\n')


## 获取poi中类、小类
def get_small_poitype():
    with open('resources/poi_type_all.txt', 'r', encoding='utf-8') as cf:
        lines = cf.readlines()
        small_type_list = []
        mid_type_dict = {}
        for line in lines:
            llist = line.split('\t')
            tcode = llist[1]
            if tcode[2:6] == '0000':
                continue
            midcode = tcode[0:4]
            if midcode in mid_type_dict:
                mid_type_dict[midcode] = mid_type_dict[midcode] + 1
            else:
                mid_type_dict[midcode] = 1
        for line in lines:
            llist = line.split('\t')
            tcode = llist[1]
            if tcode[2:6] == '0000':
                continue
            if tcode[4:6] == '00' and mid_type_dict[tcode[0:4]] > 1:
                continue
            small_type_list.append(line)
        with open('resources/poi_type_small.txt', 'w', encoding='utf-8') as cf:
            for t in small_type_list:
                cf.writelines(t)
    with open('resources/poi_type_all.txt', 'r', encoding='utf-8') as cf:
        lines = cf.readlines()
        small_type_list = []
        for line in lines:
            llist = line.split('\t')
            tcode = llist[1]
            if tcode[2:6] != '0000' and tcode[4:6] == '00':
                small_type_list.append(line)
        with open('resources/poi_type_mid.txt', 'w', encoding='utf-8') as cf:
            for t in small_type_list:
                cf.writelines(t)

# typenum_dict = {}
# area_dict = {}
# with open('crawl_output/poiname/滨江_poiname_polygon.txt', 'r', encoding='utf-8') as tf:
#     lines = tf.readlines()
#     for line in lines:
#         llist = line.split('\t')
#         adcode = llist[2]
#         poitype = llist[3]
#         bigtype = poitype.split(';')[0]
#         if adcode == '330108':
#             if bigtype in typenum_dict:
#                 typenum_dict[bigtype] = typenum_dict[bigtype] + 1
#             else:
#                 typenum_dict[bigtype] = 1
#         if adcode in area_dict:
#                 area_dict[adcode] = area_dict[adcode] + 1
#         else:
#             area_dict[adcode] = 1
#     for td in typenum_dict:
#         print(td+"\t"+str(typenum_dict[td]))
#     for td in area_dict:
#         print(td+"\t"+str(area_dict[td]))

import geopandas
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes
import geohash
from shapely import geometry


def gis_map_cl(gis_data,fx=False):
    jwds=str(gis_data['geometry'][0]).replace('MULTIPOLYGON (((','').replace(')))','')
    tmp_points=jwds.split(', ')
    points=[]
    for tmp_point in tmp_points:
        tmp=tmp_point.split(' ')
        if fx:
            points.append([float(tmp[1]),float(tmp[0])])
        else:
            points.append([float(tmp[0]),float(tmp[1])])
    return points

    
def generate_geohashes(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        data = geopandas.read_file(f)
        points = gis_map_cl(data, False)
    polygon = geometry.Polygon(points)
    geohash6_outer = polygon_to_geohashes(polygon, 6, False)
    geolist = []
    for g6 in geohash6_outer:
        box = geohash.bbox(g6)
        # newlist = [[box['s'], box['w']]
        # ,[box['s'], box['e']]
        # ,[box['n'], box['e']]
        # ,[box['n'], box['w']]
        # ,[box['s'], box['w']]]
        #geolist.append(newlist)
        #newlist2 = ';'.join([str(coo[1])[0:10]+','+str(coo[0])[0:9] for coo in newlist])
        coorstr = str(box['w'])[0:10]+','+str(box['n'])[0:9]+';'+str(box['e'])[0:10]+','+str(box['s'])[0:9]
        geolist.append(coorstr)
    return geolist



def crawl_polygon_once(userkey, polygon, poitype, pagenum, offset):
    url = "https://restapi.amap.com/v3/place/polygon?key={}&polygon={}&keywords=&types={}&offset={}&page={}&extensions=all".format(
        userkey, polygon, poitype, offset, pagenum
    )
    json_data = get_json_http(url)
    if 'info' in json_data and 'USER_DAILY_QUERY_OVER_LIMIT' == json_data['info']:
        return [], -1
    try:
        count = int(json_data['count']) if json_data['count'] else 0
        pois = json_data['pois']
        poi_res_list = []
        for poi in pois:
            name = poi['name']
            address = str(poi['address']) if 'address' in poi else ''
            ptype = poi['type']
            adcode = poi['adcode']
            if 'children' in poi and len(poi['children']) > 0:
                children = poi['children']
                for cc in children:
                    cadd = str(cc['address']).strip()
                    cname = str(cc['name']).strip()
                    typecode = str(cc['typecode']).strip()
                    if typecode in citycode_dict:   
                        tpyeres = citycode_dict[typecode]
                    else:
                        tpyeres = typecode
                    poi_res_list.append([cname, cadd, adcode, tpyeres, name])
            poi_res_list.append([name, address, adcode, ptype])
    except Exception as e:
        with open('crawl_output/poiname/crawl_error.log', 'a', encoding='utf-8') as f:
            f.writelines(polygon+'\n')
        return [], 0
    return poi_res_list, count

def crawl_polygon_process():
    amap_keys = get_amap_key('resources/gaode_key.txt')
    geolist = generate_geohashes('gaode/全国区划边界数据(gaode)/33_浙江省/330106_西湖区.json')
    #type_list = get_firstfield_fromfile('resources/poi_type_small.txt') 
    type_list = [
        "010000|020000|030000|040000|050000|060000",
        "070000|080000|090000|100000|110000|120000",
        "130000|140000|150000|160000|170000|180000",
        "190000|200000|220000|970000|990000"]
    userkey = get_userkey(amap_keys)
    crawl_result =[]
    offset = 50
    keymax = 4000
    key_num = 0
    totalcount = 0

    for geo in geolist:
        for poitype in type_list:
            isContinue = True
            pagenum = 1
            count = 0
            while isContinue:
                poi_res_list, count = crawl_polygon_once(userkey, geo, poitype, pagenum, offset)
                crawl_result.extend(poi_res_list)
                if key_num >= keymax or count == -1:
                    amap_keys[userkey] = 1
                    userkey = get_userkey(amap_keys)
                    key_num = 0
                    count = 0
                    if not userkey or userkey == '':
                        print('缺少可用的key')
                        return
                    continue
                if len(poi_res_list) == 0:
                    isContinue = False
                pagenum += 1
                key_num += 1
                totalcount = totalcount + count
    print(totalcount)
    with open('crawl_output/poiname/滨江_poiname_polygon.txt', 'a', encoding='utf-8') as tf:
        for cr in crawl_result:
            tf.writelines('\t'.join(cr)+'\n')
        crawl_result = []


#crawl_polygon_process()
#crawl_polygon_once('ac23bcb00bf5ae622c25666435434203', '', '150000', '10', '0')
url = "https://restapi.amap.com/v3/place/text?key={}&keywords={}&types={}&city={}&offset={}&page={}&extensions=all".format(
    'ac23bcb00bf5ae622c25666435434203', '地铁1号线', '150000', '330100', '10', '0'
)
url = "https://restapi.amap.com/v5/direction/transit/integrated?key={}&keywords={}&types={}&city={}&offset={}&page={}&extensions=all".format(
    'ac23bcb00bf5ae622c25666435434203', '地铁1号线', '150000', '330100', '10', '0'
)
url = 'https://restapi.amap.com/v3/bus/linename?s=rsv3&extensions=all&key=a5b7479db5b24fd68cedcf24f482c156&output=json&city={}&offset=1&keywords={}&platform=JS'.format('杭州市','1路')
json_data = get_json_http(url)
print(1)
