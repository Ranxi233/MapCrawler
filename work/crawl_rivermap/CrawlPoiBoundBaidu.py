import re
from unicodedata import name
import requests
import json
import time
import geopandas
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes
import geohash
from shapely import geometry
import TransCooSys
import os
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests import adapters

def req_poi_info(url=None,cs_cnt=0):
    try:
        r=requests.get(url=url,timeout=(3,9))
        json_data=json.loads(r.text)
        return json_data
    except:
        if cs_cnt>4:
            return None
        time.sleep(2)
        return req_poi_info(url=url,cs_cnt=(cs_cnt+1))


def get_firstfield_fromfile(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        reslist = [line.replace('\n','').split('\t')[0] for line in lines if '1' not in line.split('\t')[-1]]
        return reslist

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
    geohash6_outer = polygon_to_geohashes(polygon, 5, False)
    geolist = []
    geolist2 = []
    for g6 in geohash6_outer:
        box = geohash.bbox(g6)
        yxj09 = TransCooSys.wgs84_to_bd09(box['w'],box['s'])
        zsj09 = TransCooSys.wgs84_to_bd09(box['e'],box['n'])
        # yxj09 = TransCooSys.wgs84_to_bd09(box['w'],box['n'])
        # zsj09 = TransCooSys.wgs84_to_bd09(box['e'],box['s'])
        yxj = TransCooSys.bd09tomercator(yxj09[0], yxj09[1])
        zsj = TransCooSys.bd09tomercator(zsj09[0], zsj09[1])
       # geolist.append([str(yxj[0])+','+str(yxj[1])+','+str(zsj[0])+','+str(zsj[1]), g6])

        geolist.append([str(round(yxj[0],6))+','+str(round(yxj[1],6))+';'+str(round(zsj[0],6))+','+str(round(zsj[1],6)), g6])
        geolist2.append([str(yxj09[1])+' '+str(yxj09[0]), str(zsj09[1])+' '+str(zsj09[0]), g6])
    return geolist, geolist2


def get_url(keyword='公司',qy=None,pagenum='0'):
    url='http://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=after_baidu&pcevaname=pc4.1&qt=s&da_src=searchBox.button'
    url+='&wd='+keyword+'&c=179&src=0&wd2=&sug=0&l=21&b=('+qy+')&rn=50'
    url+='&from=webmap&biz_forward={%22scaler%22:1,%22styles%22:%22pl%22}&sug_forward='
    url+='&device_ratio=1&tn=B_NORMAL_MAP&nn=0ie=utf-8&t=1655540635226&newfrom=zhuzhan_webmap'
    url+=('&pn='+pagenum)
    return url

def parse_content_data(contents):
    pois_list = []
    for content in contents:
        name = content['name']
        addr = content['addr']
        address_norm = content['address_norm'] if 'address_norm' in content else ''
        std_tag = content['std_tag'] if 'std_tag' in content else ''
        tel = content['tel'] if 'tel' in content else ''
        geo = content['geo'] if 'geo' in content else ''
        image = ''
        guoke_geo = ''
        if 'detail_info' in content['ext']:
            detail_info = content['ext']['detail_info']
            if 'image' in detail_info:
                image = detail_info['image']
            if 'phone' in detail_info and tel == '':
                tel = detail_info['phone']
            if 'guoke_geo' in detail_info:
                guoke_geo = detail_info['guoke_geo']['geo']
        pois_list.append([name, addr, address_norm, std_tag, image, tel, geo, guoke_geo])
    return pois_list

# 爬取第一页， 计算数据量并记录dict
def crawl_geohash_dict(resource_dir, poitype, geolist):
    geohash_dict = dict()
    first_crawl_reslist = list()
    query_total = 0
    for geo in geolist:
        url = get_url(keyword=poitype, qy=geo[0], pagenum='0')
        json_data = req_poi_info(url)
        if 'content' not in json_data:
            continue
        content  = json_data['content']
        result = json_data['result']
        if 'total' not in result:
            continue
        total = result['total']
        if total <= 10: 
            first_crawl_reslist.extend(parse_content_data(content))
        else:
            geohash_dict[geo[1]] = [geo[0], poitype, total]
        query_total += total
    # result_set = set(['\t'.join(res) for res in first_crawl_reslist])
    # with open('crawl_output/poi_name/first_crawl_poiname.txt', 'a', encoding='utf-8') as f:
    #     for poi in result_set:
    #         f.writelines(poi+'\n')
    print('查询数据量：'+ str(query_total))
    return geohash_dict, first_crawl_reslist


def crwal_worker(geo):
    result_list = []
    geo_coor = geo[0]
    poitype  = geo[1]
    total = geo[2]
    pagetotal = math.ceil(total/10)
    for i in range(1, pagetotal):
        try:
            url = get_url(keyword=poitype, qy=geo_coor, pagenum=str(i))
            json_data = req_poi_info(url)
            if 'content' not in json_data:
                break
            content  = json_data['content']
            if len(content) == 0 or not content:
                break
            result_list.extend(parse_content_data(content))
        except Exception as e:
            print('爬取失败：'+url)
            continue
    return result_list


def crawl_threalpool(geohash_dict, result_list):
    query_total = 0
    with ThreadPoolExecutor(max_workers=10) as t:
        obj_list = []
        begin = time.time()
        for geo in geohash_dict:
            query_total += geohash_dict[geo][2]
            obj = t.submit(crwal_worker, geohash_dict[geo])
            obj_list.append(obj)

        for future in as_completed(obj_list):
            data = future.result()
            result_list.extend(data)
        #times = time.time() - begin
        #print(times)
    result_set = set(['\t'.join(res) for res in result_list])
    with open('crawl_output/poi_name/crawl_poiname_all.txt', 'a', encoding='utf-8') as f:
        for poi in result_set:
            f.writelines(poi+'\n')
    print('实际爬取数据量：'+ str(len(result_set)))


def crawl_baidu_process():
    type_dict = set(get_firstfield_fromfile(os.path.join('resources/baidu', 'poi_type_small.txt')))
    geolist = generate_geohashes('crawl_output/bound/33_浙江省/330108_滨江区.json')
    for poitype in type_dict:
        poitype = '公园'
        print('区域： 爬取类型：{}，{}'.format('滨江区', poitype))
        print('第一次爬取，查询POI第一页并计算数据量')
        geohash_dict, first_crawl_reslist = crawl_geohash_dict('resources/baidu', poitype, geolist)
        print('第二次爬取，查询所有POI')
        crawl_threalpool(geohash_dict, first_crawl_reslist)

# crawl_process()


# geolist = generate_geohashes('crawl_output/bound/33_浙江省/330108_滨江区.json')
# for geo in geolist:
#     #'120.201559,30.215905;120.212470,30.210250'
#     for i in range(0, 10):
#         url = get_url(keyword='百货商场', qy=geo[0], pagenum=str(i))
#         json_data = req_poi_info(url)
#         if 'content' not in json_data:
#             break
#         content  = json_data['content']
