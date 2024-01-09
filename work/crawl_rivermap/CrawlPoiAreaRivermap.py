

from fileinput import filename
from unicodedata import name
from xml.dom.minidom import parse
import os
from urllib.parse import quote
from pip import main
import requests
from sqlalchemy import all_
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
import TransCooSys
import geohash
from shapely import geometry
import geopandas
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time
from time import sleep
import zhconv
import hashlib

def get_firstfield_fromfile(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        reslist = [line.replace('\n','').split('\t')[0] for line in lines if '1' not in line.split('\t')[-1]]
        return reslist

## 解析geojson，获取经纬度数组
def gis_map_cl(gis_data,fx=False):
    geometry0 = str(gis_data['geometry'][0])
    jwds=geometry0.replace('MULTIPOLYGON (((','').replace(')))','')
    if '((' in jwds:
        jwdslist = jwds.split(')), ((')
    elif '(' in jwds:
        jwdslist = jwds.split('), (')
    else:
        jwdslist = [jwds]
    all_points = []
    for jj in jwdslist:
        tmp_points=jj.split(', ')
        points=[]
        for tmp_point in tmp_points:
            tmp=tmp_point.replace(')','').replace('(','').split(' ')
            if fx:
                points.append([float(tmp[1]),float(tmp[0])])
            else:
                points.append([float(tmp[0]),float(tmp[1])])
        all_points.append(points)
    return all_points


## 经纬度数组转geohash, 划分矩形区域
def generate_geohashes(all_points, geohash_level = 5, isDivide = False):
    def divide_geohash_to4(ws_coor, en_coor, g6):
        mid_lat = str((ws_coor[1] + en_coor[1])/2)
        min_lon = str((ws_coor[0] + en_coor[0])/2)
        w_lon = str(ws_coor[0])
        e_lon = str(en_coor[0])
        s_lat = str(ws_coor[1])
        n_lat = str(en_coor[1])
        sub_geo_list = []
        sub_geo_list.append([mid_lat + ' ' + w_lon,   n_lat   + ' ' + min_lon, g6 + '_1'])
        sub_geo_list.append([mid_lat + ' ' + min_lon, n_lat   + '  '+ e_lon,   g6 + '_2'])
        sub_geo_list.append([s_lat   + ' ' + w_lon  , mid_lat + ' ' + min_lon, g6 + '_3'])
        sub_geo_list.append([s_lat   + ' ' + min_lon, mid_lat + ' ' + e_lon, g6 + '_4'])
        return sub_geo_list
    geolist = []
    for points in all_points:
        polygon = geometry.Polygon(points)
        geohash6_outer = polygon_to_geohashes(polygon, geohash_level, False)
        for g6 in geohash6_outer:
            box = geohash.bbox(g6)
            yxj09 = TransCooSys.wgs84_to_bd09(box['w'],box['s'])
            zsj09 = TransCooSys.wgs84_to_bd09(box['e'],box['n'])
            # yxj = TransCooSys.bd09tomercator(yxj09[0], yxj09[1])
            # zsj = TransCooSys.bd09tomercator(zsj09[0], zsj09[1])
            if isDivide:
                geolist.extend(divide_geohash_to4(yxj09, zsj09, g6))
            else:
                geolist.append([str(yxj09[1])+' '+str(yxj09[0]), str(zsj09[1])+' '+str(zsj09[0]), g6])
    return geolist



class CrawlRivermapBoundpg():
    def __init__(self, outrdir, result_dir):
        self.outdir = outrdir
        self.result_dir = result_dir
        self.municipality_dict = {'北京市', '天津市', '上海市', '重庆市', '香港特别行政区', '澳门特别行政区'}
        with open('resources/admin_divisions.json', 'r', encoding='utf-8') as bf:
            self.admin_divisions = json.load(bf)
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Authorization": "Basic cml2ZXJtYXBfZ3Vlc3Q6cml2ZXJtYXBfc2hhcmU=",
            "Connection": "keep-alive",
            "Host": "service.rivermap.cn:8100",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
        }

    def crawl_rivermap_city_pg(self, ccname, city_code, province_name):
        def req_poi_info(url,headers,cs_cnt=0):
            try:
                r = requests.get(url, headers=headers, timeout=(3, 9))
                return r.content
            except:
                if cs_cnt>4:
                    return None
                sleep(2)
                return req_poi_info(url, headers, cs_cnt=(cs_cnt+1))
        url = "http://service.rivermap.cn:8100/geoserver/rivermap_service/ows?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=rivermap_service:layer_city_boundary_pg&STARTINDEX=0&COUNT=2000&FILTER=<Filter xmlns='http://www.opengis.net/fes/2.0' xmlns:rivermap_service='http://localhost:8100/rivermap_service' xmlns:gml='http://www.opengis.net/gml/3.2'><And><PropertyIsLike wildCard='*' singleChar='_' escapeChar='!'><ValueReference>code</ValueReference><Literal>*{}*</Literal></PropertyIsLike></And></Filter>".format(city_code)
        rcontent = req_poi_info(url, self.headers)
        if rcontent is None or not rcontent:
            return
        dir = os.path.join(self.outdir, province_name)
        if not os.path.exists(dir):
            os.makedirs(dir)
        filename = os.path.join(dir, city_code+'_'+ccname+'.xml')
        with open(filename, "wb") as xf: 
            xf.write(rcontent)
        return filename

    ## 处理xml经纬度数据，转数组
    def process_poilist_geo(self, pois_list):
        coordinates_list = []
        for pois_str in pois_list:
            poi_list = pois_str.split(' ')
            coordinates = []
            for i in range(0, len(poi_list)):
                if i%2 == 0:
                    lat = poi_list[i]
                if i%2 == 1:
                    lon = poi_list[i]
                    coordinates.append([float(lon), float(lat)])
            coordinates_list.append(coordinates)
        return coordinates_list

    ## 生成geojson, 用于验证结果
    def generate_geojson(self, resultdata, citynamecode):
        json_list = []
        for pois in resultdata:
            coordinates = self.process_poilist_geo(pois[3])
            val_dict = {
                "type": "Feature",
                "properties": {
                    "adcode" : pois[0],
                    "name" : pois[1], 
                    "level" : pois[2],
                    "parent": {"adcode": "710000"}},
                "geometry": {
                    "type" : "MultiPolygon",
                    "coordinates": [coordinates]}
            }
            json_list.append(val_dict)
        json_dict = {
            "type": "FeatureCollection",
            "features": json_list
        }
        with open(os.path.join(self.result_dir, citynamecode+'.geojson'), 'w', encoding='utf-8') as gf:
            json.dump(json_dict, gf, ensure_ascii=False)
    
    def MD5(self, str):
        md5 = hashlib.md5()
        md5.update(str.encode('utf-8'))
        return md5.hexdigest()

    ## 解析爬取的xml数据 
    def parse_rivermap_xmldata(self, filelist, province_name):
        ## dom解析xml文件
        def domParse(filename):
            dom = parse(filename)
            data = dom.documentElement
            fcs = data.getElementsByTagName('wfs:member')
            poiarea_list = []
            #subfile = os.path.splitext(os.path.split(filename)[-1])[0].split('_')
            for fc in fcs:
                if not fc.getElementsByTagName('rivermap_service:Name'):
                    continue
                Name = fc.getElementsByTagName('rivermap_service:Name')[0].childNodes[0].nodeValue
                code = fc.getElementsByTagName('rivermap_service:code')[0].childNodes[0].nodeValue
                layer = fc.getElementsByTagName('rivermap_service:layer')[0].childNodes[0].nodeValue
                level = 'city' if layer == '市' else 'district'
                posList = fc.getElementsByTagName('rivermap_service:geom')[0].getElementsByTagName('gml:posList')
                polylist = [pos.childNodes[0].nodeValue for pos in posList]
                poiarea_list.append([code, Name, level, polylist])
            return poiarea_list
        ## 生成标准表结果
        def generate_standard_out(res_list):
            standrad_list = []
            for poidata in res_list:
                cityname = zhconv.convert(poidata[1], 'zh-cn' )
                xlqymc = cityname
                xzqhdm = poidata[0][0:6]
                place_name = '行政区划'
                coordinates = self.process_poilist_geo(poidata[3])
                val_dict = {
                    "type": "Feature",
                    "properties": {
                        "adcode" : xzqhdm,
                        "name" : xlqymc, 
                        "level" : 'city',
                        "parent": {"adcode": "710000"}},
                    "geometry": {
                        "type" : "MultiPolygon",
                        "coordinates": [coordinates]}
                }
                descriptions = json.dumps(val_dict, ensure_ascii=False)
                md_id = self.MD5(xzqhdm+xlqymc+place_name)
                standrad_list.append('\t'.join([md_id, '0', '0', '0', '330000', '0', '', 
                    '台湾省', cityname, '', xzqhdm, xlqymc, '', place_name, descriptions, '']))
            return standrad_list
        print("开始解析xml")
        res_list = []
        with ThreadPoolExecutor(max_workers=10) as dt:
            obj_list = []
            for fdata in filelist:
                obj_list.append(dt.submit(domParse, fdata))
            for future in as_completed(obj_list):
                pol_list = future.result()
                res_list.extend(pol_list)
        # 生成geojson
        #self.generate_geojson(res_list, province_name)
        if not os.path.exists(self.result_dir):
            os.makedirs(self.result_dir)
        filename = os.path.join(self.result_dir, province_name+'.txt')
        standrad_list = generate_standard_out(res_list)
        reset = set(standrad_list)
        with open(filename, 'w', encoding='utf-8') as fpf:
            for res in reset:
                fpf.writelines(res+'\n')
        print('输出：'+filename)


    def crawl_execute(self, province_name):
        #1 执行爬虫，下载xml文件
        resfile_list = []
        for province in self.admin_divisions:
            if province in self.municipality_dict:
                continue
            if province == province_name:
                citys = self.admin_divisions[province]['citys']
                for cc in citys:
                    ccname = list(cc.keys())[0]
                    citydata = cc[ccname]
                    areas = citydata['areas']  if citydata['areas'] else [ccname+'_'+citydata['acode']]
                    city_code = citydata['acode']
                    cityfilename = self.crawl_rivermap_city_pg(ccname, city_code, province_name)
                    resfile_list.append(cityfilename)
                # codes = ['7108', '7103','7118','7101','7116','7109','7111','7119',
                #         '7120','7112','7107','7106','7105','7117','7115',
                #         '7104','7102','7110','7113', '7114']
                # for code in codes:
                #     cityfilename = self.crawl_rivermap_city_pg(province_name, code, province_name)
                #     resfile_list.append(cityfilename)
        self.parse_rivermap_xmldata(resfile_list, province_name)



    def crawl_county(self, province_name):
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
        data = set()
        sep = '\t'
        with open('resources/admin_divisions.json', 'r', encoding='utf-8') as bf:
            admin_divisions = json.load(bf)
        for province in admin_divisions:
            if province == province_name:
                citys = self.admin_divisions[province]['citys']
                for cc in citys:
                    ccname = list(cc.keys())[0]
                    citydata = cc[ccname]
                    areas = citydata['areas']  if citydata['areas'] else [ccname+'_'+citydata['acode']]
                    for area in areas:
                        try:
                            city = ccname
                            district = area.split('_')[0]
                            districtcode = area.split('_')[1]
                            townname = district
                            towncode = districtcode
                            if towncode not in data :
                                url = "http://39.104.59.235:8090/geoserver/rivermap_district/ows?SERVICE=WFS&OUTPUTFORMAT=json&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=rivermap_district:layer_street&STARTINDEX=0&COUNT=2000&FILTER=%3CFilter%20xmlns%3D%22http:%2F%2Fwww.opengis.net%2Ffes%2F2.0%22%20xmlns:rivermap_district%3D%22http:%2F%2Flocalhost:8100%2Frivermap_district%22%20xmlns:gml%3D%22http:%2F%2Fwww.opengis.net%2Fgml%2F3.2%22%3E%3CPropertyIsEqualTo%3E%3CValueReference%3Eadcode%3C%2FValueReference%3E%3CLiteral%3E"+towncode[0:9]+"%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FFilter%3E"
                                rn = requests.get(url, timeout=(3, 9),headers=headers)
                                rntext = rn.text
                                if len(rntext) < 100:
                                    url = "http://39.104.59.235:8090/geoserver/rivermap_district/ows?SERVICE=WFS&OUTPUTFORMAT=json&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=rivermap_district:layer_street&STARTINDEX=0&COUNT=2000&FILTER=%3CFilter%20xmlns%3D%22http:%2F%2Fwww.opengis.net%2Ffes%2F2.0%22%20xmlns:rivermap_district%3D%22http:%2F%2Flocalhost:8100%2Frivermap_district%22%20xmlns:gml%3D%22http:%2F%2Fwww.opengis.net%2Fgml%2F3.2%22%3E%3CPropertyIsEqualTo%3E%3CValueReference%3EName%3C%2FValueReference%3E%3CLiteral%3E"+townname+"%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FFilter%3E"
                                    rn = requests.get(url, timeout=(3, 9),headers=headers)
                                    rntext = rn.text
                                jsondata = eval(rn.text)
                                jsondata["features"][0]["properties"]["code"] = towncode
                                jsondata["features"][0]["properties"]["adcode"] = towncode[0:9]
                                jsondata["features"][0]["properties"]["padcode"] = districtcode[0:6]
                                
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
                                coordinates = str(jsondata["features"][0]["geometry"]["coordinates"])
                                str1 = '{"type": "Feature", "properties": {"adcode": '+'"{}"'.format(XZQHDM)+', "name": "{}"'.format(XLQYMC)
                                str2 = ', "level": 5, "parent": {'+'"adcode": "{}"'.format(jsondata["features"][0]["properties"]["padcode"]) +'}},'
                                str3 = ' "geometry": {"type": "MultiPolygon", "coordinates": '
                                result = str1 + str2 + str3 + coordinates
                                DESCRIPTIONS = result + '}}'
                                INFOR_CONTENT = ''
                                MD_ID = self.MD5(XZQHDM+XLQYMC+PLACE_NAME)
                                with open('street.txt','a', encoding='utf-8') as fw:
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
                            print(area)
                            with open('street_log.txt','a', encoding='utf-8', errors='ignore') as fw:
                                fw.write(str(area)+'\n')



if __name__ == '__main__':
    crbound = CrawlRivermapBoundpg('crawl_output/rivermap_poiarea', 'crawl_output/rivermap_poiarea/pg_result')
    crbound.crawl_execute('浙江省')
    # crbound.parse_rivermap_xmldata(['crawl_output/rivermap_poiarea/台湾省/7117_台湾省.xml'], '台湾省')
    # crbound.crawl_county('台湾省')

