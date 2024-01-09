from ast import walk
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
import multiprocessing
from time import sleep


headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Authorization": "Basic cml2ZXJtYXBfZ3Vlc3Q6cml2ZXJtYXBfc2hhcmU=",
    "Connection": "keep-alive",
    "Host": "service.rivermap.cn:8100",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
}
# 根目录
ROOT_DIR = 'D:/py_workspace/'
# 资源目录
RESOURECE_DIR = ROOT_DIR + 'resources/'
# POI爬取保存目录
RIVERMAP_POI_DIR = ROOT_DIR+'crawl_output/rivermap_poi/'
# POI结果保存目录
RESULT_DIR = RIVERMAP_POI_DIR+'poi_result/'
# 日志目录
LOGDIR = ROOT_DIR+'crawl_output/log/'

success_log = LOGDIR+'crawl_poi_city_success.log'
poi_nums_log = LOGDIR+'crawl_poi_riveramp.log'

municipality_dict = {'北京市', '天津市', '上海市', '重庆市', '香港特别行政区', '澳门特别行政区', '台湾省'}
admin_divisions = {}
with open(RESOURECE_DIR+'admin_divisions.json', 'r', encoding='utf-8') as bf:
    admin_divisions = json.load(bf)

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


def req_poi_info(url=None,cs_cnt=0):
    try:
        r = requests.get(url, headers=headers, timeout=(3, 9))
        return r.content
    except:
        if cs_cnt>4:
            return None
        sleep(2)
        return req_poi_info(url=url,cs_cnt=(cs_cnt+1))


def crawl_threalpool(geolist, type_dict, output_dir):
    # 执行一次爬取任务
    def crawl_rivermap_once(poitype, geo, output_dir):
        url = "http://service.rivermap.cn:8100/geoserver/rivermap_service/ows?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=rivermap_service:v2_layer_poi_amap_pt&STARTINDEX=0&COUNT=100000&FILTER=<Filter xmlns='http://www.opengis.net/fes/2.0' xmlns:rivermap_service='http://localhost:8100/rivermap_service' xmlns:gml='http://www.opengis.net/gml/3.2'><And><PropertyIsLike wildCard='*' singleChar='_' escapeChar='!'><ValueReference>type</ValueReference><Literal>*{}*</Literal></PropertyIsLike><BBOX><ValueReference>rivermap_service:geom</ValueReference><gml:Envelope><gml:lowerCorner>{}</gml:lowerCorner><gml:upperCorner>{}</gml:upperCorner></gml:Envelope></BBOX></And></Filter>".format(poitype, geo[0], geo[1])
        rcontent = req_poi_info(url)
        if rcontent is None or not rcontent:
            return 
        filename = os.path.join(output_dir, poitype+geo[2]+'.xml')
        with open(filename, "wb") as xf:
            xf.write(rcontent)
        return filename
    crawl_list = []
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for geo in geolist:
        for poitype in type_dict:
            crawl_list.append([poitype, geo])
    with ThreadPoolExecutor(max_workers=10) as t:
        obj_list = []
        for cdata in crawl_list:
            obj_list.append(t.submit(lambda cxp:crawl_rivermap_once(*cxp),(cdata[0],cdata[1],output_dir)))
        for future in as_completed(obj_list):
            resname = future.result()
            #print(resname)

## 爬虫主程序
def crawl_rivermap_process(province, cityname, city_code, areas, isDivide = False):
    ## 获取区划经纬度数组
    def get_zsk_jc_dlyqqyfgmsj(filename, area_code):
        with open(filename, 'r', encoding='utf-8') as zf:
            lines = zf.readlines()
            for line in lines:
                llist = line.split('\t')
                if area_code == llist[10]:
                    geojson = json.loads(llist[14])
        with open('geojson_tmp.json', 'w', encoding='utf-8') as gf:
            json.dump(geojson, gf, ensure_ascii=False)
        with open('geojson_tmp.json', 'r', encoding='utf-8') as gf:
            data = geopandas.read_file(gf)
            points = gis_map_cl(data, False)
        return points

    type_dict = set(get_firstfield_fromfile(RESOURECE_DIR+'tmp_type.txt'))
    output_dir = os.path.join(RIVERMAP_POI_DIR, city_code[0:2]+"_"+province, city_code+'_'+cityname)
    for area in areas:
        alist = area.split('_')
        aname, acode = alist[0], alist[1]
        points = get_zsk_jc_dlyqqyfgmsj(RESOURECE_DIR+'xzqh.txt', acode)
        geolist = generate_geohashes(points, 5 , isDivide)
        print('-------------'+aname+' 开始爬取---------------')
        crawl_threalpool(geolist, type_dict, output_dir)
        print('-------------'+aname+' 爬取完毕---------------')
    with open(success_log, 'a', encoding='utf-8') as sf:
        sf.writelines(cityname+'\n')
    return output_dir

## dom解析xml文件
def domParse(filename):
    poi_list = []
    # 读取文件
    try:
        dom = parse(filename)
        # 获取文档元素对象
        data = dom.documentElement
        fcs = data.getElementsByTagName('wfs:member')
        for fc in fcs:
            # 获取标签属性值
            gml_id = fc.getElementsByTagName('rivermap_service:v2_layer_poi_amap_pt')[0].getAttribute('gml:id')
            rname = fc.getElementsByTagName('rivermap_service:Name')[0].childNodes[0].nodeValue
            raddress = ''
            if fc.getElementsByTagName('rivermap_service:address'):
                raddress = fc.getElementsByTagName('rivermap_service:address')[0].childNodes[0].nodeValue
            rtype = fc.getElementsByTagName('rivermap_service:type')[0].childNodes[0].nodeValue
            areaid = fc.getElementsByTagName('rivermap_service:areaid')[0].childNodes[0].nodeValue
            updatetime = fc.getElementsByTagName('rivermap_service:updatetime')[0].childNodes[0].nodeValue
            isdelete = fc.getElementsByTagName('rivermap_service:isdelete')[0].childNodes[0].nodeValue
            telephone = ''
            if fc.getElementsByTagName('rivermap_service:telephone'):
                telephone = fc.getElementsByTagName('rivermap_service:telephone')[0].childNodes[0].nodeValue
            gml_pos = fc.getElementsByTagName('rivermap_service:geom')[0].getElementsByTagName('gml:pos')[0].childNodes[0].nodeValue
            poi_list.append([rname, raddress, areaid, rtype, updatetime, isdelete, telephone, gml_pos])
    except Exception as e:
        return poi_list
    #print(filename)
    return poi_list

## 读取所有爬取的xml文件，解析写入txt
def get_all_pois_rivermap(input_dir, province, cityname, citycode):
    res_list = []
    file_list = []
    print('加载: '+input_dir)
    for root, ds, fs in os.walk(input_dir):
        for fname in fs:
            file_list.append(os.path.join(input_dir, fname))
    with ThreadPoolExecutor(max_workers=10) as dt:
        obj_list = []
        print("开始解析xml")
        for fdata in file_list:
            obj_list.append(dt.submit(domParse, fdata))
        for future in as_completed(obj_list):
            pol_list = future.result()
            res_list.extend(pol_list)
    if cityname in municipality_dict:
        result_set = set(['\t'.join(res) for res in res_list if res[2][0:4] == citycode[0:4]])
    else:
        result_set = set(['\t'.join(res) for res in res_list if res[2][0:2] == citycode[0:2]])
    outputdir = RESULT_DIR + citycode[0:2] + '_' + province
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    filename = os.path.join(outputdir, citycode+'_'+cityname+'.txt')
    with open(filename, 'w', encoding='utf-8') as fpf:
        for res in result_set:
            fpf.writelines(res+'\n')
    print('输出：'+filename)
    return filename


def get_line_num(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        delete_num1 = 0
        delete_num0 = 0
        area_dict = {}
        for line in lines:
            llist = line.split('\t')
            acode = llist[2]
            area_dict[acode] =  area_dict[acode] + 1 if acode in area_dict else 1
            isdelete = llist[5]
            if isdelete == '1':
                delete_num1 += 1
            else:
                delete_num0 += 1
        print('1:'+ str(delete_num1))
        print('0:'+ str(delete_num0))
        for area in area_dict:
            print(area+': '+ str(area_dict[area]))
        with open(poi_nums_log,'a', encoding='utf-8') as lf:
            lf.writelines(str(filename)+'\t'+str(delete_num1)+'\t'+str(delete_num0)+'\n')
 
def crawl_execute():
    with open(success_log, 'r', encoding='utf-8') as sf:
        lines = sf.readlines()
        success_city_set = set([line.replace('\n', '').split('\t')[0] for line in lines])
    #1 执行爬虫，下载xml文件
    for province in admin_divisions:
        if province not in {'黑龙江省','山东省','湖北省','重庆市','新疆维吾尔自治区'}:
            continue
        if province in  municipality_dict:
            if province in success_city_set:
                continue
            areas = admin_divisions[province]['citys']
            #city_code =  admin_divisions[province]['acode']
            for area in areas:
                if province=='台湾省':
                    area_name = list(area.keys())[0]
                    area_code = area[area_name]['acode']
                    crawl_output_dir = crawl_rivermap_process(province, area_name, area_code, [area_name+'_'+area_code])
                else:
                    area_name = area.split('_')[0]
                    area_code = area.split('_')[1]
                    crawl_output_dir = crawl_rivermap_process(province, area_name, area_code, [area])
                outfile = get_all_pois_rivermap(crawl_output_dir, province, area_name, area_code)
                get_line_num(outfile)
        else:
            citys = admin_divisions[province]['citys']
            for cc in citys:
                ccname = list(cc.keys())[0]
                if ccname in success_city_set:
                    continue
                citydata = cc[ccname]
                areas = citydata['areas']  if citydata['areas'] else [ccname+'_'+citydata['acode']]
                city_code = citydata['acode']
                crawl_output_dir = crawl_rivermap_process(province, ccname, city_code, areas)
                #crawl_output_dir = 'crawl_output/rivermap_poi/33_浙江省/330300_温州市'
                outfile = get_all_pois_rivermap(crawl_output_dir, province, ccname, city_code)
                get_line_num(outfile)

 
# 程序运行时间在白天8:30 到 15:30  晚上20:30 到 凌晨 2:30
DAY_START = time(14, 00)
DAY_END = time(16, 30)
 
NIGHT_START = time(20, 00)
NIGHT_END = time(8, 00)
 
def run_child():
    #while 1:
        print("正在运行子进程")
        crawl_execute()

def scheduler_run():
    print("启动父进程")
    child_process = None  # 是否存在子进程
    while True:
        current_time = datetime.now().time()
        print(current_time)
        running = False

        # 判断时候在可运行时间内
        if DAY_START <= current_time <= DAY_END or (current_time >= NIGHT_START) or (current_time <= NIGHT_END):
            running = True

        # 在时间段内则开启子进程
        if running and child_process is None:
            print("启动子进程")
            child_process = multiprocessing.Process(target=run_child)
            child_process.start()
            print("子进程启动成功")
 
        # 非记录时间则退出子进程
        if not running and child_process is not None:
            print("关闭子进程")
            child_process.terminate()
            child_process.join()
            child_process = None
            print("子进程关闭成功")

        sleep(60)


def get_poi_result():
    def read_file_poi(filename):
        result_set = set()
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                llist = line.split('\t')
                isdelete = llist[5]
                if isdelete == '0':
                    result_set.add(llist[0])
        return result_set

    reuslt_dir = 'crawl_output/rivermap_poi/poi_result'
    for root, ds, fs in os.walk(reuslt_dir):
        for dir in ds:
            all_poi_set = set()
            for root, ds2, fs2 in os.walk(os.path.join(reuslt_dir, dir)):
                for fname in fs2:
                    all_poi_set = all_poi_set | read_file_poi(os.path.join(reuslt_dir, dir, fname))
            with open(os.path.join(reuslt_dir, dir+'.txt'), 'a', encoding='utf-8') as f:
                for poi in all_poi_set:
                    f.writelines(poi+'\n')
                print(dir+ str(len(all_poi_set)))


if __name__ == '__main__':
    #scheduler_run()
    crawl_execute()
    #get_all_pois_rivermap('crawl_output/rivermap_poi/71_台湾省', '台湾省', '台湾省', '710000')
    #get_poi_result()

