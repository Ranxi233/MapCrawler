import jsonpath
import json
import requests as rq
import time
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
import TransCooSys
import os
import re
from bs4 import BeautifulSoup
import threading
import urllib
import queue

#### 路网数据 ####
## 爬取百度地图线路，高速、国道、省道


## 爬取各省的省道线路列表
# def crawl_province_road():
#     for id in range(1, 32): 
#         url = 'http://esk365.com/tools/gsgl/gsfl.asp?id='+ str(id)
#         r=rq.get(url, timeout=(3,9))
#         r.encoding='utf-8'
#         soup = BeautifulSoup(r.text, 'html.parser')
#         print(type(soup))
#         lm_t = soup.find_all(class_='lm_t lb')
#         provline_name = lm_t[0].contents[0].contents[0]
#         line_list = []
#         #print()  #检索标签名
#         table = soup.find_all('table')[0]
#         tcontents = table.contents
#         for i in range(1, len(tcontents)):
#             content = tcontents[i]
#             one_list = []
#             for cc in content.contents:
#                 one_list.append(cc.text)
#             line_list.append('\t'.join(one_list))
#         with open('resources/省道/' + provline_name + '.txt', 'w', encoding='utf-8') as f:
#             for line in line_list:
#                 f.writelines(line+'\n')
    # print(soup.find_all('a',id='link1')) #检索属性值
    # print(soup.find_all('a',class_='sister')) 
    # print(soup.find_all(text=['Elsie','Lacie']))
    # #2、find( )
    # print(soup.find('a'))
    # print(soup.find(id='link2'))
    # #3 、向上检索
    # print(soup.p.find_parent().name)
    # for i in soup.title.find_parents():
    #     print(i.name)
    # #4、平行检索
    # print(soup.head.find_next_sibling().name)
    # for i in soup.head.find_next_siblings():
    #     print(i.name)
    # print(soup.title.find_previous_sibling())
    # for i in soup.title.find_previous_siblings():
    #     print(i.name)


class CrawHighway():
    def __init__(self, citycode_file, admin_divisions_file, output_path):
        # 行政区划信息
        with open(admin_divisions_file, 'r', encoding='utf-8') as f:
            self.admin_divisions = json.load(f)
        # 各市代号, 用于http请求地址, 例："绍兴市": "293",
        self.city_cdict = {}
        self.get_city_cdict(citycode_file)
        # 暂存爬取结果列表，避免重复爬取写入
        self.tmp_lineset = set()
        self.output_dir = os.path.join(output_path, 'highway')
        self.log_dir = os.path.join(output_path, 'log')

    # 加载城市代号(市级)
    def get_city_cdict(self, filename):
        zhixia_citys = {'北京市','上海市','天津市','重庆市'}
        print('--加载city代号--')
        with open(filename, 'r', encoding='utf-8') as f:
            line = f.readline()
        line_list = line.split(',')
        for line in line_list:
            cl = line.split('|')
            cityname = cl[0]
            citycode = cl[1]
            if cityname+'市' in zhixia_citys:
                self.city_cdict[cityname+'市'] = citycode
            else:
                for ad in self.admin_divisions:
                    isProvince = True if cityname in ad else False
                if not isProvince and cityname != '中国':
                    if not ('自治' in cityname or '地区' in cityname or '盟' in cityname or '林区' in cityname or '县' in cityname):
                        cityname = cl[0] + '市'
                    self.city_cdict[cityname] = citycode


    ## 生成JSON
    def gengerate_geojson(self, points, area_name, name, alias):
        coordinates = []
        points_new = []
        for point_block in points:
            pk_new = [] 
            if len(point_block)==0:
                continue
            for point in point_block:
                lat = float(point.split(',')[0])
                lon = float(point.split(',')[1])
                pk_new.append((lat, lon))
            points_new.append(pk_new)
        coordinates.append(points_new)
        bound_json = {
            "type": "Feature",
            "properties": {
                "name": name,
                "city": area_name,
                "alias": alias
            }, 
            "geometry": {
                "type": "MultiPolygon", 
                "coordinates": coordinates
            }
        }
        return bound_json

    ## 爬取、解析核心逻辑
    def parse_highway_citys(self, province, city, area, road_name, road_id, waytype):
        ## 拼接http请求
        keywords = road_id or road_name 
        keywords = area + keywords
        city_c = self.city_cdict[city]
        url_name='https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd='+keywords+'-道路&c='+city_c+'&src=0&wd2='+city+'&pn=0&sug=1&l=11'
        json_data0=get_json_http(url_name)
        ## 重试请求
        if 'content' not in json_data0:
            return 
            # url_name='https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd='+road_name+'-道路&c='+city_c+'&src=0&wd2='+city+'&pn=0&sug=1&l=11'
            # json_data0=get_json_http(url_name)
            # if 'content' not in json_data0:
            #     return
        ## 解析json
        content0 = json_data0['content']
        for i in range(0, len(content0)):
            ## 第一个返回结果直接处理; 其他结果如果匹配，提取信息再次请求
            ct = content0[i]
            if i == 0:
                content = ct
            else:
                if ct['acc_flag'] == 1:
                    area_name_ct = ct['area_name']
                    name_ct = ct['name']
                    url_name_ct='https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd='+area_name_ct+name_ct+'-道路&c='+city_c+'&src=0&wd2='+area_name_ct+'&pn=0&sug=1&l=11'
                    json_data_ct=get_json_http(url_name_ct)
                    content = json_data_ct['content'][0]
                else:
                    break
            try:
                name = content['name']
                cla = content['cla'] if 'cla' in content else []
                alias = content['alias'][0] if 'alias' in content else ''
                tname = name.replace('辅路', '')
                ## 判断爬取结果
                if not (tname == road_id or tname == road_name or (len(cla) > 1 and waytype in cla[1][1]) or waytype in alias):
                    print('--'+name+'-- 非'+waytype)
                    continue
                profile_geo = content['profile_geo']
                area_name = content['area_name']
                out_filename = str(name)+'_'+str(area_name)
                if out_filename in self.tmp_lineset:
                    continue
                self.tmp_lineset.add(out_filename)
                ## 数据处理, 生成结果文件
                if len(profile_geo) == 0:
                    print(city+'_'+name+"： 未找到经纬度")
                    write_log(city+'\t'+name, os.path.join(self.log_dir, 'crawl_highway.log'))
                    continue
                points_all = TransCooSys.points_transform_baidu2wgs(profile_geo)
                area_bound_json = self.gengerate_geojson(points_all, area_name, name, alias)
                ## 写入文件
                out_dir = os.path.join(self.output_dir, province, waytype) 
                out_filename = out_filename.replace(',', '').replace("'", '').replace(')', '').replace('(', '')
                write_jsonfile(out_dir, out_filename, area_bound_json)
                print(out_filename, out_dir)
            except Exception as e:
                print(city+'_'+name+"： 解析失败")
                write_log(city+'\t'+name, os.path.join(self.log_dir, 'crawl_highway.log'))
                continue
        print("===="+keywords+": 爬取完毕====")

        
## requests 
def get_json_http(url):
    try:
        r=rq.get(url, timeout=(3,9))
        json_data=json.loads(r.text)
        return json_data
    except Exception as e:
        return 

def write_log(context, logfile):
    with open(logfile, 'a', encoding='utf-8') as lf:
        lf.writelines(context+'\n')

## 写入json文件
def write_jsonfile(out_dir, out_name, json_data):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    file_name = os.path.join(out_dir, out_name+'.json')
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False)


class myThread(threading.Thread):
    def __init__(self, name, q, cway):
        threading.Thread.__init__(self)
        self.name = name
        self.q = q
        self.cway = cway

    def run(self):
        print("Starting " + self.name)
        while True:
            try:
                self.crawl_execute(self.name, self.q, self.cway)
            except:
                break
        print("Exiting " + self.name)

    def crawl_execute(self, threadNmae, q, cway):
        line_data = q.get(timeout=2)
        try:
            llist = line_data.replace('\n','').split('\t')
            province = llist[0]
            city = llist[1]
            area = llist[2]
            wayid = llist[3]
            name = llist[4]
            waytype = llist[5]
            cway.parse_highway_citys(province, city, area, name, wayid, waytype)
        except Exception as e:
            print(threadNmae, "Error: ", e) 


def entrance(waysList, threadNum, cway):
    start = time.time()
    workQueue = queue.Queue(len(waysList))
    for way in waysList:
        workQueue.put(way)
    threads = []
    for i in range(0,threadNum):
        thread = myThread("Thread-"+str(i), q=workQueue, cway=cway)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    end = time.time()
    print("Queue多线程爬虫耗时：{} s".format(end - start))


def crawl_highway(filename, output_path):
    cway =  CrawHighway(
        'resources/citycode.txt', 
        'resources/admin_divisions.json',
        output_path
    )
    with open(filename, 'r', encoding='utf-8') as ff:
        lines = ff.readlines()
    # 启动n个线程
    entrance(lines, 5, cway)

#crawl_highway('crawl_input/all_highway.txt', 'crawl_output')


# def get_highway_roads():
#     road_list = []
#     cway = CrawHighway(
#         'resources/citycode.txt', 
#         'resources/admin_divisions.json',
#         ''
#     )
#     for root, ds, fs in os.walk('baidu/waynet'):
#         for pname in ds:
#             for ad in cway.admin_divisions:
#                 if pname in ad:
#                     province_name = ad
#             for root, dsc, fsc in os.walk(os.path.join('baidu/waynet', pname, '高速')):
#                 for cname in fsc:
#                     if '大队' in cname or '支队' in cname or '管理' in cname or '公司' in cname or '出口' in cname or '入口' in cname:
#                         continue
#                     if 'json' in cname:
#                         with open(os.path.join('baidu/waynet', pname, '高速', cname), 'r', encoding='utf-8') as gf:
#                             gjson = json.load(gf)
#                             road_list.append((gjson['properties'], '高速', cname, province_name))
#             for root, dsc, fsc in os.walk(os.path.join('baidu/waynet', pname, '国道')):
#                 for cname in fsc:
#                     if 'json' in cname:
#                         with open(os.path.join('baidu/waynet', pname, '国道', cname), 'r', encoding='utf-8') as gf:
#                             gjson = json.load(gf)
#                             road_list.append((gjson['properties'], '国道', cname, province_name))
#             for root, dsc, fsc in os.walk(os.path.join('baidu/waynet', pname, '省道')):
#                 for cname in fsc:
#                     if 'json' in cname:
#                         with open(os.path.join('baidu/waynet', pname, '省道', cname), 'r', encoding='utf-8') as gf:
#                             gjson = json.load(gf)
#                             road_list.append((gjson['properties'], '省道', cname, province_name))
#     reslist = []
#     for way in road_list:
#         city = way[0]['city']
#         city_list = city.split('市') if isinstance(city, str) else city[0].split('市')
#         area_name = ''
#         city_name = city_list[0] + '市'
#         if '地区' in city_list[0]:
#             city_name = city_list[0].split('地区')[0] + '地区'
#         elif '自治州' in city_list[0]:
#             city_name = city_list[0].split('自治州')[0] + '自治州'
#         elif '盟' in city_list[0]:
#             city_name = city_list[0].split('盟')[0] + '盟'
#         elif '自治县' in city_list[0]:
#             city_name = city_list[0].split('自治县')[0] + '自治县'
#         elif '林区' in city_list[0]:
#             city_name = city_list[0].split('林区')[0] + '林区'
#         elif '县' in city_list[0]:
#             city_name = city_list[0].split('县')[0] + '县'
#         if len(city_list) == 2:
#             area_name = city_list[1]
#         elif  len(city_list) == 3:
#             area_name = city_list[1]+'市'
#         wayname = way[2].split("_")[0]
#         wayid = ''
#         waytype = way[1]
#         province_name = way[3]
#         reslist.append((province_name, city_name, area_name, wayid, wayname, waytype))
#     with open('crawl_input/test2_highway.txt', 'a', encoding='utf-8') as gf:
#         for res in reslist:
#             gf.writelines('\t'.join(res)+'\n')
