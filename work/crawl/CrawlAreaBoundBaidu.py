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

#### 行政区划数据 ####
## 爬取百度地图全国各行政区划边界

## 生成ad
# def generate_admin_level_json(self, filepath):
#     prov_list = []
#     city_list = []
#     area_list = []
#     city_dict = {}
#     prov_dict = {}
#     ## 读取省市区数据，加载到list中
#     with open(filepath, 'r', encoding='utf-8') as f:
#         lines = f.readlines()
#         for line in lines:
#             ll = line.replace('\n', '').split(',')
#             acode = ll[0]
#             aname = ll[1]
#             alevel = ll[2]
#             if 'province' == alevel:
#                 prov_list.append(acode+'_'+aname)
#             elif 'city' ==alevel:
#                 city_list.append(acode+'_'+aname)
#             else:
#                 area_list.append(acode+'_'+aname)
#     ## 生成市-区关系
#     for city in city_list:
#         city_name = city.split('_')[1]
#         city_code = city.split('_')[0]
#         area_tmp = []
#         for area in area_list:
#             area_name = area.split('_')[1]
#             area_code = area.split('_')[0]
#             if area_code[0:4] == city_code[0:4]:
#                 area_tmp.append(area_name+'_'+area_code)
#         city_dict[city_name] = {'acode':city_code, 'areas':area_tmp}
#     ## 生成省-市关系
#     for prov in prov_list:
#         prov_name = prov.split('_')[1]
#         prov_code = prov.split('_')[0]
#         city_tmp = []
#         for city in city_list:
#             city_name = city.split('_')[1]
#             city_code = city.split('_')[0]
#             if prov_code[0:2] == city_code[0:2]:
#                 city_area = {}
#                 city_area[city_name] = city_dict[city_name]
#                 city_tmp.append(city_area)
#         prov_dict[prov_name] = city_tmp
#         if len(city_tmp) == 0:
#             for area in area_list:
#                 area_name = area.split('_')[1]
#                 area_code = area.split('_')[0]
#                 if prov_code[0:2] == area_code[0:2]:
#                     city_tmp.append(area_name+'_'+area_code)
#         prov_dict[prov_name] = {'acode':prov_code, 'citys':city_tmp}
#     ## 写入文件
#     with open('data/admin_divisions.json', 'w', encoding='utf-8') as f:
#         article = json.dumps(prov_dict, ensure_ascii=False)
#         f.writelines(article+'\n')

class CrawlAreaBoundBaidu():
    def __init__(self, out_dir):
        self.out_dir = os.path.join(out_dir, 'bound')
        self.log_dir = os.path.join(out_dir, 'log')
        self.admin_divisions = {}
        self.admin_codename = self.load_codename_csv('resources/codename.csv')
        self.zhixia_dict = {'110000','120000', '310000', '500000', '810000', '820000'}
        self.zhixia_citys = {'上海市', '北京市', '天津市', '重庆市', '香港特别行政区', '澳门特别行政区'}
        self.city_cdict = {}
        self.get_city_cdict('resources/citycode.txt')
        with open('resources/admin_divisions.json', 'r', encoding='utf-8') as f:
            self.admin_divisions = json.load(f)
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        

    ## 加载省市区名称编码
    def load_codename_csv(self, filename):
        codename = []
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                ll = line.replace('\n','').split(',')
                codename.append(ll)
        print('加载省市区名称编码')
        return codename


    # def get_city_cdict(self, filename, province):
    #     citys = []
    #     city_cdict = {}
    #     for ad in self.admin_divisions:
    #         if province in ad:
    #             citys = self.admin_divisions[ad]['citys']
    #             break
    #     with open(filename, 'r', encoding='utf-8') as f:
    #         line = f.readline()
    #         line_list = line.split(',')
    #         for ll in line_list:
    #             cl = ll.split('|')
    #             cityname = cl[0] + '市'
    #             citycode = cl[1]
    #             for city in citys:
    #                 if cityname in list(city.keys())[0]:
    #                     city_cdict[cityname] = citycode
    #     return city_cdict

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
                    isProvince = False
                    for ad in self.admin_divisions:
                        if cityname in ad:
                            isProvince = True
                    if not isProvince and cityname != '中国':
                        if not ('自治' in cityname or '地区' in cityname or '盟' in cityname or '林区' in cityname or '县' in cityname):
                            cityname = cl[0] + '市'
                        self.city_cdict[cityname] = citycode

    def gengerate_geojson(self, points, area_code, area_name, parent_code, alevel):
        points_new = []
        for point_block in points:
            pk_new = []
            if len(point_block)==0:
                continue
            for point in point_block:
                pl = point.split(',')
                pk_new.append((float(pl[0]), float(pl[1])))
            points_new.append(pk_new)
        bound_json = {
            "type": "Feature",
            "properties": {
                "adcode": area_code,
                "name": area_name,
                "level": alevel,
                "parent": {
                    "adcode": parent_code
                }},
            "geometry": {
                "type": "MultiPolygon", 
                "coordinates": [points_new]
            }
        }
        return bound_json
    

     ## 获取省市下辖区县列表
    def get_area_list(self, keywords, acode, alevel):
        areas_all = []
        ## 获取省份所有区县
        if alevel == 'province':
            citys = self.admin_divisions[keywords]['citys']
            # 处理直辖市和特别行政区
            if acode in self.zhixia_dict:
                areas_all.append((citys, acode))
            else:
                for city in citys:
                    cname = list(city.keys())[0]
                    if len(city[cname]['areas']) == 0:
                        # 省直属区县
                        areas_all.append(([cname+'_'+city[cname]['acode']], keywords+'_'+self.admin_divisions[keywords]['acode']))
                    else:
                        areas_all.append((city[cname]['areas'], cname+'_'+city[cname]['acode']))
        ## 获取市中所有区县
        if alevel == 'city':
            for ad in self.admin_divisions:
                if self.admin_divisions[ad]['acode'] == acode[0:2]+'0000':
                    citys = self.admin_divisions[ad]['citys']
                    for city in citys:
                        cname = list(city.keys())[0]
                        if city[cname]['acode'] == acode:
                            if len(city[cname]['areas']) == 0:
                                areas_all.append(([cname+'_'+city[cname]['acode']], ad+'_'+self.admin_divisions[ad]['acode']))
                            else:
                                areas_all.append((city[cname]['areas'], cname+'_'+city[cname]['acode']))
                    break
        if alevel == 'district':
            areas_all.append(([keywords+'_'+acode], ''))
        return areas_all


    ## 爬虫区级行政边界
    ## 指定省、市，爬取下辖所有区县
    def parse_bound_json(self, city, area_list, alevel, province):
        for area in area_list:
            a_l = area.split('_')
            area_name = a_l[0]
            area_code = a_l[1]
            ccode = self.city_cdict[city] if city in self.city_cdict else '150'
            url_name='https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd={}&c={}&src=0&wd2=&pn=0&sug=0&l=13&b=(12711357.28,4544689.98;12757437.28,4569233.98)&from=webmap'.format(area_name,ccode)
            #url_name='https://map.baidu.com/?newmap=1&qt=s&da_src=searchBox.button&wd={}&c={}'.format(area_name,ccode)
            json_data=get_json_http(url_name)
            profile_region = ''
            try:
                if 'profile_region' in json_data['result'] and json_data['result']['profile_region']:
                    profile_region = json_data['result']['profile_region']
                elif 'ext' in json_data['content']:
                    profile_region = json_data['content']['ext']['detail_info']['guoke_geo']['geo']
                else:
                    profile_region = json_data['content'][0]['profile_geo']
            except Exception as e:
                print(area_code+'_'+area_name+"： 未找到区域边界")
                write_log(city+'\t'+area_name, os.path.join(self.log_dir, 'crawl_bound_baidu.log'))
                continue
            if len(profile_region) == 0:
                write_log(city+'\t'+area_name, os.path.join(self.log_dir, 'crawl_bound_baidu.log'))
                continue
            points_all = TransCooSys.points_transform_baidu2wgs(profile_region)
            area_bound_json = self.gengerate_geojson(points_all, area_code, area_name, area_code[0:4]+'00', alevel)
            out_path = os.path.join(self.out_dir, area_code[0:2]+'_'+province)
            write_jsonfile(out_path, area_code+'_'+area_name, area_bound_json)
            print(area_code+'_'+area_name)



    ## 爬虫省级、市级行政边界
    def parse_bound_json_citys(self, city_list, province, alevel):
        for city in city_list:
            c_l = city.split('_')
            city_name = c_l[1]
            city_code = c_l[0]
            ccode = self.city_cdict[city_name] if city_name in self.city_cdict else '9001'
            url_name='https://map.baidu.com/?newmap=1&qt=s&da_src=searchBox.button&wd='+city_name+'&c=' + ccode
            json_data=get_json_http(url_name)
            try:
                if 'profile_region' in json_data['result']:
                    profile_region = json_data['result']['profile_region']
                elif 'ext' in json_data['content']:
                    profile_region = json_data['content']['ext']['detail_info']['guoke_geo']['geo']
                else:
                    profile_region = json_data['content'][0]['profile_geo']
            except Exception as e:
                print(city_code+'_'+city_name+"： 未找到区域边界")
                write_log(province+'\t'+city_name, os.path.join(self.log_dir, 'crawl_bound_baidu.log'))
                continue
            if len(profile_region) == 0:
                write_log(province+'\t'+city_name, os.path.join(self.log_dir, 'crawl_bound_baidu.log'))
                continue
            points_all = TransCooSys.points_transform_baidu2wgs(profile_region)
            if alevel == 'province':
                area_bound_json = self.gengerate_geojson(points_all, city_code, city_name, '100000', alevel)
            else:
                area_bound_json = self.gengerate_geojson(points_all, city_code, city_name, city_code[0:2]+'0000', alevel)
            out_path = os.path.join(self.out_dir, city_code[0:2]+'_'+province)
            write_jsonfile(out_path, city_code+'_'+city_name, area_bound_json)
            print(city_code+'_'+city_name)

    ## 爬取区县
    ## 指定区县， 指定城市的所有区县， 指定省份的所有区县
    def crawl_admin_bound_area(self, province, city='', area=''):
        alevel = 'district'
        area_list = []
        citys = self.admin_divisions[province]['citys']
        if province in self.zhixia_citys:
            if city:
                for aa in citys:
                    if city in aa:
                        area_list.append(aa)
            else:
                area_list.extend(citys)
            self.parse_bound_json(city, area_list, alevel, province)
            return
        for cc in citys:
            city_name = list(cc.keys())[0]
            ## 指定市
            if city:
                if city_name == city:
                    areas = cc[city_name]['areas']
                    for aa in areas:
                        if area:
                            if area in aa:
                                area_list.append(aa)
                                break
                        else:
                            area_list.append(aa)
                    self.parse_bound_json(city, area_list, alevel, province)
                    break
            else:
                areas = cc[city_name]['areas']
                for aa in areas:
                    area_list.append(aa)
                self.parse_bound_json(city_name, area_list, alevel, province)
                area_list = []

    ## 爬取指定城市， 指定省份的城市
    def crawl_admin_boun_citys(self, province, city=''):
        city_list = []
        if province in self.zhixia_citys:
            city_list.extend(self.admin_divisions[province]['citys'])
            #self.parse_bound_json_citys(city_list, province, 'city')
            self.crawl_admin_bound_area(province)
            return
        for ad in self.admin_divisions:
            if ad == province:
                citys = self.admin_divisions[province]['citys']
                for cc in citys:
                    city_name = list(cc.keys())[0]
                    acode = cc[city_name]["acode"]
                    if city:
                        if city_name == city:
                            city_list.append(acode+'_'+city_name)
                    else:
                        city_list.append(acode+'_'+city_name)
                self.parse_bound_json_citys(city_list, province, 'city')

    # 爬取指定省份
    def crawl_admin_boun_provinces(self, province):
        area_all = []
        for ad in self.admin_divisions:
            if province == ad:
                acode = self.admin_divisions[ad]['acode']
                area_all.append(acode+'_'+province)
        self.parse_bound_json_citys(area_all, province, 'province')
    
    # 一键爬取全国区划
    def crawl_admin_bound_area_all(self):
        for ad in self.admin_divisions:
            self.crawl_admin_bound_area(ad)
            self.crawl_admin_boun_citys(ad)
            self.crawl_admin_boun_provinces(ad)


def get_json_http(url):
    try:
        r=rq.get(url, timeout=(3,9))
        json_data=json.loads(r.text)
        return json_data
    except Exception as e:
        time.sleep(3)
        return get_json_http(url)

def write_log(context, logfile):
    with open(logfile, 'a', encoding='utf-8') as lf:
        lf.writelines(context+'\n')

def write_jsonfile(out_dir, out_name, json_data):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    file_name = os.path.join(out_dir, out_name+'.json')
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False)


def crawl_bound(filename, outpath):
    cadb = CrawlAreaBoundBaidu(outpath)
    #cadb.crawl_admin_bound_area_all()
    with open(filename, 'r', encoding='utf-8') as ff:
        lines = ff.readlines()
    for line in lines:
        llist = line.replace('\n','').split('\t')
        provincename, cityname, areaname, alevel, depth = llist[0], llist[1], llist[2], llist[3], llist[4]
        if depth == "-1":
            continue
        if alevel == 'province':
            cadb.crawl_admin_boun_provinces(provincename)
            if depth == "1":
                cadb.crawl_admin_boun_citys(provincename)
            elif depth == "2":
                cadb.crawl_admin_boun_citys(provincename)
                if provincename not in cadb.zhixia_citys:
                    cadb.crawl_admin_bound_area(provincename)
        elif alevel == 'city':
            cadb.crawl_admin_boun_citys(provincename, cityname)
            if depth == "1":
                cadb.crawl_admin_bound_area(provincename, cityname)
        elif alevel == 'district':
            cadb.crawl_admin_bound_area(provincename, cityname, areaname) 
        elif alevel == 'all':
            cadb.crawl_admin_bound_area_all()
        

# if __name__ == '__main__':
#     cadb = CrawlAreaBoundBaidu('crawl_output')
#     #cadb.crawl_admin_boun_citys('台湾省') 
#     cadb.crawl_admin_bound_area('河北省', city='沧州市', area='新华区')

# tpro = {
# "监利市": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E7%9B%91%E5%88%A9%E5%B8%82&c=2783&src=0&wd2=%E8%8D%86%E5%B7%9E%E5%B8%82%E7%9B%91%E5%88%A9%E5%B8%82&pn=0&sug=1&l=12",
# "漠河市": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E6%BC%A0%E6%B2%B3%E5%B8%82&c=266&src=0&wd2=%E5%A4%A7%E5%85%B4%E5%AE%89%E5%B2%AD%E5%9C%B0%E5%8C%BA%E6%BC%A0%E6%B2%B3%E5%B8%82&pn=0&sug=1&l=11",
# "龙南市": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E9%BE%99%E5%8D%97%E5%B8%82&c=80&src=0&wd2=%E8%B5%A3%E5%B7%9E%E5%B8%82%E9%BE%99%E5%8D%97%E5%B8%82&pn=0&sug=1&l=10",
# "平城区": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E5%B9%B3%E5%9F%8E%E5%8C%BA&c=365&src=0&wd2=%E5%A4%A7%E5%90%8C%E5%B8%82%E5%B9%B3%E5%9F%8E%E5%8C%BA&pn=0&sug=1&l=11",
# "同仁市": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E5%90%8C%E4%BB%81%E5%B8%82&c=3378&src=0&wd2=%E9%BB%84%E5%8D%97%E8%97%8F%E6%97%8F%E8%87%AA%E6%B2%BB%E5%B7%9E%E5%90%8C%E4%BB%81%E5%B8%82&pn=0&sug=1&l=17",
# "光明区": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E5%85%89%E6%98%8E%E5%8C%BA&c=326&src=0&wd2=%E6%B7%B1%E5%9C%B3%E5%B8%82%E5%85%89%E6%98%8E%E5%8C%BA&pn=0&sug=1&l=10",
# "任泽区": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E4%BB%BB%E6%B3%BD%E5%8C%BA&c=161&src=0&wd2=%E9%82%A2%E5%8F%B0%E5%B8%82%E4%BB%BB%E6%B3%BD%E5%8C%BA&pn=0&sug=1&l=11&b=(13385239.6,3687530.635;13569559.6,3785706.635)&from=webmap&biz_forward={%22scaler%22:1,%22styles%22:%22pl%22}&sug_forward=b332a2bcce4c4917b13c81e0",
# "会理市": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E4%BC%9A%E7%90%86%E5%B8%82&c=266&src=0&wd2=%E5%87%89%E5%B1%B1%E5%BD%9D%E6%97%8F%E8%87%AA%E6%B2%BB%E5%B7%9E%E4%BC%9A%E7%90%86%E5%B8%82&pn=0&sug=1&l=12&b=(12715389.035,4413967.73;12807549.035,4463055.73)&from=webmap&biz_forward={%22scaler%22:1,%22styles%22:%22pl%22}&sug_forward=68d41ef468b039602afc48bb",
# "偃师区": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E5%81%83%E5%B8%88%E5%8C%BA&c=80&src=0&wd2=%E6%B4%9B%E9%98%B3%E5%B8%82%E5%81%83%E5%B8%88%E5%8C%BA&pn=0&sug=1&l=10&b=(11146159.715,2959433.495;11514799.715,3155785.495)&from=webmap&biz_forward={%22scaler%22:1,%22styles%22:%22pl%22}&sug_forward=d5dffbdfe1b94802705e8025",
# "蓬莱区": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E8%93%AC%E8%8E%B1%E5%8C%BA&c=153&src=0&wd2=%E7%83%9F%E5%8F%B0%E5%B8%82%E8%93%AC%E8%8E%B1%E5%8C%BA&pn=0&sug=1&l=12&b=(12499105.25,4066030.31;12591265.25,4115118.31)&from=webmap&biz_forward={%22scaler%22:1,%22styles%22:%22pl%22}&sug_forward=4c6fea1da8a6e39141e1924e",
# "海门区": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E6%B5%B7%E9%97%A8%E5%8C%BA&c=326&src=0&wd2=%E5%8D%97%E9%80%9A%E5%B8%82%E6%B5%B7%E9%97%A8%E5%8C%BA&pn=0&sug=1&l=10&b=(13217264.635,4442923.5649999995;13585904.635,4639275.5649999995)&from=webmap&biz_forward={%22scaler%22:1,%22styles%22:%22pl%22}&sug_forward=013d53c9de1b540ef82d618a",
# "苏州工业园区": "https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=%E8%8B%8F%E5%B7%9E%E5%B7%A5%E4%B8%9A%E5%9B%AD%E5%8C%BA&c=224&src=0&wd2=%E8%8B%8F%E5%B7%9E%E5%B8%82%E8%8B%8F%E5%B7%9E%E5%B7%A5%E4%B8%9A%E5%9B%AD%E5%8C%BA&pn=0&sug=1&l=12"
# }