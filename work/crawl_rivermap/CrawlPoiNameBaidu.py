from matplotlib.patches import Polygon
import requests as rq
import json
import TransCooSys
import threading
import time
import queue
from jsonpath import jsonpath
import os
# reuest
def get_json_http(url):
    try:
        r = rq.get(url, timeout=(3, 9))
        json_data = json.loads(r.text)
        return json_data
    except Exception as e:
        time.sleep(3)
        return get_json_http(url)

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


citycode_dict = dict()

## 关键字搜索API
def crawl_poiname_once(userkey, poitype, citycode, pagenum, offset, tag):
    url = "https://api.map.baidu.com/place/v2/search?query={}&tag={}&region={}&city_limit=true&scope=2&output=json&page_size={}&page_num={}&ak={}".format(
        poitype, tag, citycode, offset, pagenum, userkey 
    )
    json_data = get_json_http(url)
    # if 'info' in json_data and 'USER_DAILY_QUERY_OVER_LIMIT' == json_data['info']:
    #     return [], -1
    try:
        count = int(json_data['total']) if json_data['total'] else 0
        pois = json_data['results']

        poi_res_list = []
        for poi in pois:
            name = poi['name']
            address = str(poi['address']) if 'address' in poi else ''
            detail_info = poi['detail_info']
            ptag = detail_info['tag']

            if 'children' in detail_info and len(detail_info['children']) > 0:
                children = poi['children']
                for cc in children:
                    cadd = str(cc['address']).strip()
                    cname = str(cc['name']).strip()
                    ctag = str(cc['tag']).strip()
                    poi_res_list.append([cname, cadd, ctag, name])
            poi_res_list.append([name, address, ptag])
    except Exception as e:
        with open('crawl_output/poiname/crawl_error.log', 'a', encoding='utf-8') as f:
            f.writelines(poitype+'\t'+citycode+'\n')
        return [], 0
    return poi_res_list, count


## 爬虫程序
def crawl_poiname_process(resource_dir):
    amap_keys = get_amap_key(os.path.join(resource_dir, 'baidu_key.txt'))
    type_list = get_firstfield_fromfile(os.path.join(resource_dir, 'poi_type_small.txt')) 
    citycode_list = get_firstfield_fromfile(os.path.join(resource_dir, 'BaiduMap_cityCode_1102.txt'))
    city_complete_list = set(get_firstfield_fromfile('crawl_output/poiname_baidu/city_success.txt'))
    global type_dict
    type_dict = set(type_list)
    offset = 20
    keymax = 4000
    crawl_result = []
    userkey = get_userkey(amap_keys)
    key_num = 0
    totalcount = 0
    citycode_list = ['杭州市西湖区']
    #tag = '中餐厅,外国餐厅,小吃快餐店,蛋糕甜品店,咖啡厅,茶座,酒吧'
    for citycode in citycode_list:  
        # 跳过已经爬取的城市
        if citycode in city_complete_list:
            continue
        for poitype in type_list:
            isContinue = True
            pagenum = 1
            count = 0
            # poitype = '中餐馆'
            tag = poitype
            while isContinue:
                poi_res_list, countonce = crawl_poiname_once(userkey, poitype, citycode, str(pagenum), str(offset), tag)
                crawl_result.extend(poi_res_list)
                if key_num >= keymax or count == -1:
                    amap_keys[userkey] = 1
                    userkey = get_userkey(amap_keys)
                    print(userkey)
                    key_num = 0
                    count = 0
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
        with open('crawl_output/poiname_baidu/all_poiname.txt', 'a', encoding='utf-8') as tf:
            for cr in crawl_result:
                tf.writelines('\t'.join(cr)+'\n')
            crawl_result = []
        with open('crawl_output/poiname_baidu/city_success.txt', 'a', encoding='utf-8') as cf:
            cf.writelines(citycode+'\n')

#crawl_poiname_process('resources/baidu')

url = 'https://wqs.jd.com/wxsq_project/portal/m_category/index.shtml?searchFrom=bottom'
url = 'https://huodong.taobao.com/wow/tbhome/act/market-list'
json_data = get_json_http(url)
print(1)