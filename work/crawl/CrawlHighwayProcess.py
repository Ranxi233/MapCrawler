
import json
import os
import hashlib
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
import TransCooSys
####### 路网数据 #########
## 处理百度爬取的线路数据

def load_jsonfile(path):
    with open(path, 'r', encoding='utf-8') as bf:
        json_data = json.load(bf)
        return json_data

# 城市行政区划编码
bsc_city = load_jsonfile('resources/bsc_city.json')
digit_dict = {'一': '1', '二': '2', '三': '3', '四': '4', '五': '5', '六': '6', '七': '7', '八': '8', '九': '9', '零': '0'}

## 处理高速公路 名称和编码
def road_highway_id2name(name, alias):
    name_list = name.split(',') if isinstance(name, str) else name[0].split(',')
    id_set = set()
    alias_set = set()
    for nn in name_list:
        if 'S' in nn or 'G' in nn:
            id_set.add(nn)
        else:
            alias_set.add(nn)
    if alias: 
        alias_list = alias.split(',') if isinstance(alias, str) else alias[0].split(',')
        for al in alias_list:
            if 'S' in al or 'G' in al:
                id_set.add(al)
            else:
                alias_set.add(al)
    way_id = ';'.join(id_set)
    way_alias = ';'.join(alias_set)
    return way_id, way_alias

## 处理国道 名称和编码 
def road_nationroad_id2name(name, alias):
    way_id = 'G'
    way_alias = ''
    # 计算id, 取 G + 数字编号
    namestr = name if isinstance(name, str) else name[0]
    for n in namestr:
        if n in digit_dict:
            way_id = way_id + digit_dict[n]
        elif str.isdigit(n):
            way_id = way_id + n
    # 计算name，优先取 别名(永武线), 其次取 省道 + 数字编号
    if alias:
        alias_list = alias.split(',') if isinstance(alias, str) else alias[0].split(',')
        for al in alias_list:
            if 'G' in al:
                continue
            way_alias = way_alias + al
    if not way_alias:
        way_alias = way_id.replace('G', '国道')
    # 处理辅路
    if '辅路' in name:
        way_alias = way_alias + '辅路' if '辅路' not in way_alias else way_alias
        way_id = way_id  + '辅路'
    return way_id, way_alias

## 处理省道 名称和编码
def road_provinceroad_id2name(name, alias):
    way_id = 'S'
    way_alias1 = ''
    way_alias = ''
    # 计算id, 取 S + 数字编号
    namestr = name if isinstance(name, str) else name[0]
    for n in namestr:
        if n in digit_dict:
            way_id = way_id + digit_dict[n]
            way_alias1 = way_alias1 + n
        elif str.isdigit(n):
            way_id = way_id + n
    # 计算name
    if alias: 
        alias_list = alias.split(',') if isinstance(alias, str) else alias[0].split(',')
        for al in alias_list:
            if 'S' in al:
                continue
            way_alias = way_alias + al
    if not way_alias:
        if way_alias1:
            way_alias = way_alias1 + '省道'
        else: 
            way_alias = way_id.replace('S', '省道')
    if way_alias == '省道' and way_id == 'S':
        way_alias = name
        way_id = ''
    return way_id, way_alias

# 保存出现的线路 名称和编码 
way_id_alias = {}
way_alias_id = {}
def transform_id2name_savedict(name, alias, line_type):
    if line_type == '省道':
        way_id, way_alias = road_provinceroad_id2name(name, alias)
    elif line_type == '国道':
        way_id, way_alias = road_nationroad_id2name(name, alias)
    elif line_type == '高速':
        way_id, way_alias = road_highway_id2name(name, alias)
    way_id_alias[way_id] = way_alias
    way_id_alias[way_alias] = way_id


## 处理线路名称和编码
def transform_id2name(name, alias, line_type):
    if line_type == '省道':
        way_id, way_alias = road_provinceroad_id2name(name, alias)
    elif line_type == '国道':
        way_id, way_alias = road_nationroad_id2name(name, alias)
    elif line_type == '高速':
        way_id, way_alias = road_highway_id2name(name, alias)
    if way_id and  not way_alias:
        way_alias = way_id_alias[way_id]
    elif not way_id and way_alias:
        way_id = way_id_alias[way_alias]
    return way_id, way_alias


def MD5(str):
    md5 = hashlib.md5()
    md5.update(str.encode('utf-8'))
    return md5.hexdigest()


## 处理经纬度数组
def coordinates_toString(coordinates):
    coordinates_new = []
    for coor in coordinates:
        c_new = []
        if len(coor) == 2 and str.isdigit(str(coor[0])[0]):
            wg1, wg2 = TransCooSys.gcj02_to_wgs84(coor[0], coor[1])
            wg1 = float(format(wg1, '.6f'))
            wg2 = float(format(wg2, '.6f'))
            c_new.append(str(wg1)+','+str(wg2))
            #c_new.append(str(coor[0])+','+str(coor[1]))
        else:
            for c in coor:
                wg1, wg2 = TransCooSys.gcj02_to_wgs84(c[0], c[1])
                wg1 = float(format(wg1, '.6f'))
                wg2 = float(format(wg2, '.6f'))
                c_new.append(str(wg1)+','+str(wg2))
                #c_new.append(str(coor[0])+','+str(coor[1]))
        coordinates_new.append('[' + '],['.join(c_new) + ']')
    return coordinates_new

## 读取省内指定类型的所有道路geojson，存入list
def get_province_lines(province_name, filepath, road_type):
    line_list = []
    for root, ds, fs in os.walk(filepath):
        for f in fs:
            with open(os.path.join(filepath, f), 'r', encoding='utf-8') as geojson:
                line_json = json.load(geojson)
                line_list.append(line_json)
                lines=line_json['properties']
                transform_id2name_savedict(lines['name'], lines['alias'], road_type)
    way_list = []   
    for json_data in line_list:
        lines=json_data['properties']
        coordinates=json_data['geometry']['coordinates'][0]
        way_id, alias = transform_id2name(lines['name'], lines['alias'], road_type)
        city = lines['city']
        coordinates_new = coordinates_toString(coordinates)
        coordinates_str = "[[" + '],['.join(coordinates_new) + ']]'
        city_list = city.split('市') if isinstance(city, str) else city[0].split('市')
        area_name = ''
        city_name = city_list[0]+'市'
        if '地区' in city_list[0]:
            city_name = city_list[0].split('地区')[0]+'地区'
        elif '自治州' in city_list[0]:
            city_name = city_list[0].split('自治州')[0]+'自治州'
        elif '盟' in city_list[0]:
            city_name = city_list[0].split('盟')[0]+'盟'
        elif '自治县' in city_list[0]:
            city_name = city_list[0].split('自治县')[0]+'自治县'
        elif '林区' in city_list[0]:
            city_name = city_list[0].split('林区')[0]+'林区'
        elif '县' in city_list[0]:
            city_name = city_list[0]
        city_code = bsc_city[city_name]
        if len(city_list) == 2:
            area_name = city_list[1]
        elif len(city_list) == 3:
            area_name = city_list[1]+'市'
        ## 标准表结构输出数据
        md_id = MD5(city_code+road_type+coordinates_str)
        way_list.append((md_id, '0', '0', '0', city_code[0:2]+'0000', '0', '1',
            province_name, city_name, area_name, city_code, road_type, '', way_id, alias, coordinates_str,
            '0', '0', '0', '0'))
    return way_list


## 读取所有行政区划geojson文件，根据标准表结构输出txt
## 输入： 行政区划目录， 输出目录， 输出文件名
## 输出： 输出文件完整路径， 输出文件名
def get_all_province_lines(bound_dir, output_dir = 'standard_out/highway', outfilename = 'all_standard_highway.txt'):
    admin_divisions_baidu = load_jsonfile('resources/admin_divisions.json')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_file = os.path.join(output_dir, outfilename)
    with open(out_file, 'w', encoding='utf-8') as wf:
        table_header = "md_id\tfirst_time\tlast_time\tcounter\tcollect_place\tdata_source\tdata_status\tprovinces\tcity\tcounty\txzqhdm\tdevice_type\tsxh\tdlmc\tdldm\tdescriptions\tisno\tisno1\tisno2\tisno3"
        wf.writelines(table_header+'\n')
    for province_name in admin_divisions_baidu:
        if province_name in {'澳门特别行政区', '香港特别行政区', '台湾省'}:
            continue
        way_list = []
        way_list.extend(get_province_lines(province_name, os.path.join(bound_dir, province_name, '高速'), '高速'))
        way_list.extend(get_province_lines(province_name, os.path.join(bound_dir, province_name, '国道'), '国道'))
        way_list.extend(get_province_lines(province_name, os.path.join(bound_dir, province_name, '省道'), '省道'))
        with open(out_file, 'a', encoding='utf-8') as wf:
            for w in way_list:
                wf.writelines('\t'.join(w) + '\n')
        way_list = []
    return out_file

if __name__ == '__main__':
    #out_file, outfilename = get_all_province_lines('waynet/highway/')
    #PyHiveUtils.upload_data_tohive(out_file, outfilename, 'ZSK_JC_DLYQLWSJ_01')
    #PyHiveUtils.upload_data_tohive('waynet/highway/result/all_standard_highway.txt', 'all_standard_highway.txt', 'ZSK_JC_DLYQLWSJ_01')
    admin_divisions_baidu = load_jsonfile('resources/admin_divisions.json')
    for ad in admin_divisions_baidu:
        print(ad)