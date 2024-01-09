from optparse import AmbiguousOptionError
import os
import sys
from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parents[1]))
# from utils import PyHiveUtils
#import PyHiveUtils
from crawl import CrawlHighwayProcess as chighway
import json
import hashlib

def MD5(str):
    md5 = hashlib.md5()
    md5.update(str.encode('utf-8'))
    return md5.hexdigest()


municipality_dict = {'北京市', '天津市', '上海市', '重庆市', '香港特别行政区', '澳门特别行政区'}
with open('resources/admin_divisions.json', 'r', encoding='utf-8') as bf:
    admin_divisions_baidu = json.load(bf)

def process_bound_standard(filedir, provincedir, fliename):
    jsonfilepath = os.path.join(filedir, provincedir, fliename)
    with open(jsonfilepath, 'r', encoding='utf-8') as geojson:
        line_json = json.load(geojson)
        xname = fliename.replace('.json','').split('_')[1]
        province_name = provincedir.split('_')[1]
        city_name = ''
        parent_code = str(line_json['properties']['parent']['adcode'])
        adcode = str(line_json['properties']['adcode'])
        if province_name in municipality_dict:
            if adcode[3] == 1:
                city_name = '市辖区' 
            if adcode[3] == 2:
                city_name = '县' 
            county_name = '' if xname == province_name else xname
        else:
            if adcode[2:] == '0000':
                city_name = ''
                county_name = ''
            elif parent_code[2:] == '0000':
                city_name = xname
                county_name = ''
            else:
                citys = admin_divisions_baidu[province_name]['citys']
                for cc in citys:
                    ccName = list(cc.keys())[0]
                    ccCode = cc[ccName]['acode']
                    if ccCode == parent_code:
                        city_name = ccName
                        county_name = xname
                        break
        if county_name:
            xlqymc = county_name
        elif city_name:
            xlqymc = county_name
        else:
            xlqymc = province_name
        xzqhdm = adcode
        place_name = '行政区划'
        descriptions = json.dumps(line_json, ensure_ascii=False)
        md_id = MD5(xzqhdm+xlqymc+place_name)
    return (md_id, '0', '0', '0', '330000', '0', '', province_name, city_name, county_name, xzqhdm, xlqymc, '', place_name, descriptions, '')

def get_all_bound_data(filedir, output_dir = 'standard_out/bound', outfilename = 'all_standard_bound.txt'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    outfile = os.path.join(output_dir, outfilename)
    with open(outfile, 'w', encoding='utf-8') as wf:
        table_header = "md_id\tfirst_time\tlast_time\tcounter\tcollect_place\tdata_source\tdata_status\tprovinces\tcity\tcounty\txzqhdm\txlqymc\tdzmc\tplace_name\tdescriptions\tinfor_content"
        wf.writelines(table_header+'\n')
    for root, ds, fs in os.walk(filedir):
        for d1 in ds:
            for root, dsp, fsp in os.walk(os.path.join(filedir, d1)):
                gj_list = []
                for f in fsp:
                    gj_list.append(process_bound_standard(filedir, d1, f))
                with open(outfile, 'a', encoding='utf-8') as tf:
                    for gj in gj_list:
                        tf.writelines('\t'.join(gj)+'\n')
                gj_list = []
    return outfile

def process_route_standard(filedir, fliename):
    routefile = os.path.join(filedir, fliename)
    with open(routefile, 'r', encoding='utf-8') as rf:
        lines = rf.readlines()
    result_list = []
    for line in lines:
        llist = line.replace('\n','').split('\t')
        data_status = llist[7]
        provinces = llist[1]
        city = llist[2]
        xzqhdm = str(llist[3])
        dldm = llist[4]
        dlmc = llist[5]
        place_name = llist[6]
        extract_desc = llist[8]
        infor_content = llist[9]
        relafulldesc = llist[10]
        descriptions = llist[11]
        md_id = MD5(xzqhdm+dldm+dlmc+place_name)

        result_list.append((md_id, '0','0','0','330000','0', data_status, 
            provinces, city, xzqhdm, dldm, dlmc, place_name, extract_desc, infor_content, relafulldesc, descriptions))
    return result_list


def get_all_route_data(filedir, output_dir = 'standard_out/route', outfilename = 'all_standard_route.txt'):
    outfile = os.path.join(output_dir, outfilename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(outfile, 'w', encoding='utf-8') as wf:
        table_header = "md_id\tfirst_time\tlast_time\tcounter\tcollect_place\tdata_source\tdata_status\tprovinces\tcity\txzqhdm\tdldm\tdlmc\tplace_name\textract_desc\tinfor_content\trelafulldesc\tdescriptions"
        wf.writelines(table_header+'\n')
    for root, ds, fs in os.walk(filedir):
        for f in fs:
            result = process_route_standard(filedir, f)
        with open(outfile, 'a', encoding='utf-8') as tf:
            for res in result:
                tf.writelines('\t'.join(res)+'\n')
        result = []
    return outfile

def process_poi_standard(filedir, fliename):
    poifile = os.path.join(filedir, fliename)
    with open(poifile, 'r', encoding='utf-8') as rf:
        lines = rf.readlines()
    result_list = []
    for line in lines:
        llist = eval(line)
        aname = llist[0]
        adcode = str(llist[5])
        for ad in admin_divisions_baidu:
            if ad in aname:
                province_name = ad
                citys = admin_divisions_baidu[ad]['citys']
                if province_name in municipality_dict:
                    city_name = ''
                    for carea in citys:
                        if adcode in carea:
                                area_name = carea.split('_')[0]
                else:
                    for cc in citys:
                        ccname = list(cc.keys())[0]
                        if ccname in aname:
                            city_name = ccname
                            areas = cc[city_name]['areas']
                            for area in areas:
                                if adcode in area:
                                    area_name = area.split('_')[0]
                break
        poijson = {
            "type": "Feature", 
            "properties": {
                "adcode": adcode, 
                "name": aname, 
                "level": "", 
                "parent": {"adcode": adcode[0:4]+'00'}
            }, 
            "geometry": {
                "type": "MultiPolygon", 
                "coordinates": [[llist[2]]]
            }
        }
        xzqhdm = adcode
        xlqymc = aname
        dzmc = llist[1]
        place_name = llist[4]
        descriptions = json.dumps(poijson, ensure_ascii=False)
        md_id = MD5(xzqhdm+xlqymc+place_name)
        result_list.append((md_id, '0','0','0','330000','0','', 
            province_name, city_name, area_name, xzqhdm, xlqymc, dzmc, place_name, descriptions,''))
    return result_list

def get_all_poi_data(filedir, output_dir = 'standard_out/poi', outfilename = 'all_standard_poi.txt'):
    outfile = os.path.join(output_dir, outfilename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(outfile, 'w', encoding='utf-8') as wf:
        table_header = "md_id\tfirst_time\tlast_time\tcounter\tcollect_place\tdata_source\tdata_status\tprovinces\tcity\tcounty\txzqhdm\txlqymc\tdzmc\tplace_name\tdescriptions\tinfor_content"
        wf.writelines(table_header+'\n')
    for root, ds, fs in os.walk(filedir):
        for f in fs:
            result = process_poi_standard(filedir, f)
        with open(outfile, 'a', encoding='utf-8') as tf:
            for res in result:
                tf.writelines('\t'.join(res)+'\n')
        result = []
    return outfile


# def execute_entrance(filepath):
#     if 'bound' in filepath:
#         out_file, outfilename = get_all_bound_data(filepath)
#         PyHiveUtils.upload_data_tohive(out_file, outfilename, 'ZSK_JC_DLYQQYFGMSJ_01')
#     elif 'poi' in filepath:
#         out_file, outfilename = get_all_poi_data(filepath)
#         PyHiveUtils.upload_data_tohive(out_file, outfilename, 'ZSK_JC_DLYQQYFGMSJ_01')
#     elif 'route' in filepath:
#         out_file, outfilename = get_all_route_data(filepath)
#         PyHiveUtils.upload_data_tohive(out_file, outfilename, 'ZSK_JC_DLYQGJTLXLSJ_01')
#     elif 'highway' in filepath:
#         out_file, outfilename = chighway.get_all_province_lines(filepath)
#         PyHiveUtils.upload_data_tohive(out_file, outfilename, 'ZSK_JC_DLYQLWSJ_01')

#execute_entrance('crawl_output/highway/')
#get_all_bound_data('crawl_output/bound')
#execute_entrance('crawl_output/route')
#execute_entrance('crawl_output/poi')
get_all_poi_data('crawl_output/poi')