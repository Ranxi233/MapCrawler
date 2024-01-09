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
from lxml import etree
from bs4 import BeautifulSoup
import pandas as pd
import numpy  as np

#### 行政区划数据 ####
## 爬取统计局街道数据

# 根目录
ROOT_DIR = 'D:/py_workspace/'
# 资源目录
RESOURECE_DIR = ROOT_DIR + 'resources/'

def get_json_http(url):
    try:
        r=rq.get(url, timeout=(3,9))
        r.encoding='utf-8'
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup
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



def crawl():
    all_url =  []
    url = 'http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/2022/'
    index_url = url+'index.html'
    soup = get_json_http(index_url)
    provincetr = soup.find_all(class_='provincetr')
    for ptr in provincetr:
        for pcon in ptr.contents:
            provcode = pcon.next.attrs['href'].split('.')[0]
            provname = pcon.next.text
            if provcode not in {'63'}:
                continue
            province_url = url + pcon.next.attrs['href']
            province_soup = get_json_http(province_url)
            citytr = province_soup.find_all(class_='citytr')
            for ctr in citytr:
                for ccon in ctr.contents:
                    try:
                        if str(ccon.contents[0].text).endswith('000'):
                            continue
                        citycode = ccon.contents[0].attrs['href'].split('/')[1].replace('.html', '')
                        cityname = ccon.contents[0].text
                        city_url = url + ccon.contents[0].attrs['href']
                        if citycode != '6327':
                            continue
                    except:
                        continue
                    city_soup = get_json_http(city_url)
                    countytr = city_soup.find_all(class_='countytr')
                    for cotr in countytr:
                        for cocon in cotr.contents:
                            try:
                                if str(cocon.contents[0].text).endswith('000'):
                                    continue
                                countycode = cocon.contents[0].attrs['href'].split('/')[1].replace('.html', '')
                                countyname = cocon.contents[0].text
                                county_url = '/'.join(city_url.split('/')[0:-1]) + '/'+ cocon.contents[0].attrs['href']
                            except:
                                continue
                            county_soup = get_json_http(county_url)
                            towntr = county_soup.find_all(class_='towntr')
                            for ttr in towntr:
                                ttcon = ttr.contents
                                towncode=ttcon[0].text
                                townname=ttcon[1].text
                                all_url.append(
                                    '\t'.join([provcode,provname,citycode,cityname,countycode,countyname,towncode,townname])+'\n'
                                )
                            break
                    break
            with open('town.txt', 'a', encoding='utf-8') as f:
                f.writelines(all_url)      
                all_url=[]

def quchong():
    df = pd.read_csv('town.txt', encoding='utf-8', sep='\t')
    df2 = df.drop_duplicates()
    df2.to_csv('all_street_0526.txt', sep='\t', index=None)
    print(df.size)
    print(df2.size)


if __name__ == '__main__':
    #tree = get_json_http('http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/2022/index.html')
    #town_list = crawl()
    #quchong()
    # df1 = pd.read_csv(RESOURECE_DIR+'all_street_0525.txt',encoding='utf-8', header=None, sep='\t')
    df1 = pd.read_csv(ROOT_DIR+'crawl_input/2022年全国行政区域.txt',encoding='utf-8', header=None, sep='\t')
    df2 = pd.read_csv(RESOURECE_DIR+'all_street_0526.txt',encoding='utf-8', header=None, sep='\t')
    s1 = set(np.array(df1[7]).tolist())
    #s1 = set(np.array(df1[0]).tolist())
    s2 = set(np.array(df2[6]).tolist())
    print(len(s1))
    print(len(s2))
    lll=list(s1-s2)
    lll.sort()
    print(lll)
    
