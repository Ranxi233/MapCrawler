from crawl import CrawlBaiduMap_v1 as c
from concurrent.futures import ThreadPoolExecutor
import datetime
import time
import json
import os


def load_area_code(filename):
    area_code_dict={}
    with open (filename, 'r', encoding='utf-8') as f:
        line=f.readline()
        while line:
            lines=line.split(' ')
            if len(lines)<2:
                line=f.readline()
                continue
            area_code_dict[lines[0]]=lines[1].replace('\u3000','').replace('\n','')
            line=f.readline()
    return area_code_dict


area_code_dict = load_area_code("resources/area_code.txt")
outdir = ''
logdir = ''


## 调用爬虫程序
def address_search(addr,code,addr_type):
    return_val=[]
    try:
        return_val=c.crawl_poi_impl(addr)
        if len(return_val[5])==0:
            return []
        return_val.append(addr)
        return_val.append(code)
        return_val.append(addr_type.replace('/n',''))
    except:
        f2 = open (os.path.join(logdir, "poi_amap_bad.log"), 'a', encoding='utf-8')
        f2.write(addr+'\n')
        f2.close()
    return return_val



def execute_crawl(target_file):
    p=ThreadPoolExecutor(max_workers=30)
    t_list=[]
    f = open ( target_file,'r', encoding='utf-8') 
    poi_list = []
    cnt=0
    row_num=0
    while True:
        line=f.readline()
        if line is None or line=='' or line=='\n':
            break
        lines=line.split('\t')
        area_code = ''
        area_name = ''
        ltype  = ''
        if len(lines)>1:
            area_code=lines[1]
            area_name=area_code_dict[area_code[0:2]+'0000']
        if len(lines)>2:
            ltype = lines[2]
        if (area_code[0:4]+'00') in area_code_dict:
            area_name+=area_code_dict[area_code[0:4]+'00']
        if (area_code) in area_code_dict:
            area_name+=area_code_dict[area_code]
        addr=area_name+lines[0]
        t_list.append(p.submit(lambda cxp:address_search(*cxp),(addr,area_code,ltype)))
        row_num+=1
        if row_num%600000==0:
            while len(t_list)>0:
                i=0
                while i<len(t_list):
                    if t_list[i].done():
                        return_val=t_list[i].result()
                        if len(return_val)>5:
                            if len(return_val[5])>0:
                                #f2.write(str(return_val)+'\n')
                                poi_list.append(str(return_val))
                        del t_list[i]
                        i-=1
                        cnt+=1
                        if cnt%10000==0:
                            print(datetime.datetime.now(),'完成下载数据量：',cnt)
                    i+=1
                time.sleep(2)
    f.close()

    while len(t_list)>0:
        i=0
        while i<len(t_list):
            if t_list[i].done():
                return_val=t_list[i].result()
                if len(return_val)>0:
                    poi_list.append(str(return_val))
                del t_list[i]
                i-=1
                cnt+=1
                if cnt%10000==0:
                    print(datetime.datetime.now(),'完成下载数据量：',cnt)
            i+=1
        time.sleep(2)
    return poi_list


def get_poi_result(poi_list, outfilename = 'result_poi.txt'):
    #f = open (crawl_res_file,'r', encoding='utf-8')
    tmp_type_dict1={}
    #while True:
        #line=f.readline()
    for line in poi_list:
        if line is None or line=='' or line=='[]':
            break
        tmp_list=eval(line)
        tmp_addr_list=[]
        tmp_addr_list.append(tmp_list[0])
        tmp_addr_list.append(tmp_list[1])
        tmp_addr_list.append(tmp_list[5])
        tmp_addr_list.append(tmp_list[7])
        tmp_addr_list.append(tmp_list[2])
        tmp_addr_list.append(tmp_list[6])
        tmp_addr_list.append(tmp_list[8].replace('\n',''))
        tmp_type_dict1[tmp_list[0]]=str(tmp_addr_list)
    #f.close()
    result_file = os.path.join(outdir, outfilename)
    f2 = open (result_file, 'w', encoding='utf-8')
    for key in tmp_type_dict1:
        f2.write(tmp_type_dict1[key]+'\n')
    f2.close()
    return result_file

## 成功爬取的区域列表
def get_crawl_arealist(poi_list, target_file):
    #f = open (crawl_res_file,'r', encoding='utf-8')
    sj_dict={}
    cnt = 0
    for line in poi_list:
        if line is None or line=='' or line=='[]':
            break
        if len(eval(line)[5])<1:
            continue
        sj_dict[eval(line)[0]]=str(line)
        cnt+=1
    print(cnt)

    f = open (target_file,'r', encoding='utf-8')
    cl_addr_list=[]
    cnt=0
    while True:
        line=f.readline()
        if line is None or line=='':
            break
        lines=line.split('\t')
        cnt+=1
        if cnt%100000==0:
            print('已扫描数据量：',cnt)
        if len(lines) > 1:
            area_code=lines[1]
            area_name=area_code_dict[area_code[0:2]+'0000']
            if (area_code[0:4]+'00') in area_code_dict:
                area_name+=area_code_dict[area_code[0:4]+'00']
            if (area_code) in area_code_dict:
                area_name+=area_code_dict[area_code]
            addr=area_name+lines[0]
        else:
            addr=lines[0]
        if addr in sj_dict:
            cl_addr_list.append(line)
    f.close()

    f2 = open (os.path.join(logdir, "poi_amap_success.txt"),'w', encoding='utf-8')
    for save_info in cl_addr_list:
        f2.write(save_info)
    f2.close()


def execute_poi(filename, output_path):
    global outdir
    global logdir
    outdir = os.path.join(output_path, 'poi')
    logdir = os.path.join(output_path, 'log')
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    if not os.path.exists(logdir):
        os.makedirs(logdir)
    crawl_res_file = execute_crawl(filename)
    result_file = get_poi_result(crawl_res_file)
    get_crawl_arealist(crawl_res_file, filename)
    return result_file



#execute_poi('crawl_input/air_poi.txt', 'crawl_output')