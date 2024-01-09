from crawl import CrawlBaiduMap_v1 as cbaidu
from concurrent.futures import ThreadPoolExecutor
import datetime
import time
import os

# 读取城市代号
def get_citycode(filename = 'resources/citycode.txt'):
    f = open(filename, 'r', encoding='utf-8') 
    city_dict={}
    line = f.readline()
    line_list = line.split(',')
    for line in line_list:
        cl = line.split('|')
        cityname = cl[0]
        citycode  = cl[1]
        city_dict[cityname]=citycode
    f.close()
    return city_dict
    

def search_line_info(line_str, province, cityinfo, num, outdir):
    errorfile = os.path.join(outdir, 'lines_bad.txt')
    gjxls=[]
    city_dict = get_citycode()
    if cityinfo in city_dict.keys():
        citycode = city_dict[cityinfo]
    else:
        citycode = city_dict[province]
        line_str = cityinfo + line_str
    try:
        #gjxls=c.crawl_route_and_subway(line_str)
        gjxls=cbaidu.crawl_route_and_subway(line_str,citycode)
    except:
        gjxls=[]
    if len(gjxls)<1:
        f2 = open (errorfile,'w') 
        f2.write(line_str)
        f2.close()
        return []
    lines_str=[]
    try:
        for gjxl in gjxls:
            line_str=''
            line_str+=gjxl[0]
            line_str+='\t'
            line_str+=gjxl[1]
            line_str+='\t'
            line_str+=gjxl[2]
            line_str+='\t'
            if gjxl[3].find('(')>-1:
                line_str+=gjxl[3][0:gjxl[3].find('(')]
                line_str+='\t'
                line_str+=gjxl[3][gjxl[3].find('(')+1:len(gjxl[3])-1]
            else:
                line_str+=gjxl[3]
                line_str+='\t'
                line_str+=''
            line_str+='\t'
            line_str+=gjxl[4]
            line_str+='\t'
            line_str+=gjxl[5]
            line_str+='\t'
            line_str+=gjxl[7]
            line_str+='\t'
            line_str+=gjxl[8]
            line_str+='\t'
            zd_infos=gjxl[9]
            zd_list_infos=[]
            for zd_info in zd_infos:
                zds=zd_info.split(';')
                zd_dict={}
                lon_lat=zds[1].split(',')
                zd_dict["station"]=zds[0]
                zd_dict["lon"]= float(lon_lat[0])
                zd_dict["lat"]=float(lon_lat[1])
                zd_list_infos.append(zd_dict)
                station_result=str(zd_list_infos).replace("'","\"")
            line_str+=str(station_result)
            line_str+='\t'
            line_str+=str(gjxl[10])
            lines_str.append(num +'\t'+line_str)
    except:
        f2 = open (errorfile,'w') 
        f2.write(lines_str)
        f2.close()
        return []
    return lines_str

def execute_crawl(inputfile, outpath):
    outdir = os.path.join(outpath, 'route')
    logdir = os.path.join(outpath, 'log')
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    f1 = open(inputfile, 'r', encoding='utf-8')
    cnt=0
    p=ThreadPoolExecutor(max_workers=30)
    t_list=[]
    while True:
        line=f1.readline()
        if line is None or line=='' or line=='\n':
            break
        lines=line.split('\t')
        t_list.append(p.submit(lambda cxp:search_line_info(*cxp),(lines[1],lines[2],lines[3],lines[0],logdir)))
    outfilename =  os.path.join(outdir, 'result_lines.txt')
    f3 = open(outfilename, 'w', encoding='utf-8')
    while len(t_list)>0:
        i=0
        while i<len(t_list):
            if t_list[i].done():
                return_val=t_list[i].result()

                if len(return_val)>0:
                    for return_v in return_val:
                        f3.write(return_v+'\n')
                del t_list[i]
                i-=1
                cnt+=1
                if cnt%1000==0:
                    print(datetime.datetime.now(),'完成下载数据量：',cnt)
            i+=1
        time.sleep(2)
    f1.close()
    f3.close()