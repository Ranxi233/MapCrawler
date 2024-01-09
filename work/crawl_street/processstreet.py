import hashlib
import time

def processstreet():
    file = open(r'D:\Data\output\streetall.txt', 'r', encoding='utf-8')
    fileout = open(r'D:\Data\output\zsk_jc_dlyqqyfgm_street.txt', 'a', encoding='utf-8')
    lines = file.readlines()
    for line1 in lines:
        line = line1.split('\t')
        print(line)
        streetjson = eval(line[1].replace('null', 'None'))
        totalFeatures = streetjson["totalFeatures"]
        #print(totalFeatures)
        for t in range(0, totalFeatures):
            try:
                FIRST_TIME = str(int(time.mktime(time.localtime(time.time()))))
                LAST_TIME = str(int(time.mktime(time.localtime(time.time()))))
                COUNTER = '1'
                COLLECT_PLACE = '330000'
                DATA_SOURCE = '0'
                DATA_STATUS = ''
                PROVINCES = ''
                CITY = ''
                COUNTY = ''
                XZQHDM = streetjson["features"][t]["properties"]["code"]
                XLQYMC = streetjson["features"][t]["properties"]["Name"]
                DZMC = ''
                PLACE_NAME = '行政区划'
                coordinates = str(streetjson["features"][t]["geometry"]["coordinates"])
                #print(coordinates)
                str1 = '{"type": "Feature", "properties": {"adcode": '+'"{}"'.format(XZQHDM)+', "name": "{}"'.format(XLQYMC)
                str2 = ', "level": 5, "parent": {'+'"adcode": "{}"'.format(streetjson["features"][t]["properties"]["padcode"]) +'}},'
                str3 = ' "geometry": {"type": "MultiPolygon", "coordinates": '
                result = str1 + str2 + str3 + coordinates
                DESCRIPTIONS = result + '}}'
                #print(DESCRIPTIONS)
                INFOR_CONTENT = line[0]
                MD5file = XZQHDM + XLQYMC + PLACE_NAME
                MD_ID = MD5(MD5file)
                fileout.write(MD_ID + sep +
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
            except Exception as e:
                with open('D:\Data\output\error_log.txt','a', encoding='utf-8', errors='ignore') as fw:
                    fw.write("错误格式："+str(streetjson["features"][t]) + '\n')

    file.close()
    fileout.close()
def MD5(str):
    md5 = hashlib.md5()
    md5.update(str.encode('utf-8'))
    return md5.hexdigest()

if __name__ == '__main__':
    sep = '\|'
    processstreet()