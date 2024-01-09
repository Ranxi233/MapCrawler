from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import requests as rq
headers = {
    'content-encoding': 'gzip',
    'content-type': 'application/json; charset=utf-8',
    'date': 'Thu, 30 Jun 2022 02:00:14 GMT',
    'eagleeye-traceid': '2120411616565544142367237e5dd0',
    'etag': 'W/"6b84-E80ZTyqRrvSrPeDtKzXQeSMPMs8"',
    'gsid': '033040076182165655441424000017286016656',
    'server': 'Tengine',
    'strict-transport-security': 'max-age=31536000',
    'timing-allow-origin': '*',
    'vary': 'Accept-Encoding',
    'x-powered-by': 'Express',
    # 'user-agent': 'user-agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'user-Agent': UserAgent().Chrome,
    'cookie': 'cna=yvuRGuMrU2MCASeqCfOWmGT/; _uab_collina=165641408901798700454793; xlly_s=1; passport_login=NzYzMzE3MzQ0LGFtYXBLNU80VjEwZiwzZ2wzY3RjZ3VwbDZxaWJlamlzbWFyZWpqbGN3Z3p6aywxNjU2NTUxNDI4LFpHWmtZalpoTURSbVlqWmpNRFEwWlRWbE1tRTBZMlUwWkdOaE1EZ3pZMlE9; oauth_state=f874984e25f48ef13f4ecaac8bb9d989; guid=1ad9-d4f8-f32a-0e6d; gray_auth=2; dev_help=hvfQi6DG%2F3uEKZnfOpvlNTU1Zjc5YTBiMDlmMjZlMmVjYjMyYmI5NmYzNDJmNWZlNDhhY2YxNjdjMWY4MmUyYjZmMmM5YTA3ZDU3NWU4M2LSrjQSdQrxdIPhQxGrHIAB6Qqw2dLuXryqtn9%2F%2B1tkY1dcSCtNZJzxDaZgg%2FlMU3%2BasVffu%2F%2BCOGn1zzOMP69snDQmQ5gULchwMD90BnykxVGkYprOB6krfQmkX8SQwdM%3D; x5sec=7b227761676272696467652d616c69626162612d616d61703b32223a2238663764386234393165313261303166616137636132616535663030396630394350475739705547454f7139766f616c37636a334b4444656c35757041773d3d227d; x-csrf-token=21ef69cff417f63b0dac3cfd8ec40354; tfstk=cBzlB0VYauoSEIo0fag5J1PJatCAZxOqeJy7u_W0oyDxgXaViAK2bxMCmxX1UH1..; l=eBStVEmgLlAoKOO2BOfwnurza77tIIRAguPzaNbMiOCPOLC95VYCW6bIzAYpCnGVhsGpR3PqcrtpBeYBqQAonxvT8NcZJVkmn; isg=BENDs2CEJReK6ulkui67igtg0gftuNf6Li9F5XUgmaIZNGNW_YjeSj-mqsR6lC_y'
}


city_url_list =  ['10', '30', '50', '03', '01', '11', '13', '15', '20', '21', '31', '23', '35', '33',
 '25', '45', '43', '41', '51', '53', '57', '40', '61', '55', '65', '85', '71', '73', 
 '81', '75', '83', 'hongkong', 'macao', 'taiwan']

def crwal_zipcode():
    result_list = []
    for cc in city_url_list:
        url = 'http://www.ip138.com/'+cc
        try:
            r=rq.get(url, timeout=(3,9),headers=headers)
            r.encoding='utf-8'
            soup = BeautifulSoup(r.text, 'html.parser')
            print(type(soup))
            lm_outer = soup.find_all(class_='table-outer')
            for lmst in lm_outer:
                citytext = lmst.contents[1].text
                areatext = lmst.contents[3].contents[1].contents[3].text
                city_list = citytext.split("\n")
                zip_city = city_list[1].split('邮编')[0]
                area_list = areatext.split("\n\n\n")
                for ast in area_list:
                    ast = ast.replace('\n\n', '')
                    zip_area = ast.split('\n')[0]
                    zip_code = ast.split('\n')[1]
                    result_list.append('\t'.join([zip_city, zip_area, zip_code])+'\n')
        except Exception as e:
            print(cc)
            continue
    with open('zipcode.txt', 'w', encoding='utf-8') as f:
        f.writelines(result_list)

