## 简介

此程序为地图相关爬虫，主要实现四个功能：行政区划数据、区域覆盖面数据、公交地铁线路数据、高速国道路网数据

爬取结果包括 基本信息+经纬度集合

经纬度均使用wgs84坐标系

## 环境: 

python3.8.10

安装模块

pip install geopandas

pip install Polygon

pip install polygon_to_geohashes

## 功能详情：

启动主程序 CrawlController.py

输入爬取类型序号，程序会读取crawl_input目录对应的输入文件

输入目录：crawl_input

爬虫输出目录：crawl_output

标准输出目录：standard_out

### 1 行政区划数据：

读取bound文件，字段为省、市、区、级别、深度

例如：

    浙江省	杭州市	上城区	district	1
    
    浙江省	宁波市		city	2
    
如果将级别设为“all”，则程序将爬取全国所有行政区划数据


### 2 区域面数据：

读取poi文件，字段为poi、行政区划代码、类型

例如：

    杭州萧山国际机场	330109	交通设施;飞机场;飞机场

### 3 公交地铁数据：

读取route文件，字段为序号、路线、省、市

例如：

    1	杭州地铁一号线	浙江	杭州

### 4 路网数据：

读取highway文件，字段为省、市、区、道路名称、道路代号、类型

例如：

    上海市	上海市	浦东新区		S1	高速
