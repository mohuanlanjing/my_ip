#coding: utf-8
import os

import MySQLdb
import jieba
"""
此脚本用来根据纯真IP库根据需求入库IP
要求，查询数据库时能返回一个十进制整数，能转化成00000000 000000001 00000002 00000003这样的类型
"""

local = MySQLdb.connect('localhost', 'root', '123456', 'ip', charset='utf8')
fin = 'raw.txt'
fout = 'dict.big'
if os.path.isfile(fin):
    os.remove(fin)

if os.path.isfile(fout):
    os.remove(fout)


def getCountry():
    sql = 'select id, name from country'
    with local as cursor:
        cursor.execute(sql)
        ret = cursor.fetchall()
    return dict(ret)

def getProvince(country_id):
    sql = 'select id, name from province where country_id=%s'%country_id
    with local as cursor:
        cursor.execute(sql)
        ret = cursor.fetchall()
    return dict(ret)

def getCity(pid):
    sql = 'select city_index, name from city where province_id=%s'%pid
    with local as cursor:
        cursor.execute(sql)
        ret = cursor.fetchall()
    return dict(ret)

def segment_test():
    ret = jieba.cut_for_search('广东省珠海市')
    print ' '.join(ret)

def buildCityDict():
    """
    citydict:{1:['广州', '珠海'], 2:['桂林', '北海']}
    """
    print 'bilding city dict...'
    province = getProvince(1) #暂时只需要中国，所以先简化
    keys = province.keys()
    ret = dict()
    for key in keys:
        ret[key] = getCity(key)
    print 'build ok'
    return ret

def buildCountryDict():
    """
    countrydict:{1: '中国', 2: '日本'}
    """
    print 'building country dict...'
    ret = getCountry()
    print 'build ok'
    return ret

def buildProvinceDict():
    """
    provincedict: {1:{1:'广东', 2: '北京'}, 2:{}}
    """
    print 'building province dict...'
    country = getCountry()
    keys = country.keys()
    ret = dict()
    for key in keys:
        ret[key] = getProvince(key)
    print 'build ok'
    return ret

codict = buildCountryDict()
podict = buildProvinceDict()
ctdict = buildCityDict()

def segment(content):
    """
    使用结巴分词对fin进行分词并输出到fout
    """
    print 'segment...please wait..'
    words = ' '.join(jieba.cut_for_search(content))
    f = file(fout, 'wb')
    f.write(words.encode('utf8'))

def getRawDatas():
    """
    返回未经处理的数据
    """
    sql = 'select * from ip_test_copy'
    with local as cursor:
        cursor.execute(sql)
    line = cursor.fetchone()
    while line:
        yield line
        line = cursor.fetchone()

def checkData(line):
    """
    对数据库取出来的分词之后的数据进行国省市校验, 并且返回4个8位二进制转换的十进制整数(待定-国-省-市)
    """
    ext = 0
    country = 0
    province = 0
    city = 0
    data = [i.decode('utf8') for i in line.split(' ')]
    for k, v in codict.items():
        if v in data:
            country = k
            if country != 1:
                # 由于不是中国，所以可以直接返回
                return country << 16
            else:
                break
    country = 1
    for k, v in podict[1].items():
        if v in data: 
            province = k
            break
    if province == 0: #如果无省，可以直接当成噪音
        return 0

    for k, v in ctdict[province].items():
        if v in data:
            city = k
            break

    return ext << 24 | country << 16 | province << 8 | city

def writeToText(data, f):
    f.write(data[2].encode('utf8') + '\n')

def preprocess():
    lines = getRawDatas()
    f = file(fin, 'a')
    for line in lines:
        writeToText(line, f)
    f.close()
    content = file(fin).read()
    segment(content)

def process():
    print 'processing....'
    args = []
    dblines = getRawDatas()
    with file(fout) as f:
        fline = f.readline().strip()
        data = dblines.next()
        while fline:
            ip1 = data[0]
            ip2 = data[1]
            code = checkData(fline)
            if code != 0:
                args.append((ip1, ip2, code, None))
            fline = f.readline().strip()
            try:
                data = dblines.next()
            except StopIteration:
                break
    print 'process OK'
    return args

def insert(args):
    sql = 'insert into ip_code(ip1, ip2, code, ext) values(%s, %s, %s, %s) on duplicate key update code=values(code)'
    start = 0
    count = 1000
    with local as cursor:
        while start < len(args):
            cursor.executemany(sql, args[start: start+count])
            print 'inserting....', start
            start += count
    return True


def main():
    preprocess() # 进行预处理，对数据库取出来的原始数据写入文本并分词
    args = process()
    if insert(args):
        print 'all done'
    else:
        print 'may have error'

             
if __name__ == "__main__":
    main()
    #print buildProvinceDict()
    #print buildCityDict()
    #segment_test()
