#coding: utf-8
import os

import MySQLdb
import jieba

local = MySQLdb.connect('localhost', 'root', '123456', 'ip', charset='utf8')
fin = 'raw.txt'
fout = 'dict.big'
if os.path.isfile(fin):
    os.remove(fin)

if os.path.isfile(fout):
    os.remove(fout)

def getCountry():
    sql = 'select * from country'
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

def segment_test():
    ret = jieba.cut_for_search('山西省长治市\n河北省保定市涿州市\n伊拉克')
    print ' '.join(ret)

def getCity(pid):
    sql = 'select city from province where province_id=%s'%pid
    with local as cursor:
        cursor.execute(sql)
        ret = [i[0] for i in cursor.fetchall()]
    return ret

def buildCityDict(pid):
    print 'bilding city dict...'

def buildCountryDict():
    print 'building country dict...'
    return getCountry()

def buildProvinceDict():
    print 'building province dict...'
    country = getCountry()
    keys = country.keys()
    ret = dict()
    for key in keys:
        ret[key] = getProvince(key)
    return ret

def segment(content):
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

def writeToText(data, f):
    f.write(data[2].encode('utf8') + '\n')

def main():
    lines = getRawDatas()
    f = file(fin, 'a')
    for line in lines:
        writeToText(line, f)
    f.close()
    content = file(fin).read()
    segment(content)

if __name__ == "__main__":
    main()
    print buildProvinceDict()
