#coding: utf-8

import MySQLdb

db = MySQLdb.connect('localhost', 'root', '123456', 'ip', charset='utf8')
fp =  'ip.txt'

def readline(f):
    return f.readline().strip()

def process(line, args):
    l = line.split(' ')
    if '\xef\xbb\xbf' in l[0]:
        return False

    l = [i for i in l if i != '']
    if len(l) == 5:
        l = ' '.join(l).split(' ', 3)

    if len(l) != 4:
        return False

    args.append(tuple(l))
    return True

def insert(args):
    sql = 'insert into ip_test(ip1, ip2, area, idc) values (INET_ATON(%s), INET_ATON(%s), %s, %s)'
    count = 1000 
    l = len(args)
    start = 0
    cursor = db.cursor()
    while start < l:
        print 'inserting: %d'%start
        a = args[start: start+count]
        start += count
        cursor.executemany(sql, a)
    db.commit()
    cursor.close()
    db.close()


if __name__ == "__main__":
    args = []
    f = file(fp)
    line = readline(f)
    while line:
        if not process(line, args):
            print 'error: ', line
        line = readline(f)
    f.close()

    insert(args)


        

    
