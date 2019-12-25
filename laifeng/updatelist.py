import requests
import time
import math
import json
import pymysql


def get_response(url):
    header = get_header()
    response = requests.get(url, headers=header, verify=False)
    response.encoding = 'utf-8'
    return response.text


def get_header():
    agent_list = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
                  'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0',
                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
                  'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50']
    t = time.time()
    num = int(math.ceil(t * 1000000) % 10 // 3)
    user_agent = agent_list[num]
    return {'User-Agent': user_agent}


if __name__ == '__main__':
    conn = pymysql.connect(host='localhost', port=3306, user='tester',
                           password='test123', db='laifeng', charset='utf8')
    cu = conn.cursor()
    data = get_response('https://www.laifeng.com/category/detail?categoryId=-1&sort=0&page=1')
    js = json.loads(data)
    ttl_page = js['response']['data']['data']["totalPages"]
    for p in range(1, ttl_page+1):
        data = get_response('https://www.laifeng.com/category/detail?categoryId=-1&sort=0&page={0}'.format(p))
        js = json.loads(data)
        print(js)
        for dtl in js['response']['data']['data']['items']:
            room_id = dtl['roomId']
            peo_name = dtl['nickName']
            insert_sql = "INSERT INTO ROOM_LIST(ROOMID,NAME) VALUES ({0},'{1}')".format(room_id, peo_name)
            try:
                cu.execute(insert_sql)
                conn.commit()
            except Exception as err:
                with open('.listlog.log', 'a') as fh:
                    fh.write('{0}: {1}'.format(time.time(), err))
    cu.close()
    conn.close()


