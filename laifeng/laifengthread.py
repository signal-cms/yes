# -*- coding:utf-8 -*-
import copy
import traceback
import requests
from datetime import datetime, timedelta
import time
from time import sleep
import os
import xlwt
from selenium import webdriver
import json
import math
import re
import pymysql
import threading
import warnings


class NormalSpider:	
    
    def request_get(self, url):
        # set adapters for more times to retry when get nothing
        requests.adapters.DEFAULT_RETRIES = 10
        s = requests.session()
        # set keep_alive for clean session
        s.keep_alive = False
        response = requests.get(url, headers=self._get_header, verify=False)
        response.encoding = 'utf-8'
        return response.text

    @property	
    def _get_header(self):
        agent_list = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 '
            'Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
            'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50'
            ]
        t = time.time()
        num = int(math.ceil(t * 1000000) % 10 // 3)
        user_agent = agent_list[num]
        # add connection close to avoid Max retries exceeded with url
        return {'User-Agent': user_agent, 'Connection': 'close', }

    @staticmethod
    def chrome_get(url):
        warnings.filterwarnings('ignore')
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('blink-settings=imagesEnabled=false')
        client = webdriver.Chrome(chrome_options=options)
        client.get(url)
        return client.page_source

    # disable
    @staticmethod
    def firefox_get(url):
        pass


def get_attribute(default, set_value):
    if set_value:
        return set_value
    else:
        return default


class LaiThread:
    
    # the attributes used are defined in __init__, it's convenience to manage
    def __init__(self, log_path=None, list_url=None, room_url=None, error_log=None, run_log=None, database_dict=None, current_rank_url=None):
        default_log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log')
        self.log_path = get_attribute(default_log_path, log_path)
        default_list_url = 'https://www.laifeng.com/category/detail?categoryId=-1&sort=0&page={0}'
        self.list_url = get_attribute(default_list_url, list_url)
        default_room_url = 'https://v.laifeng.com/{0}'
        self.room_url = get_attribute(default_room_url, room_url)
        default_error_log = 'exception.log'
        self.error_log = get_attribute(default_error_log, error_log)
        default_run_log = 'newrun.log'
        self.run_log = get_attribute(default_run_log, run_log)
        default_current_rank_url = 'https://v.laifeng.com/room/{}/screen/stat/fans?_=1576481482233'
        self.current_rank_url = get_attribute(default_current_rank_url, current_rank_url)
        default_database_dict = {'host': 'localhost', 'port': 3306, 'user': 'tester', 'password': 'test123', 'db': 'laifeng'} #'Test_1357642'
        new_get = lambda k: database_dict.get(k, default_database_dict[k])
        if isinstance(database_dict, dict):
            self.database_dict = {'host': new_get('host'), 'port': new_get('port'), 'user': new_get('user'), 'password': new_get('password'), 'db': new_get('db')}
        elif not database_dict:
            self.database_dict = default_database_dict
        else:
            raise TypeError('database_dict is not dict')
        self.anchor_db = 'anchor'
        self.consumer_db = 'consumerrank'
        self.consumer_mark_db = 'consumermark'
        self.anchor_mark_db = 'anchormark'
        self.living_db = 'living'
        self.report_log = 'report.log'
        self.room_buffer = []

    def create_database(self):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        # the sql to create the detail database of anchor
        anchor_sql = 'create table if not exists {}(id int auto_increment, roomid varchar(20), peo varchar(50), xingbi varchar(20), renqi varchar(20), online varchar(20), time datetime,' \
                     'isdelete boolean default false,primary key(id)) charset=utf8'.format(self.anchor_db)
        cu.execute(anchor_sql)
        conn.commit()
        # the sql to create the currentrank database
        consumer_sql = 'create table if not exists {}(id int auto_increment, userid varchar(20), username varchar(50), xingbi varchar(20), roomid varchar(20), time datetime, ' \
                       'isdelete boolean default false,primary key(id)) charset=utf8'.format(self.consumer_db)
        cu.execute(consumer_sql)
        conn.commit()
        # the sql to create anchormark
        anchor_mark_sql = 'create table if not exists {}(id int auto_increment, mark varchar(50), peo varchar(20), time datetime, ' \
                          'primary key(id)) charset=utf8'.format(self.anchor_mark_db)
        cu.execute(anchor_mark_sql)
        conn.commit()
        # the sql to create rankmark
        consumer_mark_sql = 'create table if not exists {}(id int auto_increment, mark varchar(50), time datetime, ' \
                            'primary key(id)) charset=utf8'.format(self.consumer_mark_db)
        cu.execute(consumer_mark_sql)
        conn.commit()
        # living mark db
        living_sql = 'create table if not exists {}(id int unique, content varchar(100), primary key(id)) charset=utf8'.format(self.living_db)
        cu.execute(living_sql)
        conn.commit()
        cu.close()
        conn.close()
    
    @property
    def get_living_room_id(self):
        url_list = []
        js_dict = {}
        room_list = []
        spider = NormalSpider()
        # to get unliving page]
        test_page = 20
        status_cnt = 0
        while True:
            data = spider.request_get(self.list_url.format(test_page))
            js = json.loads(data)
            for dtl in js['response']['data']['data']['items']:
                if dtl['liveStatus'] == 2:
                    status_cnt += 1
                else:
                    status_cnt = 0
            if status_cnt >= 20:
                break
            else:
                test_page += 1
        for i in range(10):
            url_list.append({})
        page_num = 1
        while page_num <= test_page:
            for i in range(10):
                url_list[i][page_num] = self.list_url.format(page_num)
                page_num += 1
                if page_num > test_page:
                    break
        thread_bank = []
        for i in range(10):
            thread_task = threading.Thread(target=self.request_get_more, args=(url_list[i], js_dict, ))
            thread_bank.append(thread_task)
        for i in range(10):
            thread_bank[i].start()
        for i in range(10):
            thread_bank[i].join()
        mark_page = test_page
        for key, values in js_dict.items():
            status_cnt = 0
            if key <= mark_page:
                for dtl in values['response']['data']['data']['items']:
                    is_living = dtl['liveStatus']
                    if is_living == 2:
                        status_cnt += 1
                    else:
                        room_id = dtl['roomId']
                        peo_name = dtl['nickName']
                        room_list.append([room_id, peo_name, is_living])
                    if status_cnt >= 20:
                        mark_page = key
                        break
        return room_list

    @staticmethod
    def request_get_more(url_dict, rst_dict):
        spider = NormalSpider()
        for key, values in url_dict.items():
            data = spider.request_get(values)
            js = json.loads(data)
            rst_dict[key] = copy.deepcopy(js)
        return None
    
    def read_db_all(self, db_name, start_id, end_id):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'), user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        cu.execute('SELECT * FROM {} WHERE id >= {} AND id <= {}'.format(db_name, start_id, end_id))
        data = cu.fetchall()
        cu.close()
        conn.close()
        return data

    def execute_db_more(self, sql_list):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'), user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        for sql in sql_list:
            cu.execute(sql)
            conn.commit()
        cu.close()
        conn.close()
    
    @staticmethod
    def join_num(raw_list):
        int_list = copy.deepcopy(raw_list)
        for i in range(len(int_list)):
            int_list[i] = str(int_list[i])
        return '_'.join(int_list)

    def update_living_list(self, living_list):
        living_num = len(living_list)
        sql_list = []
        group_num = math.ceil(living_num / 10)
        # mark sql
        mark_content = '{0}_{1}'.format(group_num+1, datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
        sql_list.append(r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE content='{2}'".format(self.living_db, 1, mark_content))
        # content sql
        for i in range(group_num - 1):
            sql_list.append(r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE content='{2}'".format(self.living_db, i+2, self.join_num(living_list[i*10:(i+1)*10])))
        # the last sql
        sql_list.append(r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE content='{2}'".format(self.living_db, group_num+1, self.join_num(living_list[(group_num-1)*10:])))
        task_num = math.ceil(group_num / 10)
        t_bank = []
        for i in range(9):
            th = threading.Thread(target=self.execute_db_more, args=(sql_list[i*task_num:(i+1)*task_num], ))
            t_bank.append(th)
        th = threading.Thread(target=self.execute_db_more, args=(sql_list[9*task_num:], ))
        t_bank.append(th)
        for i in range(10):
            t_bank[i].start()
        for i in range(10):
            t_bank[i].join()
    
    def _get_online_info(self, room_list, online_info_dict):
        warnings.filterwarnings('ignore')
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('blink-settings=imagesEnabled=false')
        client = webdriver.Chrome(chrome_options=options)
        run_time = 0
        for room in room_list:
            run_time += 1
            try_time = 0
            client.get(self.room_url.format(room[0]))
            sleep(1)
            while try_time <= 5:
                sleep(1)
                content = client.page_source
                # get api from checkinfo
                if try_time == 5:
                    data = self._get_num(content, 0)
                else:
                    data = self._get_num(content, 3)
                if data is not None:
                    online_info_dict[room[0]] = data
                    break
                else:
                    try_time += 1
        client.quit()
                    
    def _judge_info(self, info_list):
        try:
            if info_list[0][0] == info_list[0][1]:
                return info_list[0][0]
            else:
                return info_list[0][0], info_list[0][1]
        except Exception as err:
            with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                fh.write('\n{2} judge_err:{0},{1}'.format(info_list[0][0], err, datetime.now()))
            return info_list

    # design for four type of present: one_star_corn,much_star_corn,one_other_gift,many_other_gift
    # when more gift type was add, we need to design a bank for it
    def _get_num(self, html_file, allow_zero):
        # get info by re
        pat_star_corn = re.compile(r'<cite title="(.*?)">(.*?)</cite>星币</div>')
        pat_popular = re.compile(r'<cite title="(.*?)">(.*?)</cite>人气')
        pat_online = re.compile(r'<cite title="(.*?)">(.*?)</cite>在线')
        star_corn = pat_star_corn.findall(html_file)
        popular = pat_popular.findall(html_file)
        online = pat_online.findall(html_file)
        if not star_corn:
            pat_star_corn = re.compile(r'<span class="stars_.*?">星币：(.*?)</span>')
            pat_popular = re.compile(r'<span class="room-popular_.*?">人气：(.*?)</span>')
            pat_online = re.compile(r'<span class="online-people_.*?">在线：(.*?)</span>')
            star_corn = pat_star_corn.findall(html_file)[0]
            popular = pat_popular.findall(html_file)[0]
            online = pat_online.findall(html_file)[0]
        else:
            star_corn = self._judge_info(star_corn)
            popular = self._judge_info(popular)
            online = self._judge_info(online)
        # when new type occur, write into file
        if not star_corn:
            with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                fh.write('\n{1}\nexcept_type:{0}'.format(html_file, datetime.now()))
        # judge the correction of info
        try:
            rst = [int(star_corn), int(popular), int(online)]
            for i in range(allow_zero):
                if rst[i] == 0:
                    return None
                else:
                    return star_corn, popular, online
        except Exception as err:
            # return None to retry
            return None

    def _write_online_data(self, room_list, online_info_dict):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'), user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()                        
        for room_info in room_list:
            if online_info_dict.get(room_info[0]):
                data = online_info_dict[room_info[0]]
                try:
                    sql_command = "INSERT INTO {6}(roomid,peo,xingbi,renqi,online,time) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}')".format(room_info[0], room_info[1].replace("'", ''), data[0], data[1], data[2],
                                                                                                                                             datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.anchor_db)
                    cu.execute(sql_command)
                    conn.commit()
                except Exception as err:
                    msg_err = ' '.join([str(m) for m in room_info])
                    data_err = ' '.join([str(da) for da in data])
                    with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                        fh.write('\n{0} write_sql_err:{1}\nmgs:{2}\ndata:{3}'.format(datetime.now(), err, msg_err, data_err))
            else:
                with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                    fh.write('\n{0} no_rank:{1}'.format(datetime.now(), room_info[0]))
        cu.close()
        conn.close()

    def _online_task(self, room_list):
        # room_list = []
        # for roomid in raw_room_list:
        #     room_list.append(self.)
        online_info_dict = {}
        # online selenium task
        online_task_num = math.ceil(len(room_list) / 10) 
        online_task_list = []
        for i in range(1, 10):
            online_task_list.append(room_list[online_task_num * (i - 1):online_task_num * i]) 
        online_task_list.append(room_list[online_task_num * 9:]) 
        online_bank = []
        for i in range(10):
            online_th = threading.Thread(target=self._get_online_info, args=(online_task_list[i], online_info_dict, ))
            online_bank.append(online_th)
        for i in range(10):
            online_bank[i].start()
        for i in range(10):
            online_bank[i].join()
        # write into database
        online_w_bank = []
        for i in range(10):
            online_th = threading.Thread(target=self._write_online_data, args=(online_task_list[i], online_info_dict, ))
            online_w_bank.append(online_th)
        for i in range(10):
            online_w_bank[i].start()
        for i in range(10):
            online_w_bank[i].join()
        # os.system('pkill -9 -u root chrome')

    def _write_offline_data(self, room_list, offline_info_dict):
        insert_sql = "INSERT INTO {0}(userid,username,xingbi,roomid,time) VALUES ('{1}','{2}','{3}','{4}','{5}')"
        sql_list = []
        for room_id in room_list:
            js = offline_info_dict.get(room_id)
            data = js['response']['data']
            if data:
                rank_list = []
                for info in data:
                    user_id = info['userId']
                    user_name = info['nickName']
                    star_coin = info['coins']
                    sql_list.append(insert_sql.format(self.consumer_db, user_id, user_name, star_coin, room_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        self.execute_db_more(sql_list)

    def _off_task_medium(self, room_part, offline_info_dict):
        url_dict = {}
        for room_id in room_part:
            url_dict[room_id] = self.current_rank_url.format(room_id)
        self.request_get_more(url_dict, offline_info_dict)

    def _offline_task(self, room_list):
        offline_info_dict = {}
        # offline request task
        offline_task_num = math.ceil(len(room_list) / 10)
        offline_task_list = []
        for i in range(1, 10):
            offline_task_list.append(room_list[offline_task_num * (i - 1):offline_task_num * i])
        offline_task_list.append(room_list[offline_task_num * 9:])
        offline_bank = []
        for i in range(10):
            offline_th = threading.Thread(target=self._off_task_medium, args=(offline_task_list[i], offline_info_dict,))
            offline_bank.append(offline_th)
        for i in range(10):
            offline_bank[i].start()
        for i in range(10):
            offline_bank[i].join()
        # write into db
        offline_w_bank = []
        for i in range(10):
            offline_th = threading.Thread(target=self._write_offline_data, args=(offline_task_list[i], offline_info_dict, ))
            offline_w_bank.append(offline_th)
        for i in range(10):
            offline_w_bank[i].start()
        for i in range(10):
            offline_w_bank[i].join()

    def _write_mark(self, living_num):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        sql_inquire = 'SELECT id FROM {} ORDER BY id DESC LIMIT 1'.format(self.anchor_db)
        cu.execute(sql_inquire)
        data = cu.fetchall()
        sql_insert = "INSERT INTO {3}(mark,peo,time) VALUES ('{0}','{1}','{2}')".format(data[0][0], living_num, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.anchor_mark_db)
        cu.execute(sql_insert)
        conn.commit()
        get_last_sql = 'SELECT id FROM {} ORDER BY time DESC LIMIT 1'.format(self.consumer_db)
        cu.execute(get_last_sql)
        cur_data = cu.fetchall()
        if cur_data:
            mark_id = cur_data[0][0]
            mark_sql = "INSERT INTO {0}(mark, time) VALUES ('{1}', '{2}')"
            cu.execute(mark_sql.format(self.consumer_mark_db, mark_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
        cu.close()
        conn.close()

    def run_spider(self):
        t1 = time.time()
        room_list = self.get_living_room_id
        living_list = []
        for info in room_list:
            living_list.append(info[0])
        living_mark = self.read_db_all(self.living_db, 1, 1)
        off_line_list = []
        last_living_list = []
        if living_mark:
            mark_string = living_mark[0][1]
            mark_list = mark_string.split('_')
            mark_date = datetime(year=int(mark_list[1]), month=int(mark_list[2]), day=int(mark_list[3]), hour=int(mark_list[4]), minute=int(mark_list[5]),second=int(mark_list[6]))
            now_time = datetime.now()
            if (now_time - mark_date).seconds < 1800:
                old_living_list = self.read_db_all(self.living_db, 2, int(mark_list[0]))
                for info in old_living_list:
                    living_part = info[1].split('_')
                    for roomid in living_part:
                        last_living_list.append(roomid)
        # update living list
        self.update_living_list(living_list)
        if last_living_list:
            for room_id in last_living_list:
                if int(room_id) not in living_list:
                    off_line_list.append(room_id)
        if off_line_list:
            for room_id in off_line_list:
                room_list.append([room_id, '下播检查', 1])
        self._online_task(room_list)
        self._offline_task(off_line_list)
        self._write_mark(len(living_list))       
        t2 = time.time()
        log = '{0} {1}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), t2 - t1)
        with open(os.path.join(self.log_path, self.run_log), 'a') as fh:
            fh.write('\n{0}'.format(log))
        print(log)

    def clean(self):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        # anchor check
        cu.execute('SELECT mark FROM {} ORDER BY id DESC LIMIT 1'.format(self.anchor_mark_db))
        try:
            end = cu.fetchall()[0][0]
        except Exception:
            end = 0
        cu.execute('UPDATE {0} SET isdelete=true WHERE id>{1}'.format(self.anchor_db, end))
        conn.commit()
        # rank check
        cu.execute('SELECT mark FROM {} ORDER BY id DESC LIMIT 1'.format(self.consumer_mark_db))
        try:
            end = cu.fetchall()[0][0]
        except Exception:
            end = 0
        cu.execute('UPDATE {0} SET isdelete=true WHERE id>{1}'.format(self.consumer_db, end))
        conn.commit()
        cu.close()
        conn.close()


if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    lai = LaiThread()
    lai.create_database()
    lai.clean()
    lai.run_spider()
