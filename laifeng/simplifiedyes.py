# -*- coding:utf-8 -*-
# this program design for spider the msg of status_change anchor and hot anchor
import copy
import requests
from datetime import datetime
import time
from time import sleep
import os
from selenium import webdriver
import json
import math
import re
import pymysql
import threading
import warnings
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from lxml import etree
import traceback
import re


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
            'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50']
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
    def __init__(
            self,
            log_path=None,
            list_url=None,
            room_url=None,
            error_log=None,
            run_log=None,
            database_dict=None,
            current_rank_url=None):
        default_log_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'log')
        self.log_path = get_attribute(default_log_path, log_path)
        default_list_url = 'https://www.laifeng.com/category/detail?categoryId=-1&sort=0&page={0}'
        self.list_url = get_attribute(default_list_url, list_url)
        default_room_url = 'https://v.laifeng.com/{0}'
        self.room_url = get_attribute(default_room_url, room_url)
        default_error_log = 'exception{}.log'
        self.error_log = get_attribute(default_error_log, error_log)
        default_run_log = 'run{}.log'
        self.run_log = get_attribute(default_run_log, run_log)
        default_current_rank_url = 'https://v.laifeng.com/room/{}/screen/stat/fans?_=1576481482233'
        self.current_rank_url = get_attribute(
            default_current_rank_url, current_rank_url)
        default_database_dict = {
            'host': 'localhost',
            'port': 3306,
            'user': 'tester',
            'password': 'Test_1357642',
            'db': 'laifeng'}

        def new_get(k): return database_dict.get(k, default_database_dict[k])
        if isinstance(database_dict, dict):
            self.database_dict = {
                'host': new_get('host'),
                'port': new_get('port'),
                'user': new_get('user'),
                'password': new_get('password'),
                'db': new_get('db')}
        elif not database_dict:
            self.database_dict = default_database_dict
        else:
            raise TypeError('database_dict is not dict')
        self.anchor_db = 'anchor'
        self.consumer_db = 'consumerrank'
        self.consumer_mark_db = 'consumermark'
        self.anchor_mark_db = 'anchormark'
        self.living_db = 'living'
        self.useless_db = 'useless'
        self.zero_db = 'zero'
        self.hot_anchor_db = 'hot'

    def create_database(self):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        # the sql to create the detail database of anchor
        anchor_sql = 'create table if not exists {}(id int auto_increment, roomid varchar(20), peo varchar(50), ' \
                     'xingbi varchar(20), renqi varchar(20), online varchar(20), time datetime,' \
                     'isdelete boolean default false,primary key(id)) charset=utf8'.format(self.anchor_db)
        cu.execute(anchor_sql)
        conn.commit()
        # the sql to create the currentrank database
        consumer_sql = 'create table if not exists {}(id int auto_increment, userid varchar(20), ' \
                       'username varchar(50), xingbi varchar(20), roomid varchar(20), time datetime, ' \
                       'isdelete boolean default false,primary key(id)) charset=utf8'.format(self.consumer_db)
        cu.execute(consumer_sql)
        conn.commit()
        # the sql to create anchormark
        anchor_mark_sql = 'create table if not exists {}(id int auto_increment, mark varchar(50), peo varchar(20), ' \
                          'time datetime, primary key(id)) charset=utf8'.format(self.anchor_mark_db)
        cu.execute(anchor_mark_sql)
        conn.commit()
        # the sql to create rankmark
        consumer_mark_sql = 'create table if not exists {}(id int auto_increment, mark varchar(50), time datetime, ' \
                            'primary key(id)) charset=utf8'.format(self.consumer_mark_db)
        cu.execute(consumer_mark_sql)
        conn.commit()
        # living mark db
        living_sql = 'create table if not exists {}(id int unique, content varchar(150), primary key(id)) ' \
                     'charset=utf8'.format(self.living_db)
        cu.execute(living_sql)
        conn.commit()
        # useless data to retry
        useless_sql = 'create table if not exists {}(id int unique, content varchar(150), primary key(id)) ' \
                      'charset=utf8'.format(self.useless_db)
        cu.execute(useless_sql)
        conn.commit()
        # offline zero data to retry
        zero_sql = 'create table if not exists {}(id int unique, content varchar(150), primary key(id)) ' \
                   'charset=utf8'.format(self.zero_db)
        cu.execute(zero_sql)
        conn.commit()
        cu.close()
        conn.close()

    # return the one in spe_list but not in search_list
    @staticmethod
    def get_not_in_list(spe_list, search_list):
        not_in_list = []
        for one in spe_list:
            if one not in search_list:
                not_in_list.append(one)
        return not_in_list

    # return the one in spe_list and in search_list
    @staticmethod
    def get_in_list(spe_list, search_list):
        in_list = []
        for one in spe_list:
            if one in search_list:
                in_list.append(one)
        return in_list

    def request_get_more(self, url_dict, rst_dict):
        spider = NormalSpider()
        err_key = []
        for key, values in url_dict.items():
            try:
                data = spider.request_get(values)
                js = json.loads(data)
                rst_dict[key] = copy.deepcopy(js)
            except Exception as err:
                err_key.append(key)
                self._write_exception_log(
                    '\n{0} request_info_error url:{1}\n{2}'.format(datetime.now(), values, err)
                )
        return err_key

    def read_db_all(self, db_name, start_id, end_id):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        cu.execute(
            'SELECT * FROM {} WHERE id >= {} AND id <= {}'.format(db_name, start_id, end_id))
        data = cu.fetchall()
        cu.close()
        conn.close()
        return data

    def execute_db_more(self, sql_list):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        for sql in sql_list:
            cu.execute(sql)
            conn.commit()
        cu.close()
        conn.close()

    @staticmethod
    def join_num(raw_list):
        int_list = copy.deepcopy(raw_list)
        int_list = list(map(str, int_list))
        return '_'.join(int_list)

    def _write_exception_log(self, content):
        with open(os.path.join(
                self.log_path, self.error_log.format(datetime.now().strftime('%Y%m%d'))), 'a') as fh:
            fh.write(content)

    def _write_run_log(self, content):
        with open(os.path.join(self.log_path, self.run_log.format(datetime.now().strftime('%Y%m%d'))), 'a') as fh:
            fh.write(content)

    @staticmethod
    def join_list(one_list, other_list):
        rst_list = []
        for item in one_list:
            rst_list.append(item)
        for item in other_list:
            rst_list.append(item)
        return rst_list

    @staticmethod
    def add2item(add_list, add2item):
        for item in add_list:
            add2item.add(item)

    # sort para for one para threading_task
    @staticmethod
    def sort_thread_para_list(para_list, default_task_num=10):
        task_list = []
        list_len = len(para_list)
        if list_len <= default_task_num:
            for item in para_list:
                task_list.append([item, ])
        else:
            for i in range(default_task_num):
                task_list.append([])
            write_num = 0
            while write_num < list_len:
                for i in range(default_task_num):
                    task_list[i].append(para_list[write_num])
                    write_num += 1
                    if write_num == list_len:
                        break
        return task_list

    def run_spider(self):
        hot_list = self._get_hot_list()
        # self._run_spider(hot_list)
        try:
            self._run_spider(hot_list)
        except Exception as err:
            traceback.print_exc()
            # when err restart and write into run.log
            self._write_run_log('\n{0} restart'.format(datetime.now()))
            self.clear_program()
            self.clean()
            sleep(5)
            self.run_spider()

    def _get_hot_list(self):
        hot_list = []
        # read hot list
        hot_mark = self.read_db_all(self.hot_anchor_db, 1, 1)
        raw_list = self.read_db_all(self.hot_anchor_db, 2, int(hot_mark[0][1]))
        for room in raw_list:
            pat_list = room[1].split('_')
            for room_id in pat_list:
                hot_list.append(room_id)
        return list(map(int, hot_list))

    def _run_spider(self, hot_list):
        t1 = time.time()
        # get living_list include room_id,some of them maybe redundant
        room_list = self.get_living_room_id
        living_list = []
        for info in room_list:
            living_list.append(int(info[0]))
        # delete redundant data
        living_list = set(living_list)
        # anchor_list include room_id which will be spidered by selenium
        anchor_list = set()
        # consumer_task_list to save the room_id to get consumer rank
        consumer_task_list = set()
        # add hot_list into consumer_task to avoid kongshua
        self.add2item(hot_list, consumer_task_list)
        # first step: get the room_id run_error in last spider and when offline whose data having zero
        # add them into task_list
        useless_mark = self.read_db_all(self.useless_db, 1, 1)
        if useless_mark:
            mark_string = useless_mark[0][1]
            if mark_string:
                mark_list = mark_string.split('_')
                mark_date = datetime(
                    year=int(mark_list[1]),
                    month=int(mark_list[2]),
                    day=int(mark_list[3]),
                    hour=int(mark_list[4]),
                    minute=int(mark_list[5]),
                    second=int(mark_list[6])
                )
                if (datetime.now() - mark_date).seconds < 3600:
                    useless_list = self.read_db_all(self.useless_db, 2, int(mark_list[0]))
                    for info in useless_list:
                        part_info = info[1].split('_')
                        self.add2item(list(map(int, part_info)), anchor_list)
        zero_mark = self.read_db_all(self.zero_db, 1, 1)
        if zero_mark:
            mark_string = zero_mark[0][1]
            if mark_string:
                mark_list = mark_string.split('_')
                mark_date = datetime(
                    year=int(mark_list[1]),
                    month=int(mark_list[2]),
                    day=int(mark_list[3]),
                    hour=int(mark_list[4]),
                    minute=int(mark_list[5]),
                    second=int(mark_list[6])
                )
                if (datetime.now() - mark_date).seconds < 1800:
                    zero_list = self.read_db_all(self.zero_db, 2, int(mark_list[0]))
                    for info in zero_list:
                        part_info = info[1].split('_')
                        self.add2item(list(map(int, part_info)), anchor_list)
        # second step: get last_living_list to get online and offline list. if not exist, suspect the hot list to avoid
        # loss of data
        living_mark = self.read_db_all(self.living_db, 1, 1)
        last_living_list = []
        online_list = []
        offline_list = [[], []]
        if living_mark:
            mark_string = living_mark[0][1]
            if mark_string:
                mark_list = mark_string.split('_')
                # last_living lives in 1 hour
                mark_date = datetime(
                    year=int(mark_list[1]),
                    month=int(mark_list[2]),
                    day=int(mark_list[3]),
                    hour=int(mark_list[4]),
                    minute=int(mark_list[5]),
                    second=int(mark_list[6])
                )
        if living_mark and living_mark[0][1] and (datetime.now() - mark_date).seconds < 3600:
            old_living_list = self.read_db_all(self.living_db, 2, int(mark_list[0]))
            for info in old_living_list:
                living_part = info[1].split('_')
                for room_id in living_part:
                    last_living_list.append(int(room_id))
            # get offline list
            suspect_offline = self.get_not_in_list(last_living_list, living_list)
            offline_list = self.get_sort_suspect_list(suspect_offline)
            # add fake offline into living_list
            self.add2item(offline_list[1], living_list)
            # get online list
            suspect_online = self.get_not_in_list(living_list, last_living_list)
            online_list = self.get_sort_suspect_list(suspect_online)[1]
            # add offline and online to anchor_list
            self.add2item(offline_list[0], anchor_list)
            self.add2item(online_list, anchor_list)
            # third step: add hot and online anchor to anchor_list
            hot_online = self.get_in_list(hot_list, living_list)
            # add hot_online to anchor_list
            self.add2item(hot_online, anchor_list)
            # add offline 2 consumer_task_list
            self.add2item(offline_list[0], consumer_task_list)
        else:
            # get anchor_list who is not in living_list, get its isShowing
            suspect_list = self.get_not_in_list(hot_list, living_list)
            sorted_suspect_list = self.get_sort_suspect_list(suspect_list)[1]
            self.add2item(sorted_suspect_list, living_list)
            # add all into anchor_list
            self.add2item(living_list, anchor_list)
        # add living list into consumer task
        self.add2item(living_list, consumer_task_list)
        # change set into list
        living_list = list(living_list)
        anchor_list = list(anchor_list)
        consumer_task_list = list(consumer_task_list)
        # start spider
        no_data_room_list = self._anchor_task(anchor_list, offline_list[0])
        self._consumer_task(consumer_task_list)
        # update living list
        self.update_ten_mark_list(self.living_db, living_list)
        # update useless list
        if no_data_room_list[0]:
            self.update_ten_mark_list(self.useless_db, no_data_room_list[0])
        else:
            self._drop_rebuilt(self.useless_db)
        # update zero
        if no_data_room_list[1]:
            self.update_ten_mark_list(self.zero_db, no_data_room_list[1])
        else:
            self._drop_rebuilt(self.zero_db)
        # write mark
        self._write_mark(len(anchor_list))
        t2 = time.time()
        log = '\n{0} {1}'.format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), t2 - t1)
        self._write_run_log(log)
        if time.localtime(t2).tm_hour == 5 and time.localtime(t2).tm_min < 5:
            hot_list = self._get_hot_list()
        self._run_spider(hot_list)

    def _drop_rebuilt(self, db_name):
        sql_list = [
            'truncate table {};'.format(db_name),
        ]
        self.execute_db_more(sql_list)

    # thread to sort list renturn [not_showing_list, showing_list]
    def get_sort_suspect_list(self, suspect_list):
        return_list = [[], []]
        if suspect_list:
            # thread to finish task
            show_task_list = self.sort_thread_para_list(suspect_list)
            show_task_bank = []
            for task in show_task_list:
                show_task_bank.append(threading.Thread(
                    target=self.get_showing_status, args=(task, return_list, )
                ))
            for task in show_task_bank:
                task.start()
            for task in show_task_bank:
                task.join()
        return return_list

    # return [not_showing_list, showing_list]
    def get_showing_status(self, search_list, return_list=None):
        if return_list is None:
            return_list = [[], []]
        spider = NormalSpider()
        show_pat = re.compile(r'"isShowing": (.*?),')
        for room_id in search_list:
            data = spider.request_get(self.room_url.format(room_id))
            is_showing = show_pat.findall(data)[0]
            if is_showing == 'false':
                return_list[0].append(room_id)
            elif is_showing == 'true':
                return_list[1].append(room_id)
            else:
                self._write_exception_log(
                    '\n{0} except type of showing_status:{1} room:{2}'.format(datetime.now(), is_showing, room_id)
                )
        return return_list

    @staticmethod
    def clear_program():
        try:
            os.system("ps -ef | grep chrome | grep -v grep | awk '{print $2}' | xargs kill -9")
        except Exception:
            pass
        try:
            os.system("ps -ef | grep chromedriver | grep -v grep | awk '{print $2}' | xargs kill -9")
        except Exception:
            pass
        try:
            os.system("ps -ef | grep webdriver | grep -v grep | awk '{print $2}' | xargs kill -9")
        except Exception:
            pass

    def _write_mark(self, living_num):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        sql_inquire = 'SELECT id FROM {} ORDER BY id DESC LIMIT 1'.format(
            self.anchor_db)
        cu.execute(sql_inquire)
        data = cu.fetchall()
        sql_insert = "INSERT INTO {3}(mark,peo,time) VALUES ('{0}','{1}','{2}')".format(
            data[0][0], living_num, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.anchor_mark_db)
        cu.execute(sql_insert)
        conn.commit()
        get_last_sql = 'SELECT id FROM {} ORDER BY id DESC LIMIT 1'.format(
            self.consumer_db)
        cu.execute(get_last_sql)
        cur_data = cu.fetchall()
        if cur_data:
            mark_id = cur_data[0][0]
            mark_sql = "INSERT INTO {0}(mark, time) VALUES ('{1}', '{2}')"
            cu.execute(
                mark_sql.format(
                    self.consumer_mark_db,
                    mark_id,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
        cu.close()
        conn.close()

    def _consumer_task(self, consumer_task_list):
        consumer_info_dict = {}
        consumer_task = self.sort_thread_para_list(para_list=consumer_task_list, default_task_num=10)
        task_bank = []
        for task_info in consumer_task:
            task_bank.append(threading.Thread(
                target=self._consumer_task_medium, args=(task_info, consumer_info_dict, )
            ))
        for task in task_bank:
            task.start()
        for task in task_bank:
            task.join()
        # write into db
        consumer_w_bank = []
        for task_info in consumer_task:
            consumer_w_bank.append(threading.Thread(
                target=self._write_consumer_data, args=(task_info, consumer_info_dict, )
            ))
        for task in consumer_w_bank:
            task.start()
        for task in consumer_w_bank:
            task.join()

    def _write_consumer_data(self, room_list, consumer_info_dict):
        insert_sql = "INSERT INTO {0}(userid,username,xingbi,roomid,time) VALUES ('{1}','{2}','{3}','{4}','{5}')"
        sql_list = []
        for room_id in room_list:
            if consumer_info_dict.get(room_id):
                js = consumer_info_dict.get(room_id)
                data = js['response']['data']
                if data:
                    rank_list = []
                    for info in data:
                        user_id = info['userId']
                        user_name = info['nickName']
                        star_coin = info['coins']
                        sql_list.append(
                            insert_sql.format(
                                self.consumer_db,
                                user_id,
                                user_name,
                                star_coin,
                                room_id,
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                # Uncomment to observe the state of js
                # having observed for a short time, only when no consumer
                # else:
                #     self._write_exception_log(
                #         '\n{0} no_info_in_js {1}'.format(datetime.now(), room_id)
                #     )
            else:
                self._write_exception_log(
                    '\n{0} no_js_in_dict {1}'.format(datetime.now(), room_id)
                )
        self.execute_db_more(sql_list)

    def _consumer_task_medium(self, room_part, consumer_info_dict):
        url_dict = {}
        for room_id in room_part:
            url_dict[room_id] = self.current_rank_url.format(room_id)
        self.request_get_more(url_dict, consumer_info_dict)

    def _anchor_task(self, anchor_list, offline_list):
        anchor_info_dict = {}
        # anchor spider task
        anchor_task = self.sort_thread_para_list(para_list=anchor_list, default_task_num=5)
        anchor_task_bank = []
        for task_info in anchor_task:
            anchor_task_bank.append(
                threading.Thread(target=self._get_anchor_info, args=(task_info, anchor_info_dict,))
            )
        for task in anchor_task_bank:
            task.start()
        for task in anchor_task_bank:
            task.join()
        # write into database
        # find no data when write add room id into no_data_room_list
        no_data_room_list = [[], []]
        anchor_task = self.sort_thread_para_list(para_list=anchor_list, default_task_num=10)
        anchor_task_bank = []
        for task_info in anchor_task:
            anchor_task_bank.append(
                threading.Thread(
                    target=self._write_anchor_data, args=(task_info, anchor_info_dict, no_data_room_list, offline_list)
                )
            )
        for task in anchor_task_bank:
            task.start()
        for task in anchor_task_bank:
            task.join()
        self.clear_program()
        return no_data_room_list

    def _write_anchor_data(self, room_list, anchor_info_dict, no_data_room_list, offline_list):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        for room_info in room_list:
            if anchor_info_dict.get(room_info):
                data = anchor_info_dict[room_info]
                try:
                    if '/' in data:
                        no_data_room_list[0].append(room_info)
                        self._write_exception_log(
                            '\n{0} read_/_err:{1}'.format(datetime.now(), room_info)
                        )
                        sql_command = "INSERT INTO {6}(roomid,peo,xingbi,renqi,online,time,isdelete) VALUES ('{0}'," \
                                      "'{1}','{2}','{3}','{4}','{5}',True)".format(
                                            room_info, data[0].replace("'", ''), data[1], data[2], data[3],
                                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.anchor_db
                                                                )
                    else:
                        if '0' in data and room_info in offline_list:
                            no_data_room_list[1].append(room_info)
                            self._write_exception_log(
                                '\n{0} read_zero_data:{1}'.format(datetime.now(), room_info)
                            )
                        sql_command = "INSERT INTO {6}(roomid,peo,xingbi,renqi,online,time) VALUES ('{0}','{1}'," \
                                      "'{2}','{3}','{4}','{5}')".format(
                                            room_info, data[0].replace("'", ''), data[1], data[2], data[3],
                                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.anchor_db
                                                                )
                    cu.execute(sql_command)
                    conn.commit()
                except Exception as err:
                    data_err = ' '.join([str(da) for da in data])
                    self._write_exception_log(
                        '\n{0} write_sql_err:{1}\nmgs:{2}\ndata:{3}'.format(datetime.now(), err, room_info, data_err)
                    )
            else:
                no_data_room_list[0].append(room_info)
                self._write_exception_log(
                    '\n{0} no_read_data:{1}'.format(datetime.now(), room_info)
                )
        cu.close()
        conn.close()

    def _get_anchor_info(self, room_list, anchor_info_dict):
        warnings.filterwarnings('ignore')
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('blink-settings=imagesEnabled=false')
        client = webdriver.Chrome(chrome_options=options)
        for room in room_list:
            client.get(self.room_url.format(room))
            sleep(4)
            try:
                WebDriverWait(client, 4).until_not(
                    self.LoadSuccess()
                )
                load_status = True
            except TimeoutException as err:
                load_status = False
            content = client.page_source
            if load_status:
                try:
                    anchor_info_dict[room] = self.v_xpath_read(content)
                except Exception as err:
                    self._write_exception_log(
                        '\n{0} no_v_xpath:{1} err:{2}'.format(datetime.now(), room, err)
                    )
                    anchor_info_dict[room] = self.h_xpath_read(content)
            else:
                try:
                    anchor_info_dict[room] = self.h_xpath_read(content)
                except Exception:
                    try:
                        anchor_info_dict[room] = self.v_xpath_read(content)
                    except Exception as err:
                        self._write_exception_log(
                            '\n{0} second_v_xpath_err:{1} err:{2}'.format(datetime.now(), room, err)
                        )
        client.quit()

    @staticmethod
    def h_xpath_read(source_page):
        html = etree.HTML(source_page)
        pat = re.compile(r'^.*?：(.*?)$')
        star_coin = html.xpath(
            '/html/body/div/div/div[3]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[2]/'
            'span[@class="stars_1YLot"]/text()'
        )[0]
        star_coin = pat.findall(star_coin)[0]
        popular = html.xpath(
            '/html/body/div/div/div[3]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[2]/'
            'span[@class="room-popular_bUt9g"]/text()'
        )[0]
        popular = pat.findall(popular)[0]
        online = html.xpath(
            '/html/body/div/div/div[3]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[2]/'
            'span[@class="online-people_rLgAG"]/text()'
        )[0]
        online = pat.findall(online)[0]
        name = html.xpath(
            '/html/body/div/div/div[3]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/text()'
        )[0]
        return [name, star_coin, popular, online]

    @staticmethod
    def v_xpath_read(source_page):
        html = etree.HTML(source_page)
        star_coin = html.xpath(
            '//*[@id="LF-info-count"]/div/div[@class="gift"]/div[@class="info"]/cite/text()'
        )[0]
        popular = html.xpath(
            '//*[@id="LF-info-count"]/div/div[@class="rq"]/div[@class="info"]/cite/text()'
        )[0]
        online = html.xpath(
            '//*[@id="LF-info-count"]/div/div[@class="online"]/div[@class="info"]/cite/text()'
        )[0]
        name = html.xpath(
            '/html/body/div/div[2]/div[2]/div[2]/dl/dd/span/text()'
        )[0]
        return [name, star_coin, popular, online]

    @staticmethod
    def h_type_xpath(client):
        pat = re.compile(r'^.*?：(.*?)$')
        star_coin = client.find_element_by_xpath(
            '/html/body/div/div/div[3]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[2]/'
            'span[@class="stars_1YLot"]').text
        star_coin = pat.findall(star_coin)[0]
        popular = client.find_element_by_xpath(
            '/html/body/div/div/div[3]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[2]/'
            'span[@class="room-popular_bUt9g"]').text
        popular = pat.findall(popular)[0]
        online = client.find_element_by_xpath(
            '/html/body/div/div/div[3]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[2]/'
            'span[@class="online-people_rLgAG"]').text
        online = pat.findall(online)[0]
        name = client.find_element_by_xpath(
            '/html/body/div/div/div[3]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]'
        ).text
        return [name, star_coin, popular, online]

    @staticmethod
    def v_type_xpath(client):
        star_coin = client.find_element_by_xpath(
            '//*[@id="LF-info-count"]/div/div[@class="gift"]/div[@class="info"]/cite'
        ).text
        popular = client.find_element_by_xpath(
            '//*[@id="LF-info-count"]/div/div[@class="rq"]/div[@class="info"]/cite'
        ).text
        online = client.find_element_by_xpath(
            '//*[@id="LF-info-count"]/div/div[@class="online"]/div[@class="info"]/cite'
        ).text
        name = client.find_element_by_xpath(
            '/html/body/div/div[2]/div[2]/div[2]/dl/dd/span'
        ).text
        return [name, star_coin, popular, online]

    class LoadSuccess:
        # h_type return True, v_type return according to element.text
        def __call__(self, client):
            try:
                # start = bool(EC.text_to_be_present_in_element((
                #     By.XPATH, '//*[@id="LF-info-count"]/div/div[@class="gift"]/div[@class="info"]/cite'), u'/'
                # )(client))
                # popular = bool(EC.text_to_be_present_in_element((
                #     By.XPATH, '//*[@id="LF-info-count"]/div/div[@class="rq"]/div[@class="info"]/cite'), u'/'
                # )(client))
                # online = bool(EC.text_to_be_present_in_element((
                #     By.XPATH, '//*[@id="LF-info-count"]/div/div[@class="online"]/div[@class="info"]/cite'), u'/'
                # )(client))
                star_coin = client.find_element_by_xpath(
                    '//*[@id="LF-info-count"]/div/div[@class="gift"]/div[@class="info"]/cite'
                ).text
                popular = client.find_element_by_xpath(
                    '//*[@id="LF-info-count"]/div/div[@class="rq"]/div[@class="info"]/cite'
                ).text
                online = client.find_element_by_xpath(
                    '//*[@id="LF-info-count"]/div/div[@class="online"]/div[@class="info"]/cite'
                ).text
                return ('/' in [star_coin, popular, online]
                        ) or ('0' in [star_coin, popular, online])
            except NoSuchElementException:
                return True

    def update_ten_mark_list(self, db_name, data_list):
        data_num = len(data_list)
        sql_list = []
        group_num = math.ceil(data_num / 10)
        # mark sql
        mark_content = '{0}_{1}'.format(
            group_num + 1, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        )
        sql_list.append(
            r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE content='{2}'".format(
                db_name, 1, mark_content))
        # content sql
        for i in range(group_num - 1):
            sql_list.append(r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE "
                            r"content='{2}'".format(
                                db_name, i + 2, self.join_num(data_list[i * 10:(i + 1) * 10])
                                                    ))
        # the last sql
        sql_list.append(r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE content='{2}'".format(
            db_name, group_num + 1, self.join_num(data_list[(group_num - 1) * 10:])))
        task_list = self.sort_thread_para_list(sql_list)
        task_bank = []
        for task_info in task_list:
            task_bank.append(threading.Thread(
                target=self.execute_db_more, args=(task_info, )
            ))
        for task in task_bank:
            task.start()
        for task in task_bank:
            task.join()

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
            thread_task = threading.Thread(
                target=self.request_get_more, args=(
                    url_list[i], js_dict,))
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

    def clean(self):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        # anchor check
        cu.execute(
            'SELECT mark FROM {} ORDER BY id DESC LIMIT 1'.format(
                self.anchor_mark_db))
        try:
            end = cu.fetchall()[0][0]
        except Exception:
            end = 0
        cu.execute(
            'UPDATE {0} SET isdelete=true WHERE id>{1}'.format(
                self.anchor_db, end))
        conn.commit()
        # rank check
        cu.execute(
            'SELECT mark FROM {} ORDER BY id DESC LIMIT 1'.format(
                self.consumer_mark_db))
        try:
            end = cu.fetchall()[0][0]
        except Exception:
            end = 0
        cu.execute(
            'UPDATE {0} SET isdelete=true WHERE id>{1}'.format(
                self.consumer_db, end))
        conn.commit()
        cu.close()
        conn.close()


if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    lai = LaiThread()
    lai.anchor_db = 'anchor'
    lai.anchor_mark_db = 'anchormark'
    lai.consumer_mark_db = 'consumermark'
    lai.consumer_db = 'consumerrank'
    lai.living_db = 'living'
    lai.useless_db = 'useless'
    lai.zero_db = 'zero'
    lai.create_database()
    lai.clean()
    lai.run_spider()



