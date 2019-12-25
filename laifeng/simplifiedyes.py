# -*- coding:utf-8 -*-
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
            'db': 'laifeng'}  # Test_1357642

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
        living_sql = 'create table if not exists {}(id int unique, content varchar(100), primary key(id)) ' \
                     'charset=utf8'.format(self.living_db)
        cu.execute(living_sql)
        conn.commit()
        cu.close()
        conn.close()

    @staticmethod
    def request_get_more(url_dict, rst_dict):
        spider = NormalSpider()
        for key, values in url_dict.items():
            data = spider.request_get(values)
            js = json.loads(data)
            rst_dict[key] = copy.deepcopy(js)
        return None

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
        for i in range(len(int_list)):
            int_list[i] = str(int_list[i])
        return '_'.join(int_list)

    def run_spider(self):
        try:
            self._run_spider()
        except Exception as err:
            # when err restart and write into run.log
            with open(os.path.join(self.log_path, self.run_log.format(datetime.now().strftime('%Y%m%d'))), 'a') as fh:
                fh.write('\n{0} restart'.format(datetime.now()))
            os.system('pkill -9 -u root chrome')
            self.clean()
            sleep(5)
            self.run_spider()

    def _run_spider(self):
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
            mark_date = datetime(
                year=int(
                    mark_list[1]), month=int(
                    mark_list[2]), day=int(
                    mark_list[3]), hour=int(
                    mark_list[4]), minute=int(
                        mark_list[5]), second=int(
                            mark_list[6]))
            now_time = datetime.now()
            if (now_time - mark_date).seconds < 3600:
                old_living_list = self.read_db_all(
                    self.living_db, 2, int(mark_list[0]))
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
        log = '{0} {1}'.format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), t2 - t1)
        with open(os.path.join(self.log_path, self.run_log.format(datetime.now().strftime('%Y%m%d'))), 'a') as fh:
            fh.write('\n{0}'.format(log))
        os.system('pkill -9 -u root chrome')
        self._run_spider()

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
        get_last_sql = 'SELECT id FROM {} ORDER BY time DESC LIMIT 1'.format(
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

    def _offline_task(self, room_list):
        offline_info_dict = {}
        # offline request task
        offline_task_num = math.ceil(len(room_list) / 10)
        offline_task_list = []
        for i in range(1, 10):
            offline_task_list.append(
                room_list[offline_task_num * (i - 1):offline_task_num * i])
        offline_task_list.append(room_list[offline_task_num * 9:])
        offline_bank = []
        for i in range(10):
            offline_th = threading.Thread(
                target=self._off_task_medium, args=(
                    offline_task_list[i], offline_info_dict,))
            offline_bank.append(offline_th)
        for i in range(10):
            offline_bank[i].start()
        for i in range(10):
            offline_bank[i].join()
        # write into db
        offline_w_bank = []
        for i in range(10):
            offline_th = threading.Thread(
                target=self._write_offline_data, args=(
                    offline_task_list[i], offline_info_dict, ))
            offline_w_bank.append(offline_th)
        for i in range(10):
            offline_w_bank[i].start()
        for i in range(10):
            offline_w_bank[i].join()

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
                    sql_list.append(
                        insert_sql.format(
                            self.consumer_db,
                            user_id,
                            user_name,
                            star_coin,
                            room_id,
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        self.execute_db_more(sql_list)

    def _off_task_medium(self, room_part, offline_info_dict):
        url_dict = {}
        for room_id in room_part:
            url_dict[room_id] = self.current_rank_url.format(room_id)
        self.request_get_more(url_dict, offline_info_dict)

    def _online_task(self, room_list):
        online_info_dict = {}
        # online selenium task
        online_task_num = math.ceil(len(room_list) / 10)
        online_task_list = []
        for i in range(1, 2):
            online_task_list.append(
                room_list[online_task_num * (i - 1):online_task_num * i])
        online_task_list.append(room_list[online_task_num * 1:])
        online_bank = []
        for i in range(2):
            online_th = threading.Thread(
                target=self._get_online_info, args=(
                    online_task_list[i], online_info_dict,))
            online_bank.append(online_th)
        for i in range(2):
            online_bank[i].start()
        for i in range(2):
            online_bank[i].join()
        # write into database
        online_task_num = math.ceil(len(room_list) / 10)
        online_task_list = []
        for i in range(1, 10):
            online_task_list.append(
                room_list[online_task_num * (i - 1):online_task_num * i])
        online_task_list.append(room_list[online_task_num * 9:])
        online_w_bank = []
        for i in range(10):
            online_th = threading.Thread(
                target=self._write_online_data, args=(
                    online_task_list[i], online_info_dict,))
            online_w_bank.append(online_th)
        for i in range(10):
            online_w_bank[i].start()
        for i in range(10):
            online_w_bank[i].join()
        os.system('pkill -9 -u root chrome')

    def _write_online_data(self, room_list, online_info_dict):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        for room_info in room_list:
            if online_info_dict.get(room_info[0]):
                data = online_info_dict[room_info[0]]
                try:
                    sql_command = "INSERT INTO {6}(roomid,peo,xingbi,renqi,online,time) VALUES ('{0}','{1}','{2}'," \
                                  "'{3}','{4}','{5}')".format(
                                        room_info[0], room_info[1].replace("'", ''), data[0], data[1], data[2],
                                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.anchor_db
                                                            )
                    cu.execute(sql_command)
                    conn.commit()
                except Exception as err:
                    msg_err = ' '.join([str(m) for m in room_info])
                    data_err = ' '.join([str(da) for da in data])
                    with open(os.path.join(
                            self.log_path, self.error_log.format(datetime.now().strftime('%Y%m%d'))), 'a') as fh:
                        fh.write(
                            '\n{0} write_sql_err:{1}\nmgs:{2}\ndata:{3}'.format(
                                datetime.now(), err, msg_err, data_err))
            else:
                with open(os.path.join(
                        self.log_path, self.error_log.format(datetime.now().strftime('%Y%m%d'))), 'a') as fh:
                    fh.write(
                        '\n{0} no_rank:{1}'.format(
                            datetime.now(), room_info[0]))
        cu.close()
        conn.close()

    def _get_online_info(self, room_list, online_info_dict):
        warnings.filterwarnings('ignore')
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('blink-settings=imagesEnabled=false')
        client = webdriver.Chrome(chrome_options=options)
        for room in room_list:
            client.get(self.room_url.format(room[0]))
            sleep(3)
            try:
                WebDriverWait(client, 3).until_not(
                    self.LoadSuccess()
                )
                load_status = True
            except TimeoutException as err:
                load_status = False
            if load_status:
                try:
                    online_info_dict[room[0]] = self.v_type_xpath(client)
                except Exception as err:
                    with open(os.path.join(
                            self.log_path, self.error_log.format(datetime.now().strftime('%Y%m%d'))), 'a') as fh:
                        fh.write(
                            '\n{0} no_v_xpath:{1} err:{2}'.format(
                                datetime.now(), room[0], err))
                    online_info_dict[room[0]] = self.h_type_xpath(client)
            else:
                try:
                    online_info_dict[room[0]] = self.h_type_xpath(client)
                except Exception as err:
                    online_info_dict[room[0]] = self.v_type_xpath(client)
        client.quit()

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
        return [star_coin, popular, online]

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
        return [star_coin, popular, online]

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

    def update_living_list(self, living_list):
        living_num = len(living_list)
        sql_list = []
        group_num = math.ceil(living_num / 10)
        # mark sql
        mark_content = '{0}_{1}'.format(
            group_num + 1, datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
        sql_list.append(
            r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE content='{2}'".format(
                self.living_db, 1, mark_content))
        # content sql
        for i in range(group_num - 1):
            sql_list.append(r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE "
                            r"content='{2}'".format(
                                self.living_db, i + 2, self.join_num(living_list[i * 10:(i + 1) * 10])
                                                    ))
        # the last sql
        sql_list.append(r"INSERT INTO {0}(id, content) VALUES({1}, '{2}') ON DUPLICATE KEY UPDATE content='{2}'".format(
            self.living_db, group_num + 1, self.join_num(living_list[(group_num - 1) * 10:])))
        task_num = math.ceil(group_num / 10)
        t_bank = []
        for i in range(9):
            th = threading.Thread(target=self.execute_db_more, args=(
                sql_list[i * task_num:(i + 1) * task_num], ))
            t_bank.append(th)
        th = threading.Thread(target=self.execute_db_more,
                              args=(sql_list[9 * task_num:], ))
        t_bank.append(th)
        for i in range(10):
            t_bank[i].start()
        for i in range(10):
            t_bank[i].join()

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
    lai.create_database()
    lai.clean()
    lai.run_spider()



