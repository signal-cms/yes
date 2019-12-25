# -*- coding:utf-8 -*-
import copy
import traceback
import requests
from datetime import datetime, timedelta
import time
import os
import xlwt
from selenium import webdriver
import json
import math
import warnings
import re
import pymysql
from time import sleep
from multiprocessing import Pool


def get_attribute(default, set_value):
    if set_value:
        return set_value
    else:
        return default


class LaiFeng:

    # the attributes used are defined in __init__, it's convenience to manage
    def __init__(self, log_path=None, list_url=None, room_url=None, error_log=None, run_log=None, database_dict=None,
                 current_rank_url=None):
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
        default_database_dict = {'host': 'localhost', 'port': 3306, 'user': 'tester',
                                 'password': 'test123', 'db': 'laifeng'}
        new_get = lambda k: database_dict.get(k, default_database_dict[k])
        if isinstance(database_dict, dict):
            self.database_dict = {'host': new_get('host'), 'port': new_get('port'), 'user': new_get('user'),
                                  'password': new_get('password'), 'db': new_get('db')}
        elif not database_dict:
            self.database_dict = default_database_dict
        else:
            raise TypeError('database_dict is not dict')
        warnings.filterwarnings('ignore')
        self.detail_db = 'detail'
        self.current_db = 'currentrank'
        self.current_mark_db = 'currentmark'
        self.anchor_mark_db = 'anchormark'
        self.report_log = 'report.log'
        self.room_buffer = []

    def create_database(self):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        # the sql to create the detail database
        detail_sql = 'create table if not exists {}(id int auto_increment, roomid varchar(20), ' \
                     'peo varchar(50), xingbi varchar(20), renqi varchar(20), online varchar(20), time datetime, ' \
                     'isdelete boolean default false,primary key(id)) charset=utf8'.format(self.detail_db)
        cu.execute(detail_sql)
        conn.commit()
        # the sql to create the currentrank database
        current_sql = 'create table if not exists {}(id int auto_increment, userid varchar(20), ' \
                      'username varchar(50), xingbi varchar(20), roomid varchar(20), time datetime, ' \
                      'isdelete boolean default false,primary key(id)) charset=utf8'.format(self.current_db)
        cu.execute(current_sql)
        conn.commit()
        # the sql to create anchormark
        anchor_mark_sql = 'create table if not exists {}(id int auto_increment, ' \
                          'mark varchar(50), peo varchar(20), time datetime, ' \
                          'primary key(id)) charset=utf8'.format(self.anchor_mark_db)
        cu.execute(anchor_mark_sql)
        conn.commit()
        # the sql to create rankmark
        current_mark_sql = 'create table if not exists {}(id int auto_increment, mark varchar(50), time datetime, ' \
                           'primary key(id)) charset=utf8'.format(self.current_mark_db)
        cu.execute(current_mark_sql)
        conn.commit()
        cu.close()
        conn.close()

    # to get the room id whose living status=1
    @property
    def _get_valid_room(self):
        rst = []
        data = self._get_response(self.list_url.format(1))
        js = json.loads(data)
        ttl_page = js['response']['data']['data']["totalPages"]
        time_list = [time.time(), ]
        # set status_cnt avoid one person down in living page, when cnt > 20, the page will be judged as unliving
        status_cnt = 0
        for p in range(1, ttl_page + 1):
            data = self._get_response(self.list_url.format(p))
            js = json.loads(data)
            time_list.append(time.time())
            for dtl in js['response']['data']['data']['items']:
                room_id = dtl['roomId']
                peo_name = dtl['nickName']
                is_living = dtl['liveStatus']
                # avoid room id error
                if not isinstance(room_id, int):
                    with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                        fh.write('\npage:{0},roomid:{1},peo:{2},living_status:{3},time:{4},js_error:{5}'.format(
                            p, room_id, peo_name, is_living, datetime.now(), data
                        ))
                # status_cnt record the num of continued unling people
                if is_living == 2:
                    status_cnt += 1
                    if status_cnt > 20:
                        return rst
                elif is_living == 1:
                    status_cnt = 0
                    rst.append([room_id, peo_name, is_living])
                else:
                    with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                        fh.write(
                            '\nget living room exception,page:{0},roomid:{1},peo:{2},living_status:{3},time:{4}'.format(
                                p, room_id, peo_name, is_living, datetime.now()
                            ))

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

    def _get_response(self, url):
        # set adapters for more times to retry when get nothing
        requests.adapters.DEFAULT_RETRIES = 10
        s = requests.session()
        # set keep_alive for clean session
        s.keep_alive = False
        response = requests.get(url, headers=self._get_header, verify=False)
        response.encoding = 'utf-8'
        return response.text

    def _get_msg(self, room_list=None, extra_len=0):
        if extra_len > 0:
            allow_zero = 2
            max_task_time = len(room_list) - extra_len
        else:
            allow_zero = 3
            max_task_time = len(room_list)
        if not room_list:
            room_list = self._get_valid_room
        # for firefox
        # if browser == 'FireFox':
        #     profile = webdriver.FirefoxOptions()
        #     profile.add_argument('-headless')
        #     client = webdriver.Firefox()
        # else:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        client = webdriver.Chrome(chrome_options=options)
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        run_time = 0
        for msg in room_list:
            run_time += 1
            try_time = 0
            client.get(self.room_url.format(msg[0]))
            sleep(1)
            while try_time < 11:
                sleep(0.5)
                content = client.page_source
                # get api from checkinfo
                if 8 <= try_time < 10:
                    data = self._get_num(content, allow_zero)
                elif try_time == 10:
                    data = self._get_num(content, 0)
                else:
                    data = self._get_num(content, 3)
                if data is not None:
                    try:
                        sql_command = "INSERT INTO {6}(roomid,peo,xingbi,renqi,online,time) VALUES ('{0}','{1}'," \
                                      "'{2}','{3}','{4}','{5}')".format(msg[0], msg[1].replace("'", ''), data[0],
                                                                        data[1], data[2],
                                                                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                                        self.detail_db)
                        cu.execute(sql_command)
                        conn.commit()
                    except Exception as err:
                        msg_err = ' '.join([str(m) for m in msg])
                        data_err = ' '.join([str(da) for da in data])
                        with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                            fh.write('\n{0} write_sql_err:{1}\nmgs:{2}\ndata:{3}'.format(datetime.now(), err,
                                                                                         msg_err, data_err))
                        with open(os.path.join(self.log_path, '{}.log'.format(datetime.now().strftime('%Y%m%d%H%M%S'))),
                                  'a') as fl:
                            fl.write(content)
                        raise
                    # cancel spe_bank
                    #     if allow_zero == 2 and int(data[2]) > 300:
                    #         for spe1 in self.spe_bank:
                    #             if spe1[0] == msg[0]:
                    #                 break
                    #         else:
                    #             spe_bank.append(msg)
                    # except Exception as err:
                    #     with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                    #         fh.write('\n{0} date[2]_err:{1}'.format(datetime.now(), err))
                    #         print('error_spe:', msg)
                    #         spe_bank.append(msg)
                    finally:
                        if run_time > max_task_time:
                            self._into_off_line(msg)
                        break
                else:
                    try_time += 1
        client.quit()
        cu.close()
        conn.close()

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

    # get current_rank ,final star_corn and popular when the status of anchor get into 2
    def _into_off_line(self, room_info):
        # self._get_msg(room_list=room_list, allow_zero=2)
        # filter bank
        filter_list = []
        # if self.spe_bank:
        #     for item1 in room_list:
        #         for item2 in self.spe_bank:
        #             if item1[0] == item2[0]:
        #                 break
        #         else:
        #             filter_list.append(item1)
        #     rank_list = self._get_current_rank(room_list=filter_list)
        # else:
        rank_list = self._get_current_rank(room_info=room_info)
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        insert_sql = "INSERT INTO {0}(userid,username,xingbi,roomid,time) VALUES ('{1}','{2}','{3}','{4}','{5}')"
        if not rank_list:
            with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                fh.write('\n{0} not_get_current_rank_js:{1}'.format(datetime.now(), room_info))
        for info in rank_list:
            try:
                filled_sql = insert_sql.format(self.current_db, info[0], info[1].replace("'", ''), info[2],
                                               info[3], info[4])
            except Exception as err:
                filled_sql = ''
                with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                    fh.write('\n{0} get_data_err_in_into_off_line:{1}'.format(datetime.now(), err))
            try:
                cu.execute(filled_sql)
                conn.commit()
            except Exception as err:
                with open(os.path.join(self.log_path, self.error_log), 'a') as fh:
                    fh.write('\n{0} write_sql_err_in_into_off_line:{1}\n{2}'.format(datetime.now(), err,
                                                                                    filled_sql))
        cu.close()
        conn.close()

    def _get_current_rank(self, room_info):
        # three times for get info
        try_time = 1
        # print(datetime.now(), room_info[0])
        raw_data = self._get_response(self.current_rank_url.format(room_info[0]))
        js = json.loads(raw_data)
        while js['response']['msg'] != '成功':
            raw_data = self._get_response(self.current_rank_url.format(room_info[0]))
            js = json.loads(raw_data)
            try_time += 1
            if try_time > 3:
                break
        data = js['response']['data']
        if data:
            rank_list = []
            for info in data:
                rank_list.append([info['userId'], info['nickName'], info['coins'], room_info[0],
                                  datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            return rank_list
        else:
            return []

    def run_all(self, pool_num=1):
        try:
            self._run_all(pool_num)
        except Exception as err:
            print('restart')
            with open(os.path.join(self.log_path, self.run_log), 'a') as fh:
                fh.write('\n{0} restart err: {1}'.format(datetime.now(), err))
            traceback.print_exc()
            self.clean()
            os.system('pkill -9 -u signal chrome')
            sleep(5)
            self.run_all(pool_num)

    def _run_all(self, pool_num=1):
        ts = time.time()
        living_list = self._get_valid_room
        # the extra task
        off_list = []
        if self.room_buffer:
            for buf in self.room_buffer:
                for living_info in living_list:
                    if buf[0] == living_info[0]:
                        break
                else:
                    off_list.append(buf)
        # save into room buffer
        self.room_buffer = living_list
        task_list = []
        if math.ceil(len(living_list) / 5) >= len(off_list):
            for extra_room in off_list:
                living_list.append(extra_room)
            task_num = math.ceil(len(living_list) / 6)
            # divide task by cpu
            for i in range(1, 6):
                task_list.append(living_list[task_num * (i - 1):task_num * i])
            task_list.append(living_list[task_num * 5:])
            p = Pool(pool_num)
            for i in range(5):
                p.apply_async(self._get_msg, args=(task_list[i], 0))
            # the extra task
            p.apply_async(self._get_msg, args=(task_list[5], len(off_list)))
        else:
            task_num = math.ceil(len(living_list) / 5)
            for i in range(1, 5):
                task_list.append(living_list[task_num * (i - 1):task_num * i])
            task_list.append(living_list[task_num * 4:])
            p = Pool(pool_num)
            for i in range(5):
                p.apply_async(self._get_msg, args=(task_list[i], 0))
            p.apply_async(self._get_msg, args=(off_list, len(off_list)))
        p.close()
        p.join()
        te = time.time()
        log = '{0} {1}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), te - ts)
        with open(os.path.join(self.log_path, self.run_log), 'a') as fh:
            fh.write('\n{0}'.format(log))
        print(log)
        # write datamark for report
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        sql_inquire = 'SELECT id FROM {} ORDER BY id DESC LIMIT 1'.format(self.detail_db)
        cu.execute(sql_inquire)
        data = cu.fetchall()
        sql_insert = "INSERT INTO {3}(mark,peo,time) VALUES ('{0}','{1}','{2}')".format(
            data[0][0], len(living_list), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.anchor_mark_db)
        cu.execute(sql_insert)
        conn.commit()
        get_last_sql = 'SELECT id FROM {} ORDER BY time DESC LIMIT 1'.format(self.current_db)
        cu.execute(get_last_sql)
        cur_data = cu.fetchall()
        if cur_data:
            mark_id = data[0][0]
            mark_sql = "INSERT INTO {0}(mark, time) VALUES ('{1}', '{2}')"
            cu.execute(mark_sql.format(self.current_mark_db, mark_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
        cu.close()
        conn.close()
        # kill chrome avoid too many program is running
        os.system('pkill -9 -u signal chrome')
        # cycle
        self._run_all(pool_num)

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
        cu.execute('UPDATE {0} SET isdelete=true WHERE id>{1}'.format(self.detail_db, end))
        conn.commit()
        # rank check
        cu.execute('SELECT mark FROM {} ORDER BY id DESC LIMIT 1'.format(self.current_mark_db))
        try:
            end = cu.fetchall()[0][0]
        except Exception:
            end = 0
        cu.execute('UPDATE {0} SET isdelete=true WHERE id>{1}'.format(self.current_db, end))
        conn.commit()
        cu.close()
        conn.close()

    def reporter(self):
        # get start time for report
        today = datetime.now()
        # init by zero
        today = datetime(year=today.year, month=today.month, day=today.day, hour=0, minute=10, second=0)
        one_day = today - timedelta(days=1)
        seven_day = today - timedelta(days=7)
        thirty_day = today - timedelta(days=30)
        day_list = [one_day, seven_day, thirty_day]
        # day_list = [one_day, ]
        # data bank for the data we will read, data[0] is detail rank, data[1] is current rank
        data_bank = [[], [], ]
        # reader
        for day_info in day_list:
            # detail rank
            id_list_peo = self._get_id_list(day_info, today, self.anchor_mark_db)
            rank_yield = self._detail_rank_by_id(id_list_peo[0], id_list_peo[1])
            for rank in rank_yield:
                data_bank[0].append(rank)
            # current rank
            id_list_cur = self._get_id_list(day_info, today, self.current_mark_db)
            data_bank[1].append(
                self._current_rank_by_id(id_list_cur[0], id_list_cur[1], id_list_peo[0], id_list_peo[1]))
        # writer
        sheet_name_list = [['主播星币日榜', '主播人气日榜', '主播在线人数日榜', '主播星币周榜', '主播人气周榜', '主播在线人数周榜',
                            '主播星币月榜', '主播人气月榜', '主播在线人数月榜'],
                           ['看客星币日榜', '看客星币周榜', '看客星币月榜']]
        wb_name = '{}.xls'.format(one_day.strftime('%Y%m%d'))
        wb = xlwt.Workbook(encoding='utf-8')
        for i in range(9):
            sh = wb.add_sheet(sheet_name_list[0][i])
            self._write_sheet(sh, data_bank[0][i])
        for i in range(3):
            sh = wb.add_sheet(sheet_name_list[1][i])
            self._write_sheet(sh, data_bank[1][i])
        wb.save(os.path.join('/mnt/hgfs/share', wb_name))

    # only for list or generator
    @staticmethod
    def _write_sheet(sheet_obj, data):
        for i in range(len(data)):
            for j in range(len(data[i])):
                sheet_obj.write(i, j, data[i][j])

    def _detail_rank_by_id(self, start, end):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        cu.execute('SELECT * FROM {2} WHERE isdelete=false AND id>={0} AND id<={1} '
                   'ORDER BY time'.format(start, end, self.detail_db))
        data = cu.fetchall()
        cu.close()
        conn.close()
        peo_list = [[data[0][1], data[0][2]], ]
        # sorted by room id
        detail_dict = {data[0][1]: [], }
        for dtl in data:
            for info in peo_list:
                if dtl[1] == info[0]:
                    detail_dict[dtl[1]].append([dtl[3], dtl[4], dtl[5], dtl[6]])
                    break
            else:
                peo_list.append([dtl[1], dtl[2]])
                detail_dict[dtl[1]] = [[dtl[3], dtl[4], dtl[5], dtl[6]], ]
        # bill is the calculated data
        bill = []
        for room in peo_list:
            # bill = [[xingbi(ttl), renqi(max), online(max), roomid, peoname], ]
            bill.append(self._detail_cleaner(detail_dict[room[0]], room))
        # rank is similar to bill, but by order
        title = ['星币', '最高人气', '最大在线数']
        for j in range(3):
            rank_list = self._bill_rank(bill, j)
            top30 = []
            for i in range(31):
                # print('排名：{0} 房间号：{1} 主播名：{2} 星币：{3}'.format(i+1, rank_list[i][3], rank_list[i][4], rank_list[i][0]))
                if i == 0:
                    top30.append(['排名', '房间号', '主播名', title[j]])
                else:
                    top30.append([i, rank_list[i-1][3], rank_list[i-1][4], rank_list[i-1][j]])
            yield top30

    @staticmethod
    def _detail_cleaner(clean_list, peo_info):
        pattern = []
        # divide pattern and cal data
        bank = [0, 0, 0]
        get_time = clean_list[0][3]
        coin_num = int(clean_list[0][0])
        len_clean_list = len(clean_list)
        for list_num in range(len_clean_list):
            item = clean_list[list_num]
            if (item[3] - get_time).seconds > 1800 or int(item[0]) < coin_num:
                if int(item[0]) == 0:
                    spe_time = 1
                    while spe_time < 3 and (list_num + spe_time) <= (len_clean_list - 1):
                        if int(clean_list[list_num + spe_time][0]) == coin_num and \
                                (clean_list[list_num + spe_time][3] - get_time).seconds < 1800:
                            get_time = item[3]
                            coin_num = int(item[0])
                            for i in range(3):
                                bank[i] = max(bank[i], int(item[i]))
                            break
                        else:
                            spe_time += 1
                    else:
                        pattern.append(bank)
                        bank = [0, 0, 0]
                        get_time = item[3]
                        coin_num = int(item[0])
                else:
                    pattern.append(bank)
                    bank = [0, 0, 0]
                    get_time = item[3]
                    coin_num = int(item[0])
            else:
                get_time = item[3]
                coin_num = int(item[0])
                for i in range(3):
                    bank[i] = max(bank[i], int(item[i]))
        pattern.append(bank)
        top = [0, 0, 0]
        for pat in pattern:
            top[0] += pat[0]
            top[1] = max(top[1], pat[1])
            top[2] = max(top[2], pat[2])
        top.append(peo_info[0])
        top.append(peo_info[1])
        return top

    @staticmethod
    def _bill_rank(bill, pid):
        rank_list = copy.deepcopy(bill)
        tune = 0
        top = 0
        while top < (len(rank_list) - tune):
            big = rank_list[top][pid]
            for i in range(top, len(rank_list) - 1):
                if big < rank_list[i + 1][pid]:
                    rank_list[i], rank_list[i + 1] = rank_list[i + 1], rank_list[i]
                    tune = 0
                else:
                    tune += 1
                    big = rank_list[i + 1][pid]
        return rank_list

    def _current_rank_by_id(self, start_cur, end_cur, start_peo, end_peo):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        cu.execute(
            'SELECT * FROM {2} WHERE isdelete=false AND id>={0} AND id<={1} '
            'ORDER BY time'.format(start_cur, end_cur, self.current_db))
        data_cur = cu.fetchall()
        cu.execute(
            'SELECT * FROM {2} WHERE isdelete=false AND id>={0} AND id<={1} '
            'ORDER BY time'.format(start_peo, end_peo, self.detail_db))
        data_peo = cu.fetchall()
        cu.close()
        conn.close()
        peo_list = [[data_peo[0][1], data_peo[0][2]], ]
        # sort detail by room id
        detail_dict = {}
        for dtl in data_peo:
            if detail_dict.get(dtl[1]):
                # start_coin, popular, online, time
                detail_dict[dtl[1]].append([dtl[3], dtl[4], dtl[5], dtl[6]])
            else:
                detail_dict[dtl[1]] = [[dtl[3], dtl[4], dtl[5], dtl[6]], ]
        # get playing time
        playing_time = {}
        for key, values in detail_dict.items():
            playing_time[key] = self._get_playing_time(values)
        # sort cur by room id
        anchor_list = []
        cur_dict_by_room = {}
        for dtl in data_cur:
            # dtl: id, userid, user name, start coin, room id, time, isdelete
            if cur_dict_by_room.get(dtl[4]):
                cur_dict_by_room[dtl[4]].append([dtl[1], dtl[2], dtl[3], dtl[5]])
            else:
                cur_dict_by_room[dtl[4]] = [[dtl[1], dtl[2], dtl[3], dtl[5]], ]
        # get the really consume in different room
        really_consume_dict_by_room_id = {}
        for key, values in playing_time.items():
            if cur_dict_by_room.get(key):
                really_consume_dict_by_room_id[key] = self._first_current_cleaner(cur_dict_by_room[key],
                                                                                  playing_time[key])
            else:
                with open(os.path.join(self.log_path, self.report_log), 'a') as fh:
                    fh.write('\nget_no_consume_data:{0} data_time{1}'.format(key, values[0]))
        # sum the consume of user in different room
        sum_by_user_id = {}
        for values in really_consume_dict_by_room_id.values():
            for key, val in values.items():
                if sum_by_user_id.get(key):
                    sum_by_user_id[key][0] = val[0]
                    sum_by_user_id[key][1] += val[1]
                else:
                    sum_by_user_id[key] = [val[0], val[1]]
        # sort dict into bill and rank it
        consume_rank_bill = []
        for key, values in sum_by_user_id.items():
            consume_rank_bill.append([key, values[0], values[1]])
        rank_list = self._bill_rank(consume_rank_bill, 2)
        rank_rst = [['排名', '玩家id', '玩家名', '消费星币数'], ]
        for i in range(30):
            rank_rst.append([i + 1, rank_list[i][0], rank_list[i][1], rank_list[i][2]])
        return rank_rst

    @staticmethod
    def _first_current_cleaner(clean_list, time_table):
        # clean_list: user id, user name, start coin,  time,
        read_mark = 0
        # same to returned item
        sort_dict = {}
        # returned item who has a list include name and ttl consume
        consume_dict_by_user_id = {}
        # sort customer info by time_table
        for mark_time in time_table:
            for i in range(read_mark, len(clean_list)):
                if clean_list[i][3] <= mark_time + timedelta(minutes=7):
                    if sort_dict.get(clean_list[i][0]):
                        sort_dict[clean_list[i][0]][0] = clean_list[i][1]
                        sort_dict[clean_list[i][0]][1] = max(sort_dict[clean_list[i][0]][1], int(clean_list[i][2]))
                    else:
                        sort_dict[clean_list[i][0]] = [clean_list[i][1], int(clean_list[i][2])]
                else:
                    read_mark = i
                    # when over read mark, sum data in sort_dict after which clear it
                    for key, values in sort_dict.items():
                        if consume_dict_by_user_id.get(key):
                            consume_dict_by_user_id[key][0] = sort_dict[key][0]
                            consume_dict_by_user_id[key][1] += sort_dict[key][1]
                        else:
                            consume_dict_by_user_id[key] = [sort_dict[key][0], sort_dict[key][1]]
                    sort_dict = {}
                    break
        if sort_dict:
            for key, values in sort_dict.items():
                if consume_dict_by_user_id.get(key):
                    consume_dict_by_user_id[key][0] = sort_dict[key][0]
                    consume_dict_by_user_id[key][1] += sort_dict[key][1]
                else:
                    consume_dict_by_user_id[key] = [sort_dict[key][0], sort_dict[key][1]]
        return consume_dict_by_user_id

    @staticmethod
    def _get_max_in_dict(st_dict):
        rst_dict = {}
        for k in st_dict.keys():
            rst_dict[k] = max(st_dict[k])
        return rst_dict

    @staticmethod
    def _get_playing_time(p_list):
        # p_list: start_coin, popular, online, time
        time_rst = []
        len_p = len(p_list) - 1
        for i in range(len_p):
            p_time = p_list[i][3]
            p_time_next = p_list[i + 1][3]
            p_coin = int(p_list[i][0])
            p_coin_next = int(p_list[i + 1][0])
            if (p_time_next - p_time).seconds > 1800 or p_coin_next < p_coin:
                if p_coin_next == 0:
                    spe_time = 2
                    while spe_time < 4 and (i + spe_time) <= len_p:
                        if p_coin == int(p_list[i+spe_time][0]) and (p_list[i + spe_time][3] - p_time).seconds < 1800:
                            break
                        else:
                            spe_time += 1
                    else:
                        time_rst.append(p_time_next)
                else:
                    time_rst.append(p_time_next)
        if time_rst:
            if time_rst[-1] != p_list[-1][3]:
                time_rst.append(p_list[-1][3])
        else:
            time_rst.append(p_list[-1][3])
        return time_rst

    def _get_id_list(self, s, e, db_name):
        conn = pymysql.connect(host=self.database_dict.get('host'), port=self.database_dict.get('port'),
                               user=self.database_dict.get('user'), password=self.database_dict.get('password'),
                               db=self.database_dict.get('db'), charset='utf8')
        cu = conn.cursor()
        sql = "SELECT mark FROM {2} WHERE time>='{0}' AND time<='{1}' ORDER BY time".format(
            s.strftime("%Y-%m-%d %H:%M:%S"), e.strftime("%Y-%m-%d %H:%M:%S"), db_name)
        cu.execute(sql)
        data = cu.fetchall()
        cu.close()
        conn.close()
        return [data[0][0], data[len(data) - 1][0]]


if __name__ == '__main__':
    spider = LaiFeng()
    spider.create_database()
    spider.detail_db = 'detail1218'
    spider.current_db = 'currentrank1218'
    spider.current_mark_db = 'currentmark1218'
    spider.anchor_mark_db = 'anchormark1218'
    # spider.create_database()
    # spider.run_all(pool_num=6)
    t1 = time.time()
    spider.reporter()
    # for info in rank[0]:
    #     print(info)
    t2 = time.time()
    print(t2 - t1)
