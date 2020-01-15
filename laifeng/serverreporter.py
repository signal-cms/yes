# -*- coding:utf-8 -*-
# this program design for update hot list daily in vm
import copy
import math
import os
import pymysql
import threading
from datetime import datetime, timedelta
import xlwt
import warnings
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


def get_attribute(default, set_value):
    if set_value:
        return set_value
    else:
        return default


class AnalyseData:

    # wait to do: a default file to determine the init attribute
    # the attributes used are defined in __init__, it's convenience to manage
    def __init__(
            self,
            database_dict=None,
            report_path=None):
        default_database_dict = {
            'host': 'localhost',
            'port': 3306,
            'user': 'tester',
            'password': 'Test_1357642',
            'db': 'laifeng'}
        default_report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'report')
        self.report_path = get_attribute(default_report_path, report_path)

        def new_get(k):
            return database_dict.get(k, default_database_dict[k])

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
        self.hot_anchor_db = 'hot'
        self.report_log = 'report.log'
        self.anchor_day_db = 'dailyanchor'
        self.consumer_day_db = 'dailyconsumer'

    def create_database(self):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        # anchor daily data
        anchor_sql = 'create table if not exists {}(id int auto_increment, roomid varchar(20), peo varchar(50), ' \
                     'xingbi varchar(20), renqi varchar(20), online varchar(20), time date,' \
                     'isdelete boolean default false,primary key(id)) charset=utf8'.format(self.anchor_day_db)
        cu.execute(anchor_sql)
        conn.commit()
        # the consumer daily data
        consumer_sql = 'create table if not exists {}(id int auto_increment, userid varchar(20), ' \
                       'username varchar(50), xingbi varchar(20), time date, ' \
                       'isdelete boolean default false,primary key(id)) charset=utf8'.format(self.consumer_day_db)
        cu.execute(consumer_sql)
        conn.commit()
        # hot list
        hot_sql = 'create table if not exists {}(id int unique, content varchar(150), primary key(id)) ' \
                  'charset=utf8'.format(self.hot_anchor_db)
        cu.execute(hot_sql)
        conn.commit()
        cu.close()
        conn.close()

    @staticmethod
    def join_num(raw_list):
        int_list = copy.deepcopy(raw_list)
        int_list = list(map(str, int_list))
        return '_'.join(int_list)

    def read_db_all(self, db_name, start_id, end_id, is_delete=True):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        if is_delete:
            cu.execute(
                'SELECT * FROM {} WHERE isdelete=false AND id >= {} AND id <= {}'.format(db_name, start_id, end_id)
            )
        else:
            cu.execute(
                'SELECT * FROM {} WHERE id >= {} AND id <= {}'.format(db_name, start_id, end_id)
            )
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

    def get_data_by_sql_read_once(self, sql_list):
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
            yield cu.fetchall()
        cu.close()
        conn.close()

    def get_data_by_time(self, db_name, start, end):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        cu.execute(
            "select * from {0} where isdelete=false and time>='{1}' and time<='{2}' order by id".format(
                db_name, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
        )
        data = cu.fetchall()
        cu.close()
        conn.close()
        return data

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

    def thread_task(self, raw_list, target, default_num=10):
        task_list = self.sort_thread_para_list(raw_list, default_num)
        task_bank = []
        for task_info in task_list:
            task_bank.append(threading.Thread(target=target, args=(task_info,)))
        for task in task_bank:
            task.start()
        for task in task_bank:
            task.join()

    @staticmethod
    def add2item(add_list, add2item):
        for item in add_list:
            add2item.add(item)

    def reporter(self, today):
        data_bank = self.get_one_day_data(today)
        anchor_rank_list = self._writer(data_bank, today - timedelta(days=1))
        self._update_hot_list(anchor_rank_list[0])
        self.send_mail_five(anchor_rank_list[1][0], anchor_rank_list[1][1])

    def send_mail_five(self, file_address, file_name):
        try_time = 0
        while try_time < 5:
            try:
                self.send_mail(file_address, file_name)
                break
            except Exception:
                try_time += 1

    @staticmethod
    def send_mail(file_address, file_name):
        from_address = 'cms199631@163.com'
        password = 'cms146847'
        toaddrs = ['cms199631@163.com', 'kymlinqi@outlook.com', ]
        content = '排行榜数据'
        text_part = MIMEText(content)
        xls_part = MIMEApplication(open(file_address, 'rb').read())
        xls_part.add_header('Content-Disposition', 'attachment', filename=file_name)
        m = MIMEMultipart()
        m.attach(text_part)
        m.attach(xls_part)
        m['Subject'] = '主播玩家排行'
        m['From'] = from_address
        m['To'] = ','.join(toaddrs)
        server = smtplib.SMTP_SSL('smtp.163.com', 465)
        # server.set_debuglevel(1)
        # server.connect('smtp.163.com', 25)
        server.login(from_address, password)
        server.sendmail(from_address, toaddrs, m.as_string())
        server.quit()

    def _update_hot_list(self, top_data):
        hot_list = set()
        old_hot_list = self._get_old_hot_list()
        for part in top_data:
            for i in range(3):
                for j in range(1, 31):
                    hot_list.add(int(part[i][j][1]))
        self.add2item(old_hot_list, hot_list)
        hot_list = list(hot_list)
        self.update_ten_mark_list_for_hot(self.hot_anchor_db, hot_list)

    def _get_old_hot_list(self):
        hot_list = []
        # read hot list
        hot_mark = self.read_db_all(self.hot_anchor_db, 1, 1, is_delete=False)
        raw_list = self.read_db_all(self.hot_anchor_db, 2, int(hot_mark[0][1]), is_delete=False)
        for room in raw_list:
            pat_list = room[1].split('_')
            for room_id in pat_list:
                hot_list.append(room_id)
        return list(map(int, hot_list))

    def update_ten_mark_list_for_hot(self, db_name, data_list):
        data_num = len(data_list)
        sql_list = []
        group_num = math.ceil(data_num / 10)
        # mark sql
        mark_content = '{0}'.format(
            group_num + 1
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
        self.thread_task(sql_list, self.execute_db_more)

    def get_one_day_data(self, today):
        # rst: data_bank [0] for anchor, [1] for consumer
        data_bank = [[], [], ]
        one_day = today - timedelta(days=1)
        # anchor msg
        beginning = self._get_first_time(self.consumer_mark_db, one_day)[1]
        start = self._get_last_time(self.consumer_mark_db, one_day)
        end = self._get_first_time(self.consumer_mark_db, today)
        id_list_anchor = self._get_id_list(start[1], end[1], self.anchor_db)
        detail_dict = self._sort_data_by_id(id_list_anchor[0], id_list_anchor[1])
        playing_time_dict = {}
        for key, values in detail_dict.items():
            # return[[xingbi(ttl), renqi(max), online(max), roomid, peoname], ][playing_time for consumer]
            bill = self._detail_cleaner(values, key, beginning)
            data_bank[0].append(bill[0])
            playing_time_dict[key] = bill[1]
        # consumer msg
        id_list_consumer = self._get_id_list(start[1], end[1], self.consumer_db)
        consumer_data = self._consumer_data(
            id_list_consumer[0], id_list_consumer[1], playing_time_dict, beginning, end[1]
        )
        data_bank[1] = consumer_data[0]
        # retest anchor star coin
        for i in range(len(data_bank[0])):
            if consumer_data[1].get(data_bank[0][i][3]) and consumer_data[1][data_bank[0][i][3]] > data_bank[0][i][0]:
                data_bank[0][i][0] = consumer_data[1][data_bank[0][i][3]]
        return data_bank

    def _writer(self, data_bank, one_day):
        # at first write data into database
        # write anchor
        anchor_sql_module = "insert into {0}(roomid, peo, xingbi, renqi, online, time) " \
                            "values('{1}','{2}','{3}','{4}','{5}','{6}')"
        anchor_sql = []
        # data_bank[0]: 0xingbi(ttl), 1renqi(max), 2online(max), 3roomid, 4peoname
        time_data = one_day.strftime('%Y-%m-%d')
        for info in data_bank[0]:
            anchor_sql.append(anchor_sql_module.format(
                self.anchor_day_db, info[3], info[4], info[0], info[1], info[2], time_data
            ))
        self.thread_task(anchor_sql, self.execute_db_more)
        # write consumer
        consumer_sql_module = "insert into {0}(userid, username, xingbi, time) values('{1}','{2}','{3}','{4}')"
        consumer_sql = []
        for info in data_bank[1]:
            consumer_sql.append(consumer_sql_module.format(
                self.consumer_day_db, info[0], info[1], info[2], time_data
            ))
        self.thread_task(consumer_sql, self.execute_db_more)
        # get one day rank
        # rank anchor
        anchor_top30_1 = self.rank_anchor(data_bank[0])
        # rank consumer
        consumer_top30_1 = self.rank_consumer(data_bank[1])
        # get more day rank for anchor
        anchor_top30_7 = self.more_day_anchor_rank(one_day, 7)
        anchor_top30_30 = self.more_day_anchor_rank(one_day, 30)
        # get more day rank for consumer
        consumer_top30_7 = self.more_day_consumer_rank(one_day, 7)
        consumer_top30_30 = self.more_day_consumer_rank(one_day, 30)
        # make whole into one list for write convenient
        data_list = [
            anchor_top30_1,
            anchor_top30_7,
            anchor_top30_30,
            [consumer_top30_1, ],
            [consumer_top30_7, ],
            [consumer_top30_30, ],
        ]
        # sheet_title
        sheet_name_list = [
            ['主播星币日榜', '主播人气日榜', '主播在线人数日榜'],
            ['主播星币周榜', '主播人气周榜', '主播在线人数周榜'],
            ['主播星币月榜', '主播人气月榜', '主播在线人数月榜'],
            ['玩家星币日榜', ],
            ['玩家星币周榜', ],
            ['玩家星币月榜', ],
        ]
        wb_name = '{}.xls'.format(one_day.strftime('%Y%m%d'))
        wb_address = os.path.join(self.report_path, wb_name)
        wb = xlwt.Workbook(encoding='utf-8')
        for i in range(6):
            for j in range(len(data_list[i])):
                sh = wb.add_sheet(sheet_name_list[i][j])
                self._write_sheet(sh, data_list[i][j])
        wb.save(wb_address)
        return data_list[:3], [wb_address, wb_name]

    def more_day_consumer_rank(self, one_day, delta_day):
        # read mark to get id
        before_daytime = one_day - timedelta(delta_day - 1)
        # 0id. 1userid, 2username, 3xingbi, 4time, 5isdelete
        data = self.get_data_by_time(self.consumer_day_db, before_daytime, one_day)
        # sort data by anchor id
        consumer_dict = {}
        for info in data:
            if consumer_dict.get(info[1]):
                consumer_dict[info[1]][0] = info[2]
                consumer_dict[info[1]][1] += int(info[3])
            else:
                consumer_dict[info[1]] = [info[2], int(info[3]), ]
        # organize into one list
        consumer_list = []
        for key, values in consumer_dict.items():
            consumer_list.append([key, values[0], values[1]])
        # rank and return top30
        return self.rank_consumer(consumer_list)

    def more_day_anchor_rank(self, one_day, delta_day):
        # read mark to get id
        before_daytime = one_day - timedelta(delta_day - 1)
        # 0id, 1roomid, 2peo, 3xingbi, 4renqi, 5online, 6time date, 7isdelete
        data = self.get_data_by_time(self.anchor_day_db, before_daytime, one_day)
        # sort data by anchor id
        anchor_dict = {}
        for info in data:
            if anchor_dict.get(info[1]):
                anchor_dict[info[1]][0] += int(info[3])
                for i in range(1, 3):
                    anchor_dict[info[1]][i] = max(anchor_dict[info[1]][i], int(info[i + 3]))
                anchor_dict[info[1]][3] = info[2]
            else:
                # [xingbi(ttl), renqi(max), online(max), peoname]
                anchor_dict[info[1]] = [int(info[3]), int(info[4]), int(info[5]), info[2]]
        # Organize into a list
        # [[xingbi(ttl), renqi(max), online(max), roomid, peoname], ]
        anchor_list = []
        for key, values in anchor_dict.items():
            anchor_list.append([values[0], values[1], values[2], key, values[3]])
        # rank and return top30
        return self.rank_anchor(anchor_list, delta_day)

    def rank_anchor(self, data_bank, day_num=1):
        # rank is similar to bill[0], but by order
        title = ['星币', '最高人气', '最大在线数']
        rst = []
        for j in range(3):
            # skip the robot about movie
            skip_robot = 0
            rank_list = self._bill_rank(data_bank, j)
            top30 = []
            for i in range(31):
                if i == 0:
                    top30.append(['排名', '房间号', '主播名', title[j]])
                else:
                    while rank_list[i - 1 + skip_robot][2] > 2000 and rank_list[i - 1 + skip_robot][0] < 1000 * day_num:
                        skip_robot += 1
                    top30.append([
                        i,
                        rank_list[i - 1 + skip_robot][3],
                        rank_list[i - 1 + skip_robot][4],
                        rank_list[i - 1 + skip_robot][j]
                    ])
            rst.append(top30)
        return rst

    def _get_first_time(self, db_name, date_info, is_day=True):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        if is_day:
            cu.execute(
                "SELECT * FROM {} WHERE time >= '{}' order by id asc limit 1".format(
                    db_name, date_info.strftime('%Y-%m-%d'))
            )
        else:
            cu.execute(
                "SELECT * FROM {} WHERE time >= '{}' order by id asc limit 1".format(
                    db_name, date_info)
            )
        data = cu.fetchall()
        cu.close()
        conn.close()
        if data[0][0] == 1:
            return [1, datetime(year=date_info.year, month=date_info.month, day=date_info.day)]
        else:
            for info in data[0]:
                if isinstance(info, datetime):
                    return [data[0][0], info]

    def _get_last_time(self, db_name, date_info, is_day=True):
        conn = pymysql.connect(
            host=self.database_dict.get('host'),
            port=self.database_dict.get('port'),
            user=self.database_dict.get('user'),
            password=self.database_dict.get('password'),
            db=self.database_dict.get('db'),
            charset='utf8')
        cu = conn.cursor()
        if is_day:
            cu.execute(
                "SELECT * FROM {} WHERE time <= '{}' order by id desc limit 1".format(
                    db_name, date_info.strftime('%Y-%m-%d'))
            )
        else:
            cu.execute(
                "SELECT * FROM {} WHERE time <= '{}' order by id desc limit 1".format(
                    db_name, date_info)
            )
        data = cu.fetchall()
        cu.close()
        conn.close()
        if not data or data[0][0] == 1:
            return [1, datetime(year=date_info.year, month=date_info.month, day=date_info.day)]
        else:
            for info in data[0]:
                if isinstance(info, datetime):
                    return [data[0][0], info]

    # only for list or generator
    @staticmethod
    def _write_sheet(sheet_obj, data):
        for i in range(len(data)):
            for j in range(len(data[i])):
                sheet_obj.write(i, j, data[i][j])

    def _sort_data_by_id(self, start, end):
        # data[]: 0id, 1room_id, 2peo_name, 3xingbi, 4renqi, 5online, 6time, 7isdelete
        data = self.read_db_all(self.anchor_db, start, end)
        # peo_list = [[data[0][1], data[0][2]], ]
        # sorted by room id
        detail_dict = {}
        for dtl in data:
            if detail_dict.get(dtl[1]):
                # detail_dict: xingbi, renqi, online, time, name
                detail_dict[dtl[1]].append([dtl[3], dtl[4], dtl[5], dtl[6], dtl[2]])
            else:
                detail_dict[dtl[1]] = [[dtl[3], dtl[4], dtl[5], dtl[6], dtl[2]], ]
        return detail_dict

    @staticmethod
    def _get_really_name(old_name, new_name):
        if new_name != '下播检查':
            return new_name
        else:
            return old_name

    def _detail_cleaner(self, clean_list, room_id, beginning):
        # clean_list: [[xingbi, renqi, online, time, name], ...] info order by time asc
        playing_time = self._get_playing_time(clean_list)
        if clean_list[0][3] < beginning:
            beginning_coin = int(clean_list[0][0])
        else:
            beginning_coin = 0
        pattern = []
        for part in playing_time[1]:
            bank = [0, 0, 0]
            for dtl in part:
                peo_name = dtl[4]
                for i in range(3):
                    bank[i] = max(bank[i], int(dtl[i]))
            pattern.append(bank)
        top = [0, 0, 0]
        for pat in pattern:
            top[0] += pat[0]
            top[1] = max(top[1], pat[1])
            top[2] = max(top[2], pat[2])
        top[0] -= beginning_coin
        top.append(room_id)
        top.append(peo_name)
        # return [xingbi(ttl), renqi(max), online(max), roomid, peoname]
        return [top, playing_time[0]]

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

    def _consumer_data(self, start, end, playing_time_dict, beginning, ending):
        # data: 0id, 1user_id, 2user_name, 3xingbi, 4room_id, 5time, 6isdelete
        data = self.read_db_all(self.consumer_db, start, end)
        # sort detail by room id
        # consumer[]=[[id, name, xingbi, time], ]
        consumer_dict_by_room = {}
        for info in data:
            if consumer_dict_by_room.get(info[4]):
                consumer_dict_by_room[info[4]].append([info[1], info[2], info[3], info[5]])
            else:
                consumer_dict_by_room[info[4]] = [[info[1], info[2], info[3], info[5]], ]
        # change playing_time from player to consumer
        self._change_into_consumer_time(playing_time_dict, ending)
        # get the really consume in different room
        really_consume_dict_by_room_id = {}
        for key, values in playing_time_dict.items():
            if consumer_dict_by_room.get(key):
                really_consume_dict_by_room_id[key] = self._first_current_cleaner(
                    consumer_dict_by_room[key],
                    playing_time_dict[key],
                    beginning
                )
            else:
                with open(os.path.join(self.report_path, self.report_log), 'a') as fh:
                    fh.write('\nget_no_consume_data:{0} data_time {1}'.format(key, values[0]))
        # sum the consume of user in different room
        sum_by_user_id = {}
        anchor_retest_dict = {}
        for anchor_room, values in really_consume_dict_by_room_id.items():
            for key, val in values.items():
                # get retest data dict of star coin
                if anchor_retest_dict.get(anchor_room):
                    anchor_retest_dict[anchor_room] += val[1]
                else:
                    anchor_retest_dict[anchor_room] = val[1]
                # sum star coin by user id
                if sum_by_user_id.get(key):
                    sum_by_user_id[key][0] = val[0]
                    sum_by_user_id[key][1] += val[1]
                else:
                    sum_by_user_id[key] = [val[0], val[1]]
        # sort dict into bill for ranking
        consume_rank_bill = []
        for key, values in sum_by_user_id.items():
            consume_rank_bill.append([key, values[0], values[1]])
        return [consume_rank_bill, anchor_retest_dict]

    def rank_consumer(self, consume_rank_bill):
        rank_list = self._bill_rank(consume_rank_bill, 2)
        rank_rst = [['排名', '玩家id', '玩家名', '消费星币数'], ]
        for i in range(30):
            rank_rst.append([i + 1, rank_list[i][0], rank_list[i][1], rank_list[i][2]])
        return rank_rst

    def _change_into_consumer_time(self, playing_time, ending):
        # get peo_list
        peo_list = []
        for key in playing_time.keys():
            peo_list.append(key)
        task_list = self.sort_thread_para_list(peo_list)
        task_bank = []
        for task_info in task_list:
            task_bank.append(threading.Thread(
                target=self._consumer_time_task, args=(task_info, playing_time, ending)
            ))
        for task in task_bank:
            task.start()
        for task in task_bank:
            task.join()

    def _consumer_time_task(self, id_list, playing_time, ending):
        for id_num in id_list:
            for i in range(len(playing_time[id_num])):
                playing_time[id_num][i] = self._get_first_time(
                    self.consumer_mark_db, playing_time[id_num][i], is_day=False
                )[1]
                if i == len(playing_time[id_num]) - 1 and (ending - playing_time[id_num][i]).seconds > 3600:
                    playing_time[id_num].append(ending)
                    break

    @staticmethod
    def _first_current_cleaner(clean_list, time_table, beginning):
        # clean_list: user id, user name, start coin,  time,
        # get beginning coin
        beginning_coin_by_user_id = {}
        for info in clean_list:
            if info[3] <= beginning:
                beginning_coin_by_user_id[info[0]] = int(info[2])
        read_mark = 0
        # same to returned item
        sort_dict = {}
        # returned item who has a list include name and ttl consume
        consume_dict_by_user_id = {}
        # sort customer info by time_table
        for mark_time in time_table:
            for i in range(read_mark, len(clean_list)):
                if clean_list[i][3] < mark_time:
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
            # cut the data yesterday
            for key in consume_dict_by_user_id.keys():
                if beginning_coin_by_user_id.get(key):
                    consume_dict_by_user_id[key][1] -= beginning_coin_by_user_id[key]
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
        rst = [[], [[], ]]
        len_p = len(p_list) - 1
        for i in range(len_p):
            p_time = p_list[i][3]
            p_time_next = p_list[i + 1][3]
            p_coin = int(p_list[i][0])
            p_coin_next = int(p_list[i + 1][0])
            if (p_time_next - p_time).seconds > 1800 and p_coin_next < p_coin:
                rst[1][len(rst[0])].append(p_list[i])
                rst[0].append(p_time_next)
                rst[1].append([])
            elif p_coin_next >= p_coin:
                rst[1][len(rst[0])].append(p_list[i])
            else:
                if p_coin_next == 0:
                    if (i + 2) <= len_p and (p_list[i + 2][3] - p_time_next).seconds < 1801 \
                            and int(p_list[i + 2][0]) >= p_coin:
                        rst[1][len(rst[0])].append(p_list[i])
                    else:
                        rst[1][len(rst[0])].append(p_list[i])
                        rst[0].append(p_time_next)
                        rst[1].append([])
                else:
                    rst[1][len(rst[0])].append(p_list[i])
                    rst[0].append(p_time_next)
                    rst[1].append([])
        rst[1][-1].append(p_list[-1])
        if rst[0]:
            if rst[0][-1] != p_list[-1][3]:
                rst[0].append(p_list[-1][3])
        else:
            rst[0].append(p_list[-1][3])
        return rst

    def _get_id_list(self, s, e, db_name):
        return [
            self._get_first_time(db_name, s, is_day=False)[0],
            self._get_first_time(db_name, e, is_day=False)[0]
        ]


if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    ana = AnalyseData()
    # ana.hot_anchor_db = 'hot1231'
    ana.create_database()
    ana.reporter(datetime.now())
