import re


def judge_info(info_list):
    if info_list[0][0] == info_list[0][1]:
        return info_list[0][0]
    else:
        return info_list[0][0], info_list[0][1]


def get_num(html_file):
    # get info by re
    pat_xingbi = re.compile(r'<cite title="(.*?)">(.*?)</cite>星币</div>')
    pat_renqi = re.compile(r'<cite title="(.*?)">(.*?)</cite>人气')
    pat_online = re.compile(r'<cite title="(.*?)">(.*?)</cite>在线')
    xingbi = pat_xingbi.findall(data)
    renqi = pat_renqi.findall(data)
    online = pat_online.findall(data)
    # judge the correction of info
    xingbi = judge_info(xingbi)
    renqi = judge_info(renqi)
    online = judge_info(online)
    return xingbi, renqi, online


def check_info():
    with open('/home/signal/django-project/laifeng/pa.html', 'r') as fh:
        data = fh.read()
    rst = get_num(data)
    # double check for info
    for obj in rst:
        if not isinstance(obj, int):
            with open('getinfo.log', 'a') as fl:

    print(rst)


