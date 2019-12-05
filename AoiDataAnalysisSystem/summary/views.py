from django.shortcuts import render, redirect
from django.http import HttpResponse, HttpResponseRedirect
from django.views import generic
from summary.models import UpdateRecord, Detail
import xlrd
import os
from datetime import datetime, date
from AoiDataAnalysisSystem import settings
from django.urls import reverse
from django.db.models import Q, Sum
from daily.models import DayData
from datetime import date
from django.utils import timezone
from week.models import WeekData


class IndexView(generic.ListView):
    pass


# update data from xls xlsx
# class UpdateView(generic.ListView):
#     model = UpdateRecord
#     template_name = 'summary/update.html'
def update_page(request):
    msg = request.session.get('msg', None)
    pkg = {'msg': msg}
    request.session['msg'] = ''
    return render(request, 'summary/update.html', pkg)


def update_detail(request):
    if request.method == 'POST':
        f = request.FILES['file']
        if f.name.split('.')[1] in ['xls', 'xlsx']:
            file_address = os.path.join(settings.TEMPORARY_PATH, f.name)
            with open(file_address, 'wb') as fp:
                for data in f.chunks():
                    fp.write(data)
            with xlrd.open_workbook(file_address) as wb:
                sh = wb.sheet_by_index(0)
                row_num = sh.nrows
                col_name = sh.row_values(0)
                default_col = ['panel_id', 'line_id', 'product_id', 'aoi_code', 'aoi_address', 'aoi_time', 'op_id',
                               'fi_code', 'fi_address', 'fi_time']
                for i in range(10):
                    if col_name[i] != default_col[i]:
                        request.session['msg'] = '上传Excel中第{0}列不是{1},而是{2},请确认后重新上传'.format(
                            i + 1, default_col[i], col_name[i])
                        return redirect(reverse('summary:update'))
                else:
                    try:
                        for i in range(1, row_num):
                            Detail.create_detail(sh.row_values(i)[0], sh.row_values(i)[1], sh.row_values(i)[2],
                                                 sh.row_values(i)[3], sh.row_values(i)[4], sh.row_values(i)[5],
                                                 sh.row_values(i)[6], sh.row_values(i)[7], sh.row_values(i)[8],
                                                 sh.row_values(i)[9])
                    except ValueError as err:
                        request.session['msg'] = '第{0}行数据出错,请检查后重新上传,error如下：{1}'.format(i + 1, err)
                        return redirect(reverse('summary:update'))
            # delete the file after complete update
            os.remove(file_address)
            # redirect and return success message
            request.session['msg'] = '成功上传数据，点击更新可更新日月周季年数据'
            # when success redirect to update_summary page
            return redirect(reverse('summary:update'))
        else:
            # redirect and return error message
            request.session['msg'] = '请上传xls/xlsx格式文件'
            return redirect(reverse('summary:update'))
    else:
        request.session['msg'] = '请使用POST方法上传数据'
        return redirect(reverse('summary:update'))


# method for day week month quarter year update
def add_zero(num):
    if num > 9:
        return str(num)
    else:
        return '0{0}'.format(num)


# the part for daily
def update_redirect_day(request):
    # if there is none in record, it will count from Detail(pk=1)
    if not UpdateRecord.objects.values('col_num').filter(update_type='day').exists():
        record_num = 0
        mark_day = date(1970, 1, 1)
    else:
        rcd = UpdateRecord.objects.filter(update_type='day').last()
        record_num = rcd.col_num
        read_mark = rcd.update_mark.split('-')
        mark_day = date(int(read_mark[0]), int(read_mark[1]), int(read_mark[2]))
    if Detail.dtlobj.filter(pk__gt=record_num).exists():
        day_list = Detail.dtlobj.filter(pk__gt=record_num).values('rst_date').distinct()
    else:
        # need use redirect to return a unnecessary2update message
        request.session['msg'] = '日别数据库已是最新数据，无需更新'
        return redirect(reverse('summary:update_redirect_week'))
    # clean day_list
    day_list = [daytime['rst_date'] for daytime in day_list if daytime['rst_date'] >= mark_day]
    for dt in day_list:
        try:
            rst_date = dt
            miss = Detail.dtlobj.filter(Q(rst_date=rst_date) & Q(is_miss=True)).count()
            overkill = Detail.dtlobj.filter(Q(rst_date=rst_date) & Q(is_overkill=True)).count()
            useless = Detail.dtlobj.filter(Q(rst_date=rst_date) & Q(is_useless=True)).count()
            aoi_in = Detail.dtlobj.filter(Q(rst_date=rst_date)).count()
            aoi_ng = Detail.dtlobj.filter(Q(rst_date=rst_date) & Q(aoi_result=False)).count()
            fi_in = Detail.dtlobj.filter(Q(rst_date=rst_date) & ~Q(op_id='')).count()
            DayData.create_day(rst_date=rst_date, miss=miss, overkill=overkill, useless=useless, aoi_in=aoi_in,
                               aoi_ng=aoi_ng, fi_in=fi_in)
        except Exception as err:
            UpdateRecord.objects.create(update_type='day', col_num=record_num, update_mark='{0}-{1}-{2}'.format(
                dt.year, dt.month, dt.day), update_time=timezone.now())
            request.session['msg'] = '更新日别数据出现异常如下:{0}'.format(err)
            return redirect(reverse('summary:update'))
    UpdateRecord.objects.create(update_type='day', col_num=Detail.dtlobj.last().pk, update_mark='{0}-{1}-{2}'.format(
                                max(day_list).year, max(day_list).month, max(day_list).day), update_time=timezone.now())
    request.session['msg'] = r'日别数据库成功更新'
    return redirect(reverse('summary:update_redirect_week'))


# the part for week
def update_redirect_week(request):
    if not UpdateRecord.objects.values('col_num').filter(update_type='week').exists():
        record_num = 0
        mark_week = '1970.0'
    else:
        rcd = UpdateRecord.objects.filter(update_type='week').last()
        record_num = rcd.col_num
        mark_week = rcd.update_mark
    if DayData.objects.filter(pk__gt=record_num).exists():
        day_list = DayData.objects.filter(pk__gt=record_num).values('rst_date').distinct()
    else:
        # need use redirect to return a unnecessary2update message
        request.session['msg'] = '{0}{1}'.format(request.session.get('msg', ''), '/n数据库已是最新数据，无需更新')
        return redirect(reverse('summary:update_redirect_month'))
    # build day_dict
    day_dict = {}
    for dt in day_list:
        storage = '{0}.{1}'.format(dt['rst_date'].isocalendar()[0], add_zero(dt['rst_date'].isocalendar()[1]))
        if storage >= mark_week:
            if day_dict.get(storage):
                day_dict[storage].append(dt['rst_date'])
            else:
                day_dict[storage] = [dt['rst_date'], ]
    # update database by week num
    for ky in day_dict.keys():
        min_mark = min(day_dict[ky])
        max_mark = max(day_dict[ky])
        st = ky.split('.')
        year_num = int(st[0])
        week_num = int(st[1])
        miss = DayData.objects.filter(Q(pk__gt=record_num) & Q(rst_date__gte=min_mark) & Q(rst_date__lte=max_mark)
                                      ).aggregate(nums=Sum('miss'))['nums']
        overkill = DayData.objects.filter(Q(pk__gt=record_num) & Q(rst_date__gte=min_mark) & Q(rst_date__lte=max_mark)
                                          ).aggregate(nums=Sum('overkill'))['nums']
        useless = DayData.objects.filter(Q(pk__gt=record_num) & Q(rst_date__gte=min_mark) & Q(rst_date__lte=max_mark)
                                         ).aggregate(nums=Sum('useless'))['nums']
        aoi_in = DayData.objects.filter(Q(pk__gt=record_num) & Q(rst_date__gte=min_mark) & Q(rst_date__lte=max_mark)
                                        ).aggregate(nums=Sum('aoi_in'))['nums']
        aoi_ng = DayData.objects.filter(Q(pk__gt=record_num) & Q(rst_date__gte=min_mark) & Q(rst_date__lte=max_mark)
                                        ).aggregate(nums=Sum('aoi_ng'))['nums']
        fi_in = DayData.objects.filter(Q(pk__gt=record_num) & Q(rst_date__gte=min_mark) & Q(rst_date__lte=max_mark)
                                       ).aggregate(nums=Sum('fi_in'))['nums']
        try:
            WeekData.create_week(year_num=year_num, week_num=week_num, miss=miss, overkill=overkill, useless=useless,
                                 aoi_in=aoi_in, aoi_ng=aoi_ng, fi_in=fi_in)
        except Exception as err:
            request.session['msg'] = '{0}/n更新日别数据出现异常如下:{1}'.format(request.session.get('msg', ''), err)
            UpdateRecord.objects.create(update_type='week', col_num=record_num, update_mark=ky,
                                        update_time=timezone.now())
            return redirect(reverse('summary:update'))
    UpdateRecord.objects.create(update_type='week', col_num=DayData.objects.last().pk,
                                update_mark=max([ky for ky in day_dict.keys()]), update_time=timezone.now())
    request.session['msg'] = '{0}/n{1}'.format(request.session.get('msg', ''), r'周别数据库成功更新')
    return redirect(reverse('summary:update_redirect_month'))


def update_redirect_month(request):
    pass


def update_redirect_quart(request):
    pass


def update_redirect_year(request):
    pass

