from django.shortcuts import render
from django.http import HttpResponse
from django.views import generic
from summary.models import UpdateRecord, Detail
import xlrd
import os
from AoiDataAnalysisSystem import settings


class IndexView(generic.ListView):
    pass


# update data from xls xlsx
class UpdateView(generic.ListView):
    model = UpdateRecord
    template_name = 'summary/update.html'


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
                try:
                    for i in range(10):
                        if col_name[i] != default_col[i]:
                            # raise error
                            print('wrong')
                            break
                    else:
                        for i in range(1, row_num):
                            Detail.create_detail(sh.row_values(i)[0], sh.row_values(i)[1], sh.row_values(i)[2],
                                                 sh.row_values(i)[3], sh.row_values(i)[4], sh.row_values(i)[5],
                                                 sh.row_values(i)[6], sh.row_values(i)[7], sh.row_values(i)[8],
                                                 sh.row_values(i)[9])
                        print('data')
                except IndexError:
                    print('wrong')
            # delete the file after complete update
            os.remove(file_address)
            # redirect and return success message
            return HttpResponse('get')
        else:
            # redirect and return error message
            return HttpResponse('get')
    else:
        return HttpResponse('Please use post to update')

