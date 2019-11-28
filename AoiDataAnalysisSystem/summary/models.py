from django.db import models
from datetime import datetime, date
from django.utils import timezone


# Create your models here.
# create datetime(YYYY-MM-DD HH) by a string whose length longer 10
def str2date(date_string):
    fn = lambda s, x, y: int(s[x:y])
    return datetime(year=fn(date_string, 0, 4), month=fn(date_string, 4, 6), day=fn(date_string, 6, 8),
                    hour=fn(date_string, 8, 10))


class AoiManager(models.Manager):
    def get_queryset(self):
        return super(AoiManager, self).get_queryset().filter(is_delete=False)


# the model records the detail data about aoi/fi
class Detail(models.Model):
    # default objects
    objects = models.Manager()
    # new object which select the data whose is_delete=false
    dtlobj = AoiManager()
    panel_id = models.CharField(max_length=20, db_column='PanelID')
    line_id = models.CharField(max_length=6, db_column='LineID')
    product_id = models.CharField(max_length=20, db_column='ProductID')
    rst_date = models.DateField(db_column='Date')
    aoi_result = models.BooleanField(db_column='AOIResult')
    aoi_code = models.CharField(max_length=100, null=True, db_column='AOIReasonCode')
    aoi_address = models.IntegerField(db_column='AOIAddress')
    aoi_time = models.DateTimeField(db_column='AOITime')
    op_id = models.CharField(max_length=100, null=True, db_column='OperatorID')
    fi_result = models.NullBooleanField(db_column='FIResult')
    fi_code = models.CharField(max_length=100, null=True, db_column='FIReasonCode')
    fi_address = models.IntegerField(db_column='FIAddress')
    fi_time = models.DateTimeField(null=True, db_column='FITime')
    is_miss = models.NullBooleanField(db_column='isMiss')
    is_overkill = models.NullBooleanField(db_column='isOverkill')
    is_useless = models.BooleanField(db_column='isUselessData')
    is_checkout = models.NullBooleanField(db_column='isAOICheckOut')
    is_delete = models.BooleanField(default=False)

    def __str__(self):
        return '{0}AOI Judge:{1}'.format(self.panel_id, self.aoi_result)

    @staticmethod
    # offer a method to autofill detail model
    def create_detail(panel_id, line_id, product_id, aoi_code, aoi_address, aoi_time, op_id='', fi_code='',
                      fi_address=0, fi_time="", is_delete=False):
        if aoi_code == '':
            aoi_result = True
            aoi_address = 0
        else:
            aoi_result = False
        aoi_time = timezone.make_aware(str2date(aoi_time))
        rst_date = date(year=aoi_time.year, month=aoi_time.month, day=aoi_time.day)
        if op_id == '':
            fi_code = ''
            fi_result = ''
            fi_address = 0
            fi_time = aoi_time
        elif fi_code != '':
            fi_result = False
            fi_time = timezone.make_aware(str2date(fi_time))
        else:
            fi_result = True
            fi_time = timezone.make_aware(str2date(fi_time))
        # define list for useless date
        useless_list = ['异常点灯', '功能其他', '亮度异常', 'MFC02', 'MFC11', 'MBD05', 'MBD13']
        is_useless = aoi_code in useless_list
        # judge miss overkill checkout
        if op_id == '':
            is_miss = ''
            is_overkill = ''
            is_checkout = ''
        elif aoi_code == '' and fi_code != '':
            is_miss = True
            is_overkill = ''
            is_checkout = False
        elif aoi_code != '' and fi_code == '':
            is_miss = ''
            is_overkill = True
            is_checkout = ''
        elif aoi_code != '' and fi_code != '':
            is_miss = ''
            is_overkill = False
            is_checkout = True
        else:
            is_miss = False
            is_overkill = ''
            is_checkout = ''
        return Detail.dtlobj.create(panel_id=panel_id, line_id=line_id, product_id=product_id, rst_date=rst_date,
                                    aoi_result=aoi_result, aoi_code=aoi_code, aoi_address=aoi_address,
                                    aoi_time=aoi_time, op_id=op_id, fi_result=fi_result, fi_code=fi_code,
                                    fi_address=fi_address, fi_time=fi_time, is_miss=is_miss, is_overkill=is_overkill,
                                    is_useless=is_useless, is_checkout=is_checkout, is_delete=is_delete)


class UpdateRecord(models.Model):
    update_time = models.DateTimeField(auto_created=True, db_column='time')
    col_num = models.IntegerField(db_column='record')

'''
class Summary(models.Model):
    pass

    @staticmethod
    def update_data():
        pass'''
