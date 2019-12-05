from django.db import models
from django.db.models import Q, Sum


# method avoid division error
def artificial_division(x, y):
    if y == 0:
        return 0
    else:
        return x / y


class MonthData(models.Model):
    year = models.IntegerField()
    month_num = models.IntegerField(db_column='month')
    miss = models.IntegerField()
    overkill = models.IntegerField()
    useless = models.IntegerField()
    aoi_in = models.IntegerField(db_column='AOIIn')
    aoi_ok = models.IntegerField(db_column='AOIOk')
    aoi_ng = models.IntegerField(db_column='AOINg')
    fi_in = models.IntegerField(db_column='FIIn')
    miss_rate = models.FloatField(db_column='MissRate')
    overkill_rate = models.FloatField(db_column='OverkillRate')
    useless_rate = models.FloatField(db_column='UselessRate')
    miss_aoi = models.FloatField(db_column='MissRateAoiOk')
    overkill_aoi = models.FloatField(db_column='OverkillRateAoiNg')

    @classmethod
    def create_month(cls, year_num, month_num, miss, overkill, useless, aoi_in, aoi_ng, fi_in):
        if MonthData.objects.filter(month_num=month_num, year=year_num).exists():
            data = MonthData.objects.get(month_num=month_num, year=year_num)
            data.miss += miss
            data.overkill += overkill
            data.useless += useless
            data.aoi_in += aoi_in
            data.aoi_ok += aoi_in - aoi_ng
            data.aoi_ng += aoi_ng
            data.fi_in += fi_in
            data.miss_rate = artificial_division(data.miss, data.aoi_in)
            data.overkill_rate = artificial_division(data.overkill, data.aoi_in)
            data.useless_rate = artificial_division(data.useless, data.aoi_in)
            data.miss_aoi = artificial_division(data.miss, (data.fi_in - data.aoi_ng))
            data.overkill_aoi = artificial_division(data.overkill, (data.aoi_ng - data.useless))
            data.save()
        else:
            aoi_ok = aoi_in - aoi_ng
            miss_rate = artificial_division(miss, aoi_in)
            overkill_rate = artificial_division(overkill, aoi_in)
            useless_rate = artificial_division(useless, aoi_in)
            miss_aoi = artificial_division(miss, (fi_in - aoi_ng))
            overkill_aoi = artificial_division(overkill, (aoi_ng - useless))
            data = cls(year=year_num, month_num=month_num, miss=miss, overkill=overkill, useless=useless, aoi_in=aoi_in,
                       aoi_ok=aoi_ok, aoi_ng=aoi_ng, fi_in=fi_in, miss_rate=miss_rate, overkill_rate=overkill_rate,
                       useless_rate=useless_rate, miss_aoi=miss_aoi, overkill_aoi=overkill_aoi)
            data.save()
        return data


class QuarterData(models.Model):
    year = models.IntegerField()
    quarter_num = models.IntegerField(db_column='quarter')
    miss = models.IntegerField()
    overkill = models.IntegerField()
    useless = models.IntegerField()
    aoi_in = models.IntegerField(db_column='AOIIn')
    aoi_ok = models.IntegerField(db_column='AOIOk')
    aoi_ng = models.IntegerField(db_column='AOINg')
    fi_in = models.IntegerField(db_column='FIIn')
    miss_rate = models.FloatField(db_column='MissRate')
    overkill_rate = models.FloatField(db_column='OverkillRate')
    useless_rate = models.FloatField(db_column='UselessRate')
    miss_aoi = models.FloatField(db_column='MissRateAoiOk')
    overkill_aoi = models.FloatField(db_column='OverkillRateAoiNg')

    @classmethod
    def create_quarter(cls, year_num, quarter_num, miss, overkill, useless, aoi_in, aoi_ng, fi_in):
        if QuarterData.objects.filter(quarter_num=quarter_num, year=year_num).exists():
            data = QuarterData.objects.get(quarter_num=quarter_num, year=year_num)
            data.miss += miss
            data.overkill += overkill
            data.useless += useless
            data.aoi_in += aoi_in
            data.aoi_ok += aoi_in - aoi_ng
            data.aoi_ng += aoi_ng
            data.fi_in += fi_in
            data.miss_rate = artificial_division(data.miss, data.aoi_in)
            data.overkill_rate = artificial_division(data.overkill, data.aoi_in)
            data.useless_rate = artificial_division(data.useless, data.aoi_in)
            data.miss_aoi = artificial_division(data.miss, (data.fi_in - data.aoi_ng))
            data.overkill_aoi = artificial_division(data.overkill, (data.aoi_ng - data.useless))
            data.save()
        else:
            aoi_ok = aoi_in - aoi_ng
            miss_rate = artificial_division(miss, aoi_in)
            overkill_rate = artificial_division(overkill, aoi_in)
            useless_rate = artificial_division(useless, aoi_in)
            miss_aoi = artificial_division(miss, (fi_in - aoi_ng))
            overkill_aoi = artificial_division(overkill, (aoi_ng - useless))
            data = cls(year=year_num, quarter_num=quarter_num, miss=miss, overkill=overkill, useless=useless,
                       aoi_in=aoi_in,
                       aoi_ok=aoi_ok, aoi_ng=aoi_ng, fi_in=fi_in, miss_rate=miss_rate, overkill_rate=overkill_rate,
                       useless_rate=useless_rate, miss_aoi=miss_aoi, overkill_aoi=overkill_aoi)
            data.save()
        return data


class YearData(models.Model):
    year = models.IntegerField()
    miss = models.IntegerField()
    overkill = models.IntegerField()
    useless = models.IntegerField()
    aoi_in = models.IntegerField(db_column='AOIIn')
    aoi_ok = models.IntegerField(db_column='AOIOk')
    aoi_ng = models.IntegerField(db_column='AOINg')
    fi_in = models.IntegerField(db_column='FIIn')
    miss_rate = models.FloatField(db_column='MissRate')
    overkill_rate = models.FloatField(db_column='OverkillRate')
    useless_rate = models.FloatField(db_column='UselessRate')
    miss_aoi = models.FloatField(db_column='MissRateAoiOk')
    overkill_aoi = models.FloatField(db_column='OverkillRateAoiNg')

    @classmethod
    def create_year(cls, year_num, miss, overkill, useless, aoi_in, aoi_ng, fi_in):
        if YearData.objects.filter(year=year_num).exists():
            data = YearData.objects.get(year=year_num)
            data.miss += miss
            data.overkill += overkill
            data.useless += useless
            data.aoi_in += aoi_in
            data.aoi_ok += aoi_in - aoi_ng
            data.aoi_ng += aoi_ng
            data.fi_in += fi_in
            data.miss_rate = artificial_division(data.miss, data.aoi_in)
            data.overkill_rate = artificial_division(data.overkill, data.aoi_in)
            data.useless_rate = artificial_division(data.useless, data.aoi_in)
            data.miss_aoi = artificial_division(data.miss, (data.fi_in - data.aoi_ng))
            data.overkill_aoi = artificial_division(data.overkill, (data.aoi_ng - data.useless))
            data.save()
        else:
            aoi_ok = aoi_in - aoi_ng
            miss_rate = artificial_division(miss, aoi_in)
            overkill_rate = artificial_division(overkill, aoi_in)
            useless_rate = artificial_division(useless, aoi_in)
            miss_aoi = artificial_division(miss, (fi_in - aoi_ng))
            overkill_aoi = artificial_division(overkill, (aoi_ng - useless))
            data = cls(year=year_num, miss=miss, overkill=overkill, useless=useless, aoi_in=aoi_in,
                       aoi_ok=aoi_ok, aoi_ng=aoi_ng, fi_in=fi_in, miss_rate=miss_rate, overkill_rate=overkill_rate,
                       useless_rate=useless_rate, miss_aoi=miss_aoi, overkill_aoi=overkill_aoi)
            data.save()
        return data
