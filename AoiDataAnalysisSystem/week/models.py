from django.db import models


class WeekData(models.Model):
    year = models.IntegerField()
    week_num = models.IntegerField(db_column='week')
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
    def create_week(cls, year_num, week_num, miss, overkill, useless, aoi_in, aoi_ng, fi_in):
        def artificial_division(x, y):
            if y == 0:
                return 0
            else:
                return x/y

        if WeekData.objects.filter(week_num=week_num, year=year_num).exists():
            data = WeekData.objects.get(week_num=week_num, year=year_num)
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
            data = cls(year=year_num, week_num=week_num, miss=miss, overkill=overkill, useless=useless, aoi_in=aoi_in,
                       aoi_ok=aoi_ok, aoi_ng=aoi_ng, fi_in=fi_in, miss_rate=miss_rate, overkill_rate=overkill_rate,
                       useless_rate=useless_rate, miss_aoi=miss_aoi, overkill_aoi=overkill_aoi)
            data.save()
        return data