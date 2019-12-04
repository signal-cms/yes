from django.test import TestCase
from .models import DayData
from datetime import date


class DailyModelTests(TestCase):

    # Test create method
    def test_create_day_normal(self):
        data = DayData.create_day(date(2019, 11, 30), 10, 1500, 50, 10000, 2000, 3000)
        self.assertEqual(data.year, 2019)
        self.assertEqual(data.rst_date, date(2019, 11, 30))
        self.assertEqual(data.miss, 10)
        self.assertEqual(data.miss_rate, 10/10000)
        self.assertEqual(data.miss_aoi, 10/1000)
        self.assertEqual(data.overkill, 1500)
        self.assertEqual(data.overkill_rate, 1500/10000)
        self.assertEqual(data.overkill_aoi, 1500/1950)
        self.assertEqual(data.useless, 50)
        self.assertEqual(data.useless_rate, 50/10000)
        self.assertEqual(data.aoi_in, 10000)
        self.assertEqual(data.aoi_ng, 2000)
        self.assertEqual(data.aoi_ok, 8000)
        self.assertEqual(data.fi_in, 3000)

    def test_create_day_exists(self):
        DayData.create_day(date(2019, 11, 30), 10, 1500, 50, 10000, 2000, 3000)
        data = DayData.create_day(date(2019, 11, 30), 10, 1500, 50, 10000, 2000, 3000)
        self.assertEqual(data.year, 2019)
        self.assertEqual(data.rst_date, date(2019, 11, 30))
        self.assertEqual(data.miss, 20)
        self.assertEqual(data.miss_rate, 20 / 20000)
        self.assertEqual(data.miss_aoi, 20 / 2000)
        self.assertEqual(data.overkill, 3000)
        self.assertEqual(data.overkill_rate, 3000 / 20000)
        self.assertEqual(data.overkill_aoi, 3000 / 3900)
        self.assertEqual(data.useless, 100)
        self.assertEqual(data.useless_rate, 100 / 20000)
        self.assertEqual(data.aoi_in, 20000)
        self.assertEqual(data.aoi_ng, 4000)
        self.assertEqual(data.aoi_ok, 16000)
        self.assertEqual(data.fi_in, 6000)

    def test_create_day_division_zero(self):
        data = DayData.create_day(date(2019, 11, 30), 20, 1500, 50, 10000, 3000, 3000)
        self.assertEqual(data.miss_aoi, 0)