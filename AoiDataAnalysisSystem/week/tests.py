from django.test import TestCase
from .models import WeekData


class WeekModelTests(TestCase):

    def test_create_week_exists(self):
        data = WeekData.create_week(2019, 3, 10, 1500, 50, 10000, 2000, 3000)
        self.assertEqual(data.year, 2019)
        self.assertEqual(data.week_num, 3)
        self.assertEqual(data.miss, 10)
        self.assertEqual(data.miss_rate, 20 / 20000)
        self.assertEqual(data.miss_aoi, 20 / 2000)
        self.assertEqual(data.overkill, 1500)
        self.assertEqual(data.overkill_rate, 3000 / 20000)
        self.assertEqual(data.overkill_aoi, 3000 / 3900)
        self.assertEqual(data.useless, 50)
        self.assertEqual(data.useless_rate, 100 / 20000)
        self.assertEqual(data.aoi_in, 10000)
        self.assertEqual(data.aoi_ng, 2000)
        self.assertEqual(data.aoi_ok, 8000)
        self.assertEqual(data.fi_in, 3000)
        data = WeekData.create_week(2019, 3, 10, 1500, 50, 10000, 2000, 3000)
        self.assertEqual(data.year, 2019)
        self.assertEqual(data.week_num, 3)
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
