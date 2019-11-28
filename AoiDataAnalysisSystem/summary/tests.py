from django.test import TestCase
from .models import Detail
from datetime import date, datetime
from django.utils import timezone

# Create your tests here.


class DetailModelTests(TestCase):

    # There is five test for create_detail function normal
    # pass means that fi_result is null
    def test_create_detail_fi_pass(self):
        d = Detail.create_detail('testpanelid', 'linets', 'TS550-T00-TEST', '', 0, '2019112710')
        self.assertEqual(d.panel_id, 'testpanelid')
        self.assertEqual(d.line_id, 'linets')
        self.assertEqual(d.product_id, 'TS550-T00-TEST')
        self.assertEqual(d.rst_date, date(2019, 11, 27))
        self.assertEqual(d.aoi_result, True)
        self.assertEqual(d.aoi_code, '')
        self.assertEqual(d.aoi_address, 0)
        self.assertEqual(d.aoi_time, timezone.make_aware(datetime(2019, 11, 27, 10)))
        self.assertEqual(d.fi_result, '')
        self.assertEqual(d.fi_code, '')
        self.assertEqual(d.fi_address, 0)
        self.assertEqual(d.fi_time, timezone.make_aware(datetime(2019, 11, 27, 10)))
        self.assertEqual(d.is_miss, '')
        self.assertEqual(d.is_checkout, '')
        self.assertEqual(d.is_overkill, '')
        self.assertEqual(d.is_useless, False)
        self.assertEqual(d.is_delete, False)

    def test_create_detail_miss(self):
        d = Detail.create_detail('testpanelid', 'linets', 'TS550-T00-TEST', '', '', '2019112710', 'opid', 'MBD01', 8,
                                 '2019112710')
        self.assertEqual(d.is_miss, True)
        self.assertEqual(d.is_overkill, '')
        self.assertEqual(d.is_checkout, False)

    def test_create_detail_overkil(self):
        d = Detail.create_detail('testpanelid', 'linets', 'TS550-T00-TEST', 'MBD01', 9, '2019112710', 'opid', '', 0,
                                 '2019112710')
        self.assertEqual(d.is_miss, '')
        self.assertEqual(d.is_overkill, True)
        self.assertEqual(d.is_checkout, '')

    def test_create_detail_useless(self):
        useless_list = ['异常点灯', '功能其他', '亮度异常', 'MFC02', 'MFC11', 'MBD05', 'MBD13']
        for code in useless_list:
            d = Detail.create_detail('testpanelid', 'linets', 'TS550-T00-TEST', code, 5, '2019112710')
            try:
                self.assertEqual(d.is_useless, True)
            except AssertionError:
                print('the error useless code is {0}'.format(code))
                raise

    def test_create_detail_is_delete(self):
        d = Detail.create_detail('testpanelid', 'linets', 'TS550-T00-TEST', '', '', '2019112710', 'opid', 'MBD01', 8,
                                 '2019112710', is_delete=True)
        self.assertQuerysetEqual(Detail.dtlobj.all(), [])
