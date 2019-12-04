# Generated by Django 2.2.7 on 2019-12-04 07:38

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='WeekData',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.IntegerField()),
                ('week_num', models.IntegerField(db_column='week')),
                ('miss', models.IntegerField()),
                ('overkill', models.IntegerField()),
                ('useless', models.IntegerField()),
                ('aoi_in', models.IntegerField(db_column='AOIIn')),
                ('aoi_ok', models.IntegerField(db_column='AOIOk')),
                ('aoi_ng', models.IntegerField(db_column='AOINg')),
                ('fi_in', models.IntegerField(db_column='FIIn')),
                ('miss_rate', models.FloatField(db_column='MissRate')),
                ('overkill_rate', models.FloatField(db_column='OverkillRate')),
                ('useless_rate', models.FloatField(db_column='UselessRate')),
                ('miss_aoi', models.FloatField(db_column='MissRateAoiOk')),
                ('overkill_aoi', models.FloatField(db_column='OverkillRateAoiNg')),
            ],
        ),
    ]
