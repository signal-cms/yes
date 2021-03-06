# Generated by Django 2.2.7 on 2019-11-28 01:48

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Detail',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('panel_id', models.CharField(db_column='PanelID', max_length=20)),
                ('line_id', models.CharField(db_column='LineID', max_length=6)),
                ('product_id', models.CharField(db_column='ProductID', max_length=20)),
                ('rst_date', models.DateField(db_column='Date')),
                ('aoi_result', models.BooleanField(db_column='AOIResult')),
                ('aoi_code', models.CharField(db_column='AOIReasonCode', max_length=100, null=True)),
                ('aoi_address', models.IntegerField(db_column='AOIAddress')),
                ('aoi_time', models.DateTimeField(db_column='AOITime')),
                ('op_id', models.CharField(db_column='OperatorID', max_length=100, null=True)),
                ('fi_result', models.NullBooleanField(db_column='FIResult')),
                ('fi_code', models.CharField(db_column='FIReasonCode', max_length=100, null=True)),
                ('fi_address', models.IntegerField(db_column='FIAddress')),
                ('fi_time', models.DateTimeField(db_column='FITime', null=True)),
                ('is_miss', models.NullBooleanField(db_column='isMiss')),
                ('is_overkill', models.NullBooleanField(db_column='isOverkill')),
                ('is_useless', models.BooleanField(db_column='isUselessData')),
                ('is_checkout', models.NullBooleanField(db_column='isAOICheckOut')),
                ('is_delete', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='UpdateRecord',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('update_time', models.DateTimeField(auto_created=True, db_column='time')),
                ('col_num', models.IntegerField(db_column='record')),
            ],
        ),
    ]
