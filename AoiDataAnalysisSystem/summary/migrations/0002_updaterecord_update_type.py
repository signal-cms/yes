# Generated by Django 2.2.7 on 2019-12-02 06:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('summary', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='updaterecord',
            name='update_type',
            field=models.CharField(db_column='UpdateTable', max_length=20, null=True),
        ),
    ]
