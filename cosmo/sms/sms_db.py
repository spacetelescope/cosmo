from peewee import Model, TextField, IntegerField, FloatField, DateTimeField, ForeignKeyField
from playhouse.sqlite_ext import SqliteExtDatabase

from .. import SETTINGS

DB = SqliteExtDatabase(**SETTINGS['sms']['db_settings'])


class BaseModel(Model):

    class Meta:
        database = DB


class SMSFileStats(BaseModel):
    filename = TextField(primary_key=True)
    ingest_date = DateTimeField()


class SMSTable(BaseModel):

    rootname = TextField()
    filename = ForeignKeyField(SMSFileStats, backref='exposures')
    proposid = IntegerField(verbose_name='proposal id')
    detector = TextField()
    opmode = TextField()
    exptime = FloatField()
    expstart = DateTimeField()
    fuvhvstate = TextField(verbose_name='fuv hv state')
    aperture = TextField()
    osm1pos = TextField(verbose_name='OSM1 position')
    osm2pos = TextField(verbose_name='OSM2 position')
    cenwave = IntegerField()
    fppos = IntegerField()
    tsinceosm1 = FloatField(verbose_name='time since OSM1 move')
    tsinceosm2 = FloatField(verbose_name='time since OSM2 move')
