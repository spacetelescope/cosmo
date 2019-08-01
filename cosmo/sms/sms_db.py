from peewee import Model, TextField, IntegerField, FloatField, DateTimeField, ForeignKeyField
from playhouse.sqlite_ext import SqliteExtDatabase

from .. import SETTINGS

DB = SqliteExtDatabase(**SETTINGS['sms']['db_settings'])


class BaseModel(Model):

    class Meta:
        database = DB


class SMSFileStats(BaseModel):
    FILEID = TextField(primary_key=True)
    VERSION = TextField()
    FILENAME = TextField()
    INGEST_DATE = DateTimeField()


class SMSTable(BaseModel):

    ROOTNAME = TextField(primary_key=True)
    FILEID = ForeignKeyField(SMSFileStats, backref='exposures', on_delete='cascade')
    PROPOSID = IntegerField(verbose_name='proposal id')
    DETECTOR = TextField()
    OPMODE = TextField()
    EXPTIME = FloatField()
    EXPSTART = DateTimeField()
    FUVHVSTATE = TextField(verbose_name='fuv hv state')
    APERTURE = TextField()
    OSM1POS = TextField(verbose_name='OSM1 position')
    OSM2POS = TextField(verbose_name='OSM2 position')
    CENWAVE = IntegerField()
    FPPOS = IntegerField()
    TSINCEOSM1 = FloatField(verbose_name='time since OSM1 move')
    TSINCEOSM2 = FloatField(verbose_name='time since OSM2 move')
