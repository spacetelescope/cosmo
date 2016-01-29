from __future__ import print_function, absolute_import, division

import os

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Column, Index, Integer, String, Float, Boolean, Numeric
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, relationship, backref

try:
    import yaml
except ImportError:
    from .yaml import yaml

__all__ = ['open_settings', 'load_connection']

Base = declarative_base()

#-------------------------------------------------------------------------------

def open_settings(config_file=None):
    """ Parse config file and load settings

    If no config file is supplied, the configuration file will assume to be
    located at '~/configure.yaml'.

    Parameters
    ----------
    config_file : str, optional
        yaml file containing configuration settings.

    Returns
    -------
    settings : dict
        dictionary of all settings

    """

    config_file = config_file or os.path.join(os.environ['HOME'], "configure.yaml")

    with open(config_file, 'r') as f:
        settings = yaml.load(f)

    return settings

#-------------------------------------------------------------------------------

def load_connection(connection_string, echo=False):
    """Create and return a connection to the database given in the
    connection string.

    Parameters
    ----------
    connection_string : str
        A string that points to the database conenction.  The
        connection string is in the following form:
        dialect+driver://username:password@host:port/database
    echo : bool
        Show all SQL produced.

    Returns
    -------
    session : sesson object
        Provides a holding zone for all objects loaded or associated
        with the database.
    engine : engine object
        Provides a source of database connectivity and behavior.
    """

    engine = create_engine(connection_string, echo=echo)
    Session = sessionmaker(bind=engine)

    return Session, engine

#-------------------------------------------------------------------------------

class Darks(Base):
    __tablename__ = "darks"

    id = Column(Integer, primary_key=True)

    obsname = Column(String(30))
    detector = Column(String(4))
    date = Column(Numeric(7, 2))
    dark = Column(Numeric(12, 10))
    ta_dark = Column(Numeric(12, 10))
    latitude = Column(Numeric(8, 3))
    longitude = Column(Numeric(8, 3))
    sun_lat = Column(Numeric(8, 3))
    sun_lon = Column(Numeric(8, 3))
    temp = Column(Numeric(8, 4))

    file_id = Column(Integer, ForeignKey('files.id'))
    #file = relationship("Files", backref=backref('lampflash', order_by=id))

#-------------------------------------------------------------------------------

class Files(Base):
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True)

    path = Column(String(70))
    name = Column(String(40))
    rootname = Column(String(9))

    __table_args__ = (Index('idx_fullpath', 'path', 'name', unique=True), )
    __table_args__ = (Index('idx_rootname', 'rootname'), )

#-------------------------------------------------------------------------------

class Lampflash(Base):
    __tablename__ = 'lampflash'

    id = Column(Integer, primary_key=True)
    date = Column(Float)
    proposid = Column(Integer)
    detector = Column(String(4))
    opt_elem = Column(String(5))
    cenwave = Column(Integer)
    fppos = Column(Integer)
    lamptab = Column(String(30))
    flash = Column(Integer)
    x_shift = Column(Float)
    y_shift = Column(Float)
    found = Column(Boolean)

    file_id = Column(Integer, ForeignKey('files.id'))
    #file = relationship("Files", backref=backref('lampflash', order_by=id))

#-------------------------------------------------------------------------------

class Headers(Base):
    __tablename__ = "headers"

    id = Column(Integer, primary_key=True)
    filetype = Column(String(67))
    instrume = Column(String(3))
    rootname = Column(String(9))
    imagetyp = Column(String(20))
    targname = Column(String(67))
    ra_targ = Column(Float(20))
    dec_targ = Column(Float(20))
    proposid = Column(Integer)
    qualcom1 = Column(String(67))
    qualcom2 = Column(String(67))
    qualcom3 = Column(String(67))
    quality = Column(String(67))
    opus_ver = Column(String(30))
    cal_ver = Column(String(30))

    obstype = Column(String(20))
    obsmode = Column(String(20))
    exptype = Column(String(20))
    detector = Column(String(20))
    segment = Column(String(20))
    detecthv = Column(String(20))
    life_adj = Column(Integer)
    fppos = Column(Integer)
    exp_num = Column(Integer)
    cenwave = Column(Integer)
    aperture = Column(String(3))
    opt_elem = Column(String(6))
    shutter = Column(String(20))
    extended = Column(String(20))
    obset_id = Column(String(2))
    asn_id = Column(String(9))
    asn_tab = Column(String(18))
    ###randseed = Column(Integer) #-- Errors for some reason.
    hvlevela = Column(Integer)
    hvlevelb = Column(Integer)
    dpixel1a = Column(Float)
    dpixel1b = Column(Float)
    date_obs = Column(String(10))
    time_obs = Column(String(8))
    expstart = Column(Numeric(8, 3))
    expend = Column(Numeric(8, 3))
    exptime = Column(Float)
    numflash = Column(Integer)
    ra_aper = Column(Float)
    dec_aper = Column(Float)
    shift1a = Column(Float)
    shift2a = Column(Float)
    shift1b = Column(Float)
    shift2b = Column(Float)
    shift1c = Column(Float)
    shift2c = Column(Float)

    sp_loc_a = Column(Float)
    sp_loc_b = Column(Float)
    sp_loc_c = Column(Float)
    sp_nom_a = Column(Float)
    sp_nom_b = Column(Float)
    sp_nom_c = Column(Float)
    sp_off_a = Column(Float)
    sp_off_b = Column(Float)
    sp_off_c = Column(Float)
    sp_err_a = Column(Float)
    sp_err_b = Column(Float)
    sp_err_c = Column(Float)

    file_id = Column(Integer, ForeignKey('files.id'))
    #file = relationship("Files", backref=backref('headers', order_by=id))

    __table_args__ = (Index('idx_rootname', 'rootname', unique=False), )
    __table_args__ = (Index('idx_config', 'segment', 'fppos', 'cenwave', 'opt_elem', unique=False), )

#-------------------------------------------------------------------------------
"""
class Data(Base):
    __tablename__ = "data"

    id = Column(Integer, primary_key=True)

    flux_mean = Column(Float)
    flux_max = Column(Float)
    flux_std = Column(Float)

    file_id = Column(Integer, ForeignKey('files.id'))
    #file = relationship("Files", backref=backref('Data', order_by=id))
"""
#-------------------------------------------------------------------------------

class Stims(Base):
    """Record location of all STIM pulses"""
    __tablename__ = "stims"

    id = Column(Integer, primary_key=True)

    time = Column(Float)
    abs_time = Column(Numeric(10, 5))
    stim1_x = Column(Numeric(8, 3))
    stim1_y = Column(Numeric(8, 3))
    stim2_x = Column(Numeric(8, 3))
    stim2_y = Column(Numeric(8, 3))
    counts = Column(Float)

    file_id = Column(Integer, ForeignKey('files.id'))

    #__table_args__ = (Index('idx_dataset', 'rootname', unique=False), )
    #file = relationship("Files", backref=backref('Stims', order_by=id))

#-------------------------------------------------------------------------------

class Phd(Base):
    __tablename__ = 'phd'

    id = Column(Integer, primary_key=True)

    pha_0 = Column(Integer)
    pha_1 = Column(Integer)
    pha_2 = Column(Integer)
    pha_3 = Column(Integer)
    pha_4 = Column(Integer)
    pha_5 = Column(Integer)
    pha_6 = Column(Integer)
    pha_7 = Column(Integer)
    pha_8 = Column(Integer)
    pha_9 = Column(Integer)
    pha_10 = Column(Integer)
    pha_11 = Column(Integer)
    pha_12 = Column(Integer)
    pha_13 = Column(Integer)
    pha_14 = Column(Integer)
    pha_15 = Column(Integer)
    pha_16 = Column(Integer)
    pha_17 = Column(Integer)
    pha_18 = Column(Integer)
    pha_19 = Column(Integer)
    pha_20 = Column(Integer)
    pha_21 = Column(Integer)
    pha_22 = Column(Integer)
    pha_23 = Column(Integer)
    pha_24 = Column(Integer)
    pha_25 = Column(Integer)
    pha_26 = Column(Integer)
    pha_27 = Column(Integer)
    pha_28 = Column(Integer)
    pha_29 = Column(Integer)
    pha_30 = Column(Integer)
    pha_31 = Column(Integer)

    file_id = Column(Integer, ForeignKey('files.id'))
    #file = relationship("Files", backref=backref('Phd', order_by=id))

#-------------------------------------------------------------------------------
'''
class Gain(Base):
    __tablename__ = 'gain'

    id = Column(Integer, primary_key=True)

    x = Column(Float)
    y = Column(Float)
    xbin = Column(Float)
    ybin = Column(Float)
    gain = Column(Float)
    counts = Column(Float)
    sigma = Column(Float)

    file_id = Column(Integer, ForeignKey('files.id'))
    #file = relationship("Files", backref=backref('Gain', order_by=id))
'''
#-------------------------------------------------------------------------------
