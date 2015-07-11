from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Column, Index, Integer, String, Float, Boolean
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, relationship, backref

import yaml

Base = declarative_base()

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
    base : base object
        Provides a base class for declarative class definitions.
    engine : engine object
        Provides a source of database connectivity and behavior.
    """

    engine = create_engine(connection_string, echo=echo)
    Base = declarative_base(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    return session, Base, engine

with open('../configure.yaml', 'r') as f:
    SETTINGS = yaml.load(f)
session, Base, engine = load_connection(SETTINGS['connection_string'])

#-------------------------------------------------------------------------------

class Files(Base):
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True)
    path = Column(String(70))
    name = Column(String(40))

    Index('idx_fullpath', 'path', 'name', unique=True)

    def __repr__(self):
        return "<Files(path='{}', name='{}')>".format(self.path, self.name)

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
    file = relationship("Files", backref=backref('lampflash', order_by=id))

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
    opt_elem = Column(String(5))
    shutter = Column(String(20))
    extended = Column(String(20))
    obset_id = Column(String(2))
    asn_id = Column(String(9))
    asn_tab = Column(String(18))

    file_id = Column(Integer, ForeignKey('files.id'))
    file = relationship("Files", backref=backref('headers', order_by=id))

    Index('idx_rootname', 'rootname', unique=False)
    Index('idx_config', 'segment', 'fppox', 'cenwave', 'opt_elem', unique=False)

#-------------------------------------------------------------------------------

class Data(Base):
    __tablename__ = "data"

    id = Column(Integer, primary_key=True)

    hvlevela = Column(Integer)
    hvlevelb = Column(Integer)
    date_obs = Column(String(10))
    time_obs = Column(String(8))
    expstart = Column(Float)
    expend = Column(Float)
    exptime = Column(Float)
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

    flux_mean = Column(Float)
    flux_max = Column(Float)
    flux_std = Column(Float)

    file_id = Column(Integer, ForeignKey('files.id'))
    file = relationship("Files", backref=backref('Data', order_by=id))
#-------------------------------------------------------------------------------
