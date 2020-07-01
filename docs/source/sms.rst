SMS Data Ingestion
==================
Some of the monitors in COSMO rely on data that are found in Science Mission Schedule (SMS) reports.
These reports are produced outside of the COS Branch and supplied via *txt* files.

A variety of data on exposures is provided in these report files, and in some cases can actually be used to verify
information in the regular data products themselves.

SMS File Format and Naming Conventions
--------------------------------------
The Report files use a human readable table format, which requires special processing to read since it's not a "typical"
table format (such as *csv*).

The naming convention for these files are comprised of two different ID codes:
 1. A "SMS ID" which uniquely identifies a particular SMS report
 2. A "Version ID" which identifies the version of a particular SMS

The SMS ID is the start date of the SMS in the form "YYDDD", and is the first six digits in the report filename.
The Version ID is typically an incrementing group of two alpha-numeric characters (with some rare exceptions such as for
when an SMS report is re-created, at which point an "r" will be added to the end of the normal Version ID) that occur
after the SMS ID in the filename.
For example, SMS 091403 version "b4" is a more recent version than "a1."

SMS reports can have multiple versions available.
Reports are generated at a particular cadence, and can contain "working" versions of an observation schedule that can
change across versions.
The "largest" version alpha-numerically corresponds to the most up-to-date version of the SMS.
This is the version that should be used for comparisons with COS data products.

.. _sms-database:

SMS Database
------------
Further configuration of the SMS database can be accomplished with the following environment variables (defaults as
comments):

.. code-block::

    COSMO_SMS_DB  # 'sms.db'
    COSMO_SMS_DB_JOURNAL  # 'wal'
    COSMO_SMS_DB_FORIEGN_KEYS  # 1
    COSMO_SMS_DB_IGNORE_CHECK_CONSTRAINTS  # 0
    COSMO_SMS_DB_SYNCHRONOUS  # 0

The first item is the path to the database file, while the remaining options are supported pragma statements.

The SMS Database itself is comprised of two tables, ``SMSFileStats`` and ``SMSTable``.

The SMSFileStats table records information about the report files themselves including the SMS ID, Version ID, date of
ingestion and a "FILEID," which is the combination of the SMS ID and Version ID and is used as a reference in SMSTable.

SMSTable contains the data of the SMS reports.
Each record is identified uniquely based on an exposure key that is generated out of a handful of columns from the
report itself that identifies an exposure (the ROOTNAME cannot be used as this may actually change across SMS versions).
These records are linked to a particular SMS report in the SMSFileStats table.

SMSFileStats and SMSTable are both ``peewee.Model`` objects that can be used to query SMS data from the database.

For example:

.. code-block:: python

    import pandas as pd
    from cosmo.sms import SMSFileStats, SMSTable

    # select all sms records
    query = SMSFileStats.select()  # Returns a query object

     # Put the data in a pandas DataFrame
    df = pd.DataFrame(list(query.dicts()))

    # Show data records from a single SMS report using the SMSID
    report = SMSFileStats.get(SMSFileStats.SMSID == '123456')

    # Or
    report = SMSFileStats.get('123456')  # The SMSID is the primary key for SMSFileStats, so this syntax also works

    # Or
    report = SMSFileStats['123456']

    report.exposures  # returns a peewee.Query object

    list(report.exposures.dicts())  # returns a list of dictionaries of the table data

    # Query the SMS Data
    all_data = SMSTable.select()

    # Query and filter SMS Data
    fuv_data = SMSTable.select().where(SMSTable.DETECTOR == 'FUV')

SMS Ingestion Rules
-------------------
SMS reports are ingested into the local SMS database, ``sms.db`` using a set of rules to ensure that the correct
information is stored.

Ingestion rules are as follows:
 - If multiple versions of the same SMS are available, the most recent version is used (largest alpha-numeric Version ID).
 - If a new version of an SMS is found with a larger Version ID, that new report will supersede the current entry for
   that particular SMS.
 - If data (identified by the exposure key) from one SMS is also found in another SMS, the data will be ingested from
   the SMS report with the larger SMS ID (and therefore more recent), and will replace any existing matching records.

.. note::

    These rules ensure that the data in SMSTable is accurate, but can also result in "stale" reports in SMSFileStats if
    data occurs in a more recent SMS report.

    It's also possible to have SMSTable records that don't actually correspond to any COS dataset.
    This is due to the fact that the SMS reports reflect the observation schedule, which can change a number of times
    right up to the planned observation date.
    As this happens, ROOTNAME and other exposure information is updated accordingly, and since the ROOTNAME is based on
    an incrementing naming system, exposures can be assigned several ROOTNAME until the "final" schedule is established.
    Additionally, if an exposure does not execute (for example, when COS enters safe mode), those exposures will still
    appear in the SMS.

Finding and Ingesting SMS Report Files
--------------------------------------
Reading and ingesting the data of the SMS Report files is done by the ``SMSFile`` class.
SMSFile reads and ingests data from the *txt* files using a series of regular expressions (due to the irregular, human-
readable format) and creates a ``pandas.DataFrame`` to contain the data.

SMSFile can also ingest the data into the SMS database with the ``ingest_smsfile`` method.

.. note::

    If the file already exists in the database, the file will *not* be ingested.
    Additionally, the file will be ingested according to the rules described above, and will be ingested (or not)
    accordingly.

The ``SMSFinder`` class can be used for locating the most recent versions of any SMS report found in a given directory.
SMSFinder also classifies the reports that it finds as "new" (not currently in the database) or "old" (currently in the
database), and can ingest all "new" reports into the database.
