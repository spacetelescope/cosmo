API
===
Monitors and their DataModels
-----------------------------
Here, we give a brief description of the monitor and DataModel API.
For more detailed information on the ``monitorframe`` framework and the Monitor and DataModel objects, please see that
`project's documentation <https://spacetelescope.github.io/monitor-framework/?>`_.

Since COSMO is built on the ``monitorframe`` framework, all monitors are objects that inherit the ``BaseMonitor``
object, and all DataModels are objects that inherit the ``BaseDataModel`` object.

Each monitor must be defined with a corresponding DataModel.
The DataModel represents the monitor's interface with the data needed to perform monitoring; it defines how new data for
the monitor is acquired and how that data is then defined and stored in the database.
The Monitor utilizes the database to access the necessary data, perform any filtering, perform the necessary analysis,
and produce outputs (typically plots).

The examples described below include assumptions about the existing implementation with respect to the configuration
file.

DataModels
^^^^^^^^^^
The DataModels usage and API will be described in terms of an example DataModel:

.. code-block:: python

    from monitorframe.datamodel import BaseDataModel

    class DataModel(BaseDataModel):
        files_source = FILES_SOURCE  # The source of the COS data files is defined in the configuration file
        cosmo_layout = True

        primary_key = 'SomeKeyWord'

        def get_new_data(self):
            ...  # This method depends on what data is needed

.. py:class:: DataModel(find_new=True)

    :param bool find_new: Default is ``True``. If ``True``, ``get_new_data`` will be executed during init.

    .. note::

        If a database does not exist or if the table representation of the DataModel data does not exist, executing the
        ``ingest`` method will create them.

    Example Usage:

    .. code-block:: python

        import pandas as pd

        # Using the defined example model above...
        # Create a model instance
        model = DataModel()  # by default this will attempt to find new data

        # Create an instance without finding new data
        model = DataModel(find_new=False)

        # Ingest data into a corresponding database table; Also creates a SQLite database and corresponding table if
        #  they don't exist.
        model.ingest()

        # Perform a query (assumes that the database table "DataModel" exists)
        query = model.model.select()  # Grab everything from this particular table

        # Convert a query to a dataframe
        df = pd.DataFrame(model.model.select().dicts())

        # If there are columns with array elements
        df = model.query_to_pandas(query)

    .. py:attribute:: files_source

        :type: ``str``

        Path to COS data cache.
        This attribute is used in the ``get_new_data`` method to find COS data files, and is set with a configuration
        file

    .. py:attribute:: cosmo_layout

        :type: ``bool``

        Used for determining the how to find files.
        If set to True, the files will be assumed to be organized by COSMO (i.e. by program ID).
        Otherwise, it is assumed that the data files are located in the files_source directory with no subdirectories.

    .. py:attribute:: model

        :type: ``None`` until a corresponding table exists in the database, then ``peewee.Model``

        This attribute can be used to execute queries on the corresponding table.

    .. py:attribute:: new_data

        :type: ``pandas.DataFrame``

        new data as defined by ``get_new_data``.

    .. py:method:: get_new_data

        Method that determines how new data is found and sets the ``new_data`` attribute.

        This method is always wrapped by the ``monitorframe`` framework to produce a pandas ``DataFrame``, and so any
        new data must be in column-wise (a dictionary of lists) or row-wise (a list of dictionaries) format.

        :return: A dataframe of new data
        :rtype: ``pandas.DataFrame``

    .. py:method:: ingest

            Ingest the ``new_data`` DataFrame into the database.

            If the ``primary_key`` attribute is set, that key will be used as the primary key for the table.

    .. py:method:: query_to_pandas(query, array_cols=None, array_dtypes=None)

            Execute a given query and return the result as a pandas ``DataFrame``.
            If there are columns with array elements, convert those elements from the string representation used in
            storing back to the correct type.

            :param peewee.ModelSelect query: query object from ``DataModel.model``.
            :param list array_cols: Optional. If not given, the array columns will be inferred from ``new_data``.
            :param list array_dtypes: Optional. If not given, and array columns are detected, then ``float`` is assumed.

Monitors
^^^^^^^^
Relevant information for the monitors' API will be described in terms of an example monitor that
we'll call "Monitor" and the example DataModel object that was described above.
The monitor class that will be used as an example looks like this:

.. code-block:: python

    from monitorframe.monitor import BaseMonitor


    class Monitor(BaseMonitor):
        name = "Monitor"
        data_model = DataModel  # Same example as described above
        labels = ['Some', 'List', 'Of', 'Header', 'Keywords']
        output = COS_MONITORING  # Typically, the output path is given via the configuration file
        notification_settings = {'active': True, 'username': 'user', 'recipients': ['user2', 'user3']}

        def get_data(self):
            ...  # May include filtering, mixing of old and new data, etc

        def track(self):
            ...  # What quantity or quantities the monitor calculates or keeps track of

        def plot(self):
            ...  # Produce an output plot

        def set_notification(self):
            ... # Define a string that will be used in an email notification (if active)

        def store_results(self):
            ...  # What and how results are stored.

.. note::

    ``monitorframe`` provides some built-in basic plotting and results storage.
    To use the basic plotting, an ``x`` and a ``y`` (with an optional color dimension, ``z``) attribute must be set in
    the definition of the new monitor.

    Most of the monitors in COSMO require plots too complex to take advantage of this feature, and so the example here
    uses a more representative signature. For more information on the basic plotting functionality, see the
    `monitorframe documentation <https://spacetelescope.github.io/monitor-framework/?>`_.

.. py:class:: Monitor

    All COSMO monitors will have this signature.

    In some cases, such as for the ACQ/PEAKD and ACQ/PEAKXD monitors, the monitors are similar enough to warrant the
    creation of an additional, partial implementation layer to avoid duplicate code, in which case the top most layer
    may be an even simpler signature than the example above (as several attributes or methods may be set or implemented
    respectively in the partial implementation). In the case of the PEAKD and PEAKXD monitors, the shared layer is
    ``SpecAcqBaseMonitor``.

    Additionally, it is sometimes useful to store information in the new Monitor class itself for use in the monitoring
    methods.
    Again, an example of this can be found in the shared "base layer" of the spectroscopic acquisition monitors,
    ``SpecAcqBaseMonitor``

    Example Usage:

    .. code-block:: python

        import Monitor

        # Create a new instance of the monitor
        monitor = Monitor()

        # Run the monitor
        monitor.monitor()

        # Access outliers (if find_outliers is defined and returns a mask as per COSMO convention)
        outliers = monitor.data[monitor.outliers]

    .. py:attribute:: name

        :type: ``str``

        Optional.
        If this attribute is not set for the Monitor class upon definition, then the name will be derived from the
        object's classname.

    .. py:attribute:: data_model

        :type: ``DataModel``

        Required.
        At the definition of the Monitor, a DataModel object must be assigned.
        The monitor utilizes the DataModel object to access data.

    .. py:attribute:: labels

        :type: ``list``

        Optional.
        List of keywords (that must be included in the data available) to be used in the hover labels in the plots.
        A ``hover_text`` column is added to the monitor ``data`` attribute based on these keys and can be accessed like
        any other column in the ``DataFrame``.

    .. py:attribute:: output

        :tye: ``str``

        Optional.
        Either a directory or a full file path to use for the output.
        If not given, the current directory will be used, and a filename will be created with the form
        "monitor_yyyy_mm_dd."

    .. py:attribute:: model

        :type: ``DataModel``

        Instance of the supplied DataModel from the ``data_model`` attribute.

    .. py:attribute:: data

        :type: ``pandas.DataFrame``

        Monitor data that was defined by the DataModel.

    .. py:attribute:: results

        :type: Any

        Results from the ``track`` method

    .. py:attribute:: outliers

        :type: Any

        Results from the ``find_outliers`` method.

    .. py:attribute:: figure

        :type: ``plotly.graph_objects.Figure``

        Plotly figure used for output plots.

    .. py:attribute:: docs

        :type: ``str``

        Link to the corresponding monitor's documentation page.
        This attribute is not set by default, but is useful to include in the monitor definitions.

    .. py:attribute:: date

        :type: ``datetime.datetime``

        Datetime when the monitor instance was created.
        This date is used throughout the monitoring process (figures, filenames, etc).

    .. py:method:: get_data

        Get data from the DataModel for use in the monitor.

        :return: data
        :rtype: ``pandas.DataFrame``

    .. py:method:: track

        Return a specific value or perform analysis on data to track through time.

        :return: Results from analysis
        :rtype: Any

    .. py:method:: find_outliers

        Optional.
        Define outliers in the data.

        :return: Typically a mask (or masks) for ``data`` that describe the outliers, or ``Any``
        :rtype: Any

    .. py:method:: plot

        Create traces and update ``figure``.

        :return: None

    .. py:method:: initialize_data

        Set the ``data`` attribute based on how ``get_data`` was defined and create hover labels

        :return: None

    .. py:method:: run_analysis

        Set the ``results``, ``outliers``, and ``notification`` attributes via executing
        ``track``, ``find_outliers``, and ``set_notification`` respectively.

        :return: None

        .. note::

            Order matters! If steps of the monitoring process are run individually, they must be run in the correct
            order.
            For example, if ``Monitor.find_outliers`` is called before ``Monitor.initialize_data``, an error will be
            raised since the ``data`` attribute was not set.

    .. py:method:: write_figure

        Write the output figure to an html file using the ``output`` directory and/or name provided.

        :return: None

    .. py:method:: store_results

        Store the results.
        By default, ``monitorframe`` is set up to create and use a "results" database.
        However, to use the default method and the database, the ``format_results`` method may be required as the
        ``monitorframe`` results database will attempt to store results as a ``json`` field (and so the data needs to be
        ``json``-friendly). See
        `this <https://spacetelescope.github.io/monitor-framework/advanced_monitors.html#database>`_ for more
        information.

        :return: None

    .. py:method:: set_notification

        Defines the notification string to be used in the notification email.

        :return: notification string
        :rtype: ``str``
        :raises NotImplementedError: If the ``notification_settings`` attribute is set with "active": ``True`` and
            the new monitor does not define this method.

    .. py:method:: monitor

        Executes all monitoring steps

        :return: None

SMS File Ingestion and Support
------------------------------
Here we describe basic use of the ``sms`` subpackage.

.. py:currentmodule:: ingest_sms

.. py:class:: SMSFile(smsfile)

    Class used for reading in, exploring, and ingesting SMS data from an SMS file.

    :param str smsfile: ``.txt`` or ``.l-exp`` file to ingest.

    Example Usage:

    .. code-block:: python

        from cosmo.sms import SMSFile

        smsfile = 'path/to/some/181137b4.txt'  # Ingestion also works for the .l-exp file extension

        sms = SMSFile(smsfile)  # Ingest the file

        sms.file_id
        # '181137b4'

        sms.sms_id
        # '181137'

        sms.version
        # 'b4'

        sms.data  # pandas DataFrame of the ingested data

        # Construct a new record out of the ingested file and insert into the database
        sms.insert_to_db()

    .. py:attribute:: datetime_format

        :type: ``str``

        Format for the date and time to use in the INGEST_DATE column.

    .. py:attribute:: filename

        :type: ``str``

        Path of the file to be ingested.

    .. py:attribute:: file_id

        :type: ``str``

        The "complete" ID of the SMS file being ingested.
        Includes the SMS ID and the version.
        Typically this is the file name of the SMS file.

    .. py:attribute:: sms_id

        :type: ``str``

        ID of the SMS report.
        Typically the first 6 digits of the SMS file name.

    .. py:attribute:: version

        :type: ``str``

        Version of the SMS report.
        Typically the last 2 characters following the SMS ID in the file name (with exceptions for special cases).

    .. py:attribute:: ingest_date

        :type: ``datetime.datetime``

        Date that the file was ingested (date of the creation of the SMSFile instance).

    .. py:attribute:: data

        :type: ``pandas.DataFrame``

        Ingested data from the SMS file.

    .. py:method:: ingest_smsfile

        Read the input SMS text file and ingest data from the string.

        :return: Ingested data
        :rtype: ``dict``

    .. py:method:: insert_to_db

        Create a new record for the SMS file and insert into the SMSFileStats table.
        Creates new records for each row ingested from the SMS file and inserts into the SMSTable table.

        .. note::

            This methods follows the SMS version and ingestion rules outlined in the SMS section.
            If you try to insert an SMS file that is already in the table(s), nothing will happen.

.. py:class:: SMSFinder

        Class for finding SMS files in a given directory and determining which of those found are already ingested in
        the database.
        Of the SMS files that exist in the directory, only the highest version is returned for each unique SMS ID.

        Example Usage:

        .. code-block:: python

            from cosmo.sms import SMSFinder

            finder = SMSFinder()  # Default files location is set in the configuration file

            finder.all_sms   # DataFrame with all SMS files found (of highest version)

            # See "old" SMS files
            finder.old_sms

            # See "new" SMS files
            finder.new_sms

            # Ingest new files into the database
            finder.ingest_files()

        .. py:attribute:: currently_ingested

            :type: None if no data is ingested or if the SMSFileStats table doesn't exist, else ``pandas.DataFrame``

            All files that exist in the SMSFileStats table.

        .. py:attribute:: all_sms

            :type: ``pandas.DataFrame``

            All SMS files found in the target directory regardless of whether or not they exist in the database.

        .. py:attribute:: new_sms

            :type: ``pandas.DataFrame``

            Property that returns only the files that were classified as "new."

        .. py:attribute:: old_sms

            :type: ``pandas.DataFrame``

            Property that returns only the files that were classified as "old."

        .. py:method:: find_all

            Find all SMS files from the source directory.
            Determine if the file is "new" or "old."

            :return: ``DataFrame`` of found files with "version," "sms_id," "smsfile," and "is_new" columns.
            :rtype: ``pandas.DataFrame``

        .. py:method:: ingest_files

            Ingest "new" SMS files into the database.

            :return: None

.. py:currentmodule:: sms_db

.. py:class:: SMSFileStats

    This class is a ``peewee.Model`` object that represents the ``SMSFileStats`` table in the SMS database.
    This table includes information about the SMS files.

    Columns include:

    .. table::

        =========== ============
        Column      Description
        =========== ============
        SMSID       ID that describes a single SMS. Primary key.
        VERSION     String of 2 or 3 characters that give the SMS version.
        FILEID      Combination of the SMSID and the VERSION.
        FILENAME    Filename of the ingested SMS file.
        INGEST_DATE Date that the file was inserted into the database.
        =========== ============

    See `peewee's documentation <http://docs.peewee-orm.com/en/latest/peewee/querying.html#selecting-multiple-records>`_
    for more examples on querying and filtering.

    Example Usage:

    .. code-block:: python

        from cosmo.sms import SMSFileStats

        query = SMSFileStats.select()  # Query for every SMS file in the database

        results = list(query.dicts())  # convert the peewee records into a list of dictionaries: {col: value}

        # You can also perform more complicated queries. See the peewee documentation for a complete description
        import datetime

        more_complicated = SMSFileStats.select(
            SMSFileStats.SMSID).where(SMSFileStats.INGEST_DATE < datetime.datetime.today()
        )

        # Get the data associated with a particular SMS
        sms = SMSFileStats.get(SMSFileStats.SMSID == '118537')

        sms.exposures  # Rows in the SMSTable table that reference the particular SMS

.. py:class:: SMSTable

    This class is a ``peewee.Model`` object that represents the ``SMSTable`` table in the SMS database.
    This table includes extracted data from the SMS files.

    Columns include:

    .. table::

        ========== ============
        Column     Description
        ========== ============
        EXPOSURE   String that describes an exposure based on Phase II information. Primary Key.
        FILEID     Same field as in the SMSFileStats table. Allows for back-referencing.
        ROOTNAME   Rootname of the exposure.
        PROPOSID   Proposal ID of the exposure.
        DETECTOR   Name of the detector used for the exposure.
        OPMODE     ACCUM, TIME-TAG, or one of the other acquisition keys.
        EXPTIME    Start time of the exposure (yyyy.ddd:hh:mm:ss).
        FUVHVSTATE Commanded High-Voltage for FUV.
        APERTURE   Aperture name.
        OSM1POS    OSM1 position.
        OSM2POS    OSM2 position.
        CENWAVE    Cenwave of the exposure.
        FPPOS      FPPOS position of the exposure.
        TSINCEOSM1 Time since the last OSM1 move.
        TSINCEOSM2 Time since the last OSM2 move.
        ========== ============

Other Modules
-------------
Cosmo also contains other modules used in supporting either the monitors or data acquisition.

.. py:currentmodule:: filesystem

.. py:function:: find_files(file_pattern, data_dir, cosmo_layout)

    Find COS data files from a source directory.
    The default ``data_dir`` is set in the configuration file.
    If another source is used, it's assumed that the directory only contains the data files, or is organized by
    program ID like the cosmo data cache.

    Example Usage:

    .. code-block:: python

        from cosmo.filesystem import find_files

        # Using the configuration file data source

        # Find all lampflash files
        lamps = find_files('*lampflash*')

        # Using a different data source with the data not organized in subdirectories
        results = find_files('*', data_dir='some/file/directory/', cosmo_layout=False)

    :param str file_pattern: file pattern to search for.
    :param str data_dir: Directory to use in searching for data files.
    Defaults to the source in the config file.
    :param bool cosmo_layout: Option for searching if the files are organized in the same way as the COSMO cache.
    Default is ``True``.

    :return: List of paths to files found.
    :rtype: ``list``

.. py:class:: FileData(*args, **kwargs)

    Class used for collecting the requested data from a particular COS FITS file.
    This class subclasses python's ``dict`` object to create a dictionary-like object.
    For a complete list of methods, see documentation for ``dict``

    :param fits.HDUList hdu: opened HDUList from the desired file.
    :param dict header_request: dictionary of requested data with extensions as keys and lists of keywords as values.
    :param dict table_request: dictionary of requested data with extensions as keys and lists of column names as values.
    :param header_defaults: dictionary of default values to use in case a header keyword is not found.
        This is useful, for example, when attempting to construct a DataModel around a particular file type that has
        similar keywords, but may or may not be missing some values depending on the exposure type (like with
        `rawacq` files: ``ACQSLEWX`` and ``ACQSLEWY`` are not always present across different acquisition types, but
        all other data required for the Acq monitors `are` shared across all `rawacq` files.
    :param bool bytes_to_str: Option to convert string data from ``bytes`` to python ``str``.
    :raises ValueError: A ``ValueError`` is raised if any set of keywords is given without a corresponding set of
        extensions or if the keywords and extensions are of different lengths.

    Example Usage:

    .. code-block:: python

        from cosmo.filesystem import FileData

        # Get the desired data from some_fitsfile.fits
        file_data = FileData.from_file(
            'some_fitsfile.fits',
            header_request={0: ('ROOTNAME', 'DETECTOR')}
        )

        # file_data is basically a dictionary with an alternate construction method

        file_data.keys()
        # dict_keys(['FILENAME', 'ROOTNAME', 'DETECTOR'])  # Note, FILENAME is automatically included

        file_data.values()
        # dict_values(['some_fitsfile.fits', 'lb4c10niq', 'NUV'])

        for key, value in file_data.items():
            print(key, value)
        # FILENAME some_fitsfile.fits
        # ROOTNAME lb4c10niq
        # DETECTOR NUV


    .. py:classmethod:: from_file(filename, *args, **kwargs)

        Create a class instance by opening the file specified by ``filename``.

    .. py:method:: get_header_data(hdu, header_keywords, header_extensions, header_defaults=None)

        Retrieve the specified header data from the input FITS file.

        :param astropy.io.fits.HDUList hdu: FITS HDUList object.
        :param dict header_request: dictionary of requested data with extensions as keys and lists of keywords as values.
        :param dict header_defaults: Default, ``None``. Dictionary of keywords that if not found should be set with a
            default value.

        :return: ``None``. This method updates the instance's dictionary.

    .. py:method:: get_table_data(hdu, table_request)

        Get the requested columns from the file's table(s).

        :param astropy.io.fits.HDUList hdu: FITS HDUList object.
        :param dict table_request: dictionary of requested data with extensions as keys and lists of column names as
            values.
        :return: ``None``. Updates the instance's dictionary.

    .. py:method:: combine(other, right_name)

        Combine ``FileData`` dictionary with another. If there are any matching keys, ``right_name`` is added.

        :param dict other: FileData or ``dict`` to combine.
        :param str right_name: label to add in the cases where there are matching keys.

.. py:class:: ReferenceData(*args, **kwargs)

    A subclass of ``FileData`` for getting requested data from COS reference files that correspond to the input COS data
    file.

    :param fits.HDUList input_hdu: ``HDUList`` from a corresponding COS data file.
    :param str reference_name: Header keyword corresponding to the requested reference file.
    :param list-like match_keys: Keys used to locate the row in the reference file that applies to the input data file.
    :param dict header_request: dictionary of requested data with extensions as keys and lists of keywords as values.
    :param dict table_request: dictionary of requested data with extensions as keys and lists of column names as values.
    :param header_defaults: dictionary of default values to use in case a header keyword is not found.

    Example Usage:

    .. code-block:: python

        from cosmo.filesystem import ReferenceData
        from astropy.io import fits

        with fits.open('path/to/some/cos_file.fits') as input_hdu:
            refdata = ReferenceData(
                input_hdu,
                'LAMPTAB',
                match_keys=('OPT_ELEM', 'CENWAVE', 'FPOFFSET'),
                table_request={1: ('TIME', 'SHIFT_DISP', 'SEGMENT')},
            )

.. py:class:: SPTData(*args, **kwargs)

    A subclass of ``FileData`` for getting requested data from an SPT file that corresponds with the input filename.

    :param str input_filename: path to an input COS data file.
    :param dict header_request: dictionary of requested data with extensions as keys and lists of keywords as values.
    :param dict table_request: dictionary of requested data with extensions as keys and lists of column names as values.
    :param header_defaults: dictionary of default values to use in case a header keyword is not found.

    Example Usage:

    .. code-block:: python

        from cosmo.filesystem import SPTData

        cos_file = '/path/to/some/cos_file.fits'

        sptdata = SPTData(cos_file, header_request={0: 'DGESTAR'})

.. py:class:: JitterFileData(*args, **kwargs)

    Class for getting requested data from COS Jitter files (either acq jitter files or association jitter files).
    Since association Jitter files have data for multiple exposures across exensions, ``JitterFileData`` subclasses the
    python ``list`` and instances are equivalent to a list of ``FileData`` dictionaries (one per extension) to enable
    collecting requested data for each included exposure.

    For a more complete set of available methods and attributes, see the documentation for python's ``list``.

    :param str filename: path to requested COS jitter file.
    :param list-like primary_header_keys: Collection of header keywords to retrieve from the primary header.
    :param list-like ext_header_keys: Collection of header keywords to retrieve from extension headers.
    :param list-like table_keys: Collection of columns to retrieve from table extensions.
    :param bool get_expstart: Option to attempt to find the corresponding EXPSTART. Requires the EXPNAME keyword to be
        retrieved.

    Example Usage:

    .. code-block:: python

        from cosmo.filesystem import JitterFileData

        asn_jitter = '/path/to/some/association_jit.fits'
        acq_jitter = '/path/to/some/single_jit.fits'

        asn_jitter_data = JitterFileData(
            asn_jitter,
            primary_header_keys=('PROPOSID', 'CONFIG'),
            ext_header_keys('EXPNAME'),
            table_keys=('SI_V2_AVG', 'SI_V3_AVG'),
            get_expstart=True  # Setting this tells JitterFileData to try and find EXPSTART from a corresponding file.
        )
        asn_jitter_data
        # [{...}, {...}, {...}, ...] Results in a list of FileData dictionaries

        # Can reduce the table data collected to a statistic, or several statistics.
        #   Removes SI_V2_AVG array, adds SI_V2_AVG_mean, SI_V2_AVG_std, and SI_V2_AVG_max
        asn_jitter_data.reduce_to_stat({'SI_V2_AVG': ('mean', 'std', 'max')})

        # Getting data from a jitter file that's not an association still returns a list
        acq_jitter = JitterFileData(acq_jitter, ...)
        acq_jitter
        # [{...}]

    .. py:method:: get_expstart()

        Attempt to find EXPSTART from a corresponding `raw` file.
        Will try to locate one of: "rawacq.fits.gz," "rawtag.fits.gz," "rawtag_a.fits.gz," "rawtag_b.fits.gz" with a
        corresponding rootname.

        Additionally retrieve the EXPTYPE keyword if a match is found.

    .. py:method:: reduce_to_stat(description)

        From a ``description`` dictionary that describes which column data to reduce to which stats, remove the original
        column data array and add keys/values for the requested stats.
        This is useful if collecting data from many jitter files.

        :param dict description: Dictionary with column names as keys and collection of desired stats.
            Supported options include max, std, and mean.


.. py:function:: get_exposure_data(filename, **kwargs)

    A convenience function for retrieving data from multiple sources (COS data file, SPT file, and reference file).
    If the data requests include different sources, the results will be combined into a single dictionary.

    :param str filename: path to the requested COS file.
    :param dict reference_request: Dictionary that combines requests for multiple reference files.
    :param **kwargs: request dictionaries that correspond to header and table request arguments in `FileData` and
        ``SPTData``.
    :return: Combined ``FileData`` dictionary

    Example Usage:

    .. code-block::python

        from cosmo.filesystem import get_exposure_data

        data = get_exposure_data(
            '/path/to/some/cos_file.fits',
            header_request={0: ('ROOTNAME', 'DETECTOR')},
            table_request={1: ('SOME_TABLE_COL')}
            spt_header_request={0: ('DGESTAR')},
            reference_request={  # Request data from multiple reference files
                'LAMPTAB': {
                    'match_keys': ('OPT_ELEM', 'CENWAVE', 'FPOFFSET'),
                    'table_request': {1: ('SEGMENT', 'FP_PIXEL_SHIFT')}
                },
                'WCPTAB': {
                    'match_keys': ('OPT_ELEM'),
                    'table_request': {1: ('XC_RANGE', 'SEARCH_OFFSET')}
                },
                ...
            }
        )

.. py:function:: get_jitter_data(*args, **kwargs)

    Convenience function for getting data from a COS Jitter file while guarding against broken files.
    Optionally, reduce requested column arrays to statistics.

    :param *args: See JitterFileData for more on ``args``
    :param dict reduce_to_stats: Apply ``description`` dict to Jitter data to reduce cols to statistics.
        See JitterFileData.reduce_to_stat for more on ``description``.
    :return: list-result of requested Jitter data.

    Example Usage:

    .. code-block:: python

        from cosmo.filesystem import get_jitter_data

        jit_data = get_jitter_data(
        '/path/to/some/jitter_file.fits',
        primary_header_keys=('PROPOSID',),
        ext_header_keys=('EXTNAME',),
        table_keys=('Roll',),
        reduce_to_stats={'Roll': ('std', 'max', 'mean')}
        )

.. py:function:: data_from_exposures(fitsfiles, **kwargs)

    Get data for multiple COS data files from multiple sources in parallel.

    :param list-like fitsfiles: Collection of COS data files from which to retrieve data.
    :param **kwargs: See ``get_exposure_data`` for more kwargs
    :return: List of combined FileData dictionaries per input file.

.. py:function:: data_from_jitters(jitter_files, **kwargs)

    Get data for multiple COS Jitter files.

    :param list-like jitter_files: Collection of jitter files to retrieve data.
    :param **kwargs: See ``get_jitter_data`` for more kwargs
    :return: List of ``JitterFileData`` lists

.. py:currentmodule:: monitor_helpers

.. py:function:: convert_day_of_year(date)

    Convert day of year date (defined as yyyy.ddd where ddd is the numbered day of that year) to an astropy ``Time``
    object.

    Example Usage:

    .. code-block:: python

        from cosmo.monitor_helpers import convert_day_of_year

        doy = convert_day_of_year('2019.125')  # doy is an astropy Time object

        # Use it as a datetime object
        dt = doy.to_datetime()

        # Use it in mjd format
        mjd = doy.mjd

        # Also works for a float
        doy = convert_day_of_year(2019.125)

    :param str date: Date of the form yyyy.ddd
    :return: Astropy Time object
    :rtype: ``astropy.time.Time``

.. py:function:: fit_line(x, y)

    Given arrays, x and y, fit a line.

    Example Usage:

    .. code-block:: python

        from cosmo.monitor_helpers import fit_line

        x, y = [1, 2 ,3]

        fit, result = fit_line(x, y)  # fit is the numpy.poly1d object, and result is the y-fit values

        # Get the slope and intercept
        slope, intercept = fit[1], fit[0]   # See numpy documentation for more info on this

    :param list or numpy.ndarray x: Independent variable for fitting.
    :param list or numpy.ndarray y: Dependent variable for fitting.
    :return: fit object
    :rtype: ``numpy.poly1d``
    :return: fit result
    :rtype: ``numpy.ndarray``

.. py:function:: explode_df(df, list_keywords)

    For a ``DataFrame`` that contains arrays for the elements of a column or columns given by ``list_keywords``, expand
    the dataframe to one row per array element.
    Each row in list_keywords must be the same length.

    Example Usage:

    .. code-block:: python

        import pandas as pd
        from cosmo.monitor_helpers import explode_df

        df = pd.DataFrame({'a': [1], 'b': [[1, 2, 3]], 'c': [[4, 5, 6]]})

        df
        #     a          b          c
        # 0  1  [1, 2, 3]  [4, 5, 6]

        exploded = explode_df(df, ['b', 'c'])

        exploded
        #    b  c  a
        # 0  1  4  1
        # 1  2  5  1
        # 2  3  6  1

    :raises AttributeError: If a column included in ``list_keywords`` does not have arrays as elements.
    :raises ValueError: If targeted columns have elements with different lengths in the same row.
    :param pandas.DataFrame df: Input ``DataFrame`` with array elements.
    :param list list_keywords: List of column-names that correspond to columns that should be expanded.
    :return: Exploded ``DataFrame``. Elements that were arrays are expanded to one element per row with non-array
        elements duplicated

.. py:function:: absolute_time(df=None, expstart=None, time=None, time_key=None, time_format='sec')

    Compute the time sequence relative to the start of the exposure (EXPSTART).
    Can be computed from a DataFrame that contains an EXPSTART column and some other time array column, or from an
    EXPSTART array and time array pair.

    Absolute time = EXPSTART[i] + time[i]

    Example Usage:

    .. code-block:: python

        from cosmo.monitor_helpers import absolute_time

        # This is silly, but the expstart array will be converted to a Time object with the mjd format
        #  and the time array will be assumed to be in seconds
        abs_time = absolute_time(expstart=[1, 2, 3], time=[4, 5, 6])

        # From a DataFrame, df with EXPSTART and TIME columns
        abs_time = absolute_time(df=df)

        # If the time column is named something different
        abs_time = absolute_time(df=df, time_key='SomeOtherTime')

        # Use the result as a datetime object
        absolute_datetime = abs_time.to_datetime()

    :raises TypeError: If no values are given, or if one array is given without the other.
    :raises ValueError: If a ``DataFrame`` is given with arrays.
    :param pandas.DataFrame df: ``DataFrame`` with the relevant information. If the time array is under a column name
        other than "TIME", then the column name must be specified with ``time_key``.
    :param array-like expstart: Exposure start time values.
    :param array-like time: Time values (typically this indicates events within an exposure).
    :param str time_key: Optional. Column-name of the time array. Required if the time values are not under "TIME".
    :param str time_format: Default, 'sec'. Specify a different format for a different time unit (see astropy's
        TimeDelta documentation for more format options).
    :return: Time relative to the start of the exposure.
    :rtype: astropy.time.TimeDelta

.. py:function:: create_visibility(trace_lengths, visible_list)

    Create a "visibility list" for use with constructing ``plotly`` buttons.
    Creates a list of ``True`` and ``False`` that corresponds to the traces in the monitors' plotly figures.

    Example Usage:

    .. code-block:: python

        from cosmo.monitor_helpers import create_visibility

        # All traces for a figure will be in a single list, but "sets" of traces that should be active are usually kept
        #  track of by the length of the set, and the order in which the sets are created.
        trace_lengths = [1, 2, 3]  # The figure has a total of 6 traces, with three distinct sets (ex: button options)
        visible = [True, False, False]  # For this setting, we only want the first set visible (True) and all others not

        visibility = create_visibility(trace_lengths, visible)

        visibility
        # [True, False, False, False, False, False]

    :param list trace_lengths: List of integer lengths that correspond to the number of traces that determine a "set".
    :param list visible_list: List of ``bool`` that determine which "sets" should be active (``True``) or not
        (``False``)
    :return: List of visibility options for each trace, determined by the set lengths
    :rtype: ``list``

.. py:function:: detector_to_v2v3(slew_x, slew_y)

    Convert slews in detector coordinates to V2/V3 coordinates.

    v2 = x * cos(45degrees) + y * sin(45degrees)
    v3 = x * cos(45degrees) - y * sin(45degrees)

    Example Usage:

    .. code-block:: python

        import numpy as np
        from cosmo.monitor_helpers import detector_to_v2v3(slew_x)

        x, y = np.array([1, 2 ,3])

        v2, v3 = v2v3(x, y)

        v2, v3
        # (array([1.41421356, 2.82842712, 4.24264069]),
        #  array([1.11022302e-16, 2.22044605e-16, 4.44089210e-16]))

    :param array-like slew_x: X-Slew values in detector coordinates
    :param array-like slew_y: Y-Slew values in detector coordinates
    :return: Slews in V2 and V3 coordinates
    :rtype: ``tuple`` (V2, V3)

.. py:function:: get_osm_data(datamodel, detector)

    Query for all OSM data and append any relevant new data.

    Example Usage:

    .. code-block:: python

        from cosmo.monitors import get_osm_data
        from cosmo.monitors.data_models import OSMDatamodel

        datamodel = OSMDatamodel()

        data = get_osm_data(datamodel, 'FUV')

    :param OSMDatamodel datamodel: instance of the OSMDatamodel class
    :param str detector: COS Detector name (used in filtering between the two OSM Monitors). "NUV" or "FUV"
    :return: ``DataFrame`` with required data for the OSM monitors.
    :rtype: pandas.DataFrame
