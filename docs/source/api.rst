API
===
..  3. API for filesystem.py
    4. API for monitor_helpers.py
    5. API for the sms subpackage
    6. API for retrieval

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
For more help with the configuration file, see this section: :ref:`Settings via a Configuration File`

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
        ``track``, ``find_outliers``, and ``set_notification`` respectivelly.

        :return: None

        .. note::

            Order matters! If steps of the monitoring process are run individually, they must be run in the correct
            order.
            For example, if ``Monitor.find_outliers`` is called before ``Monitor.initialize_data``, an error will be
            raised since the ``data`` attribute was not set.

    .. py:method:: write_figure

        Write the output figure to an htnml file using the ``output`` directory and/or name provided.

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

