COS Monitors
============
The monitors in COSMO are designed to provide information on the health of the COS instrument, changes in behavior of
the detectors or mechanical components, and target acquisition statuses and trends.
These monitors allow the COS Branch at STScI to detect and resolve issues with the COS instrument acquisitions, or
data quality quickly as well as track the evolution of the instrument over time in order to provide the best science
products and scientific capabilities to the astronomical community.

Currently, COSMO supports monitoring in three areas of COS operations:

- Target Acquisitions
- Optics Select Mechanism (OSM) trends/evolution
- Dark rate trends/evolution

These monitors build off of a previous COS Branch monitoring program, an overview of which is given in
`Technical Instrument Report COS 2014-04 <https://innerspace.stsci.edu/download/attachments/166755094/TIR2014_04.pdf?version=1&modificationDate=1557948271236&api=v2>`_.

Monitor Cadence
---------------
The following monitors are executed at a *monthly* cadence:

- ``AcqImageMonitor``
- ``AcqPeakdMonitor``
- ``AcqPeakxdMonitor``
- ``AcqImageV2V3Monitor``
- ``FuvOsmShift1Moniotr``
- ``FuvOsmShift2Monitor``
- ``NuvOsmShift1Monitor``
- ``NuvOsmShift2Monitor``
- ``FUVOSMDriftMonitor``
- ``NUVOSMDriftMonitor``

.. eventually there will be daily monitors

The Monitor Runner
------------------
In order to execute collections of Monitors, COSMO uses ``pytest``.

Collections are determined based on the ``run`` attribute of the Monitor classes, and should reflect the cadence that
the monitor should be executed with.

Supported cadences:

- ``monthly``
- ``daily``

By default, all Monitors are also included in an ``all`` collection, which is the default collection if no other options
are provided.

An ``ingest``  collection is available for executed data ingestion into the Monitor Data database and the
SMS database independently of executing the Monitors themselves.

When developing COSMO, Monitors can be collected and run via ``pytest`` from the commandline in the typical pytest
fashion (after cloning the repository into "cosmo"::

    (cosmoenv) mycomputer:cosmo user$ pytest cosmo/run_monitors.py

This basic example will execute all monitors available and attempt to ingest new data into their respective databases.

To execute a collection of monitors (again, from a local "cosmo" repository)::

    (cosmoenv) mycomputer:cosmo user$ pytest cosmo/run_monitors.py -m monthly

COSMO also provides a command line option for executing the monitors without calling ``pytest`` directly, and without
the need to specify the location of the ``run_monitors`` module::

    (cosmoenv) mycomputer:~ user$ cosmo --monthly

Target Acquisition Monitors
---------------------------
The goal of the Target Acquisition monitors is to assist in cases of failed acquisitions as well as keep track of
performance order to identify any systematic trends or issues with COS target acquisitions.

ACQIMAGE Monitor
^^^^^^^^^^^^^^^^
.. note::

    This monitor is still in development.
    Output and tracking will be described in more detail once it is finalized.

The ``AcqImage`` monitor is intended to be a more generalized monitor that keeps track of acquisition statuses and slew
statistics.

Spectroscopic Acquisition Monitors: ACQPEAKD and ACQPEAKXD
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The Spectroscopic Acquisition Monitors include the ``AcqPeakdMonitor`` and the ``AcqPeakxdMonitor``.
These monitors are used to track changes in in the precision of the spectroscopic acquisition modes.

Tracking
++++++++
Both Spectroscopic Acquisition Monitors track the standard deviation of their respective slew values (ACQSLEWX for
AcqPeakdMonitor and ACQSLEWY for AcqPeakxdMonitor) for each Fine Guidance Sensor (FGS).

Outliers are defined as those Spectroscopic Acquisitions with slew magnitudes of greater than 1 arcsecond.

Output
++++++
Each Spectroscopic Acquisition Monitor produces similar output with some small differences:

- For AcqPeakdMonitor, the Offset axis is created from the ACQSLEWX keyword, while AcqPeakxdMonitor uses the ACQSLEWY
  key.
- AcqPeakxdMonitor includes a vertical line that indicates when the ACQ/PEAKXD algorithm was changed.

Apart from those differences, the outputs are the same, and so will be described together.

The figures produced for the Spectroscopic Acquisition Monitors plots Offset vs Datetime as a scatter plot.
The Offset axis is the negative SLEW, while the Datetime axis is EXPSTART, or start time of the exposures converted from
``mjd`` to a date and time.
Outliers are denoted as slightly enlarged, red X's, while FUV and NUV settings are denoted by circles and x's
respectively.
A green band spans +/- 1 arcsecond to help guide the eye and further separate outlying points from the rest.

The scatter plot is grouped in two different ways: Lifetime Position (LP) and FGS.
Individual LPs can be selected or deselected via the legend while FGS groups are controlled via the drop-down button on
the left side of the figure.

The hover text for each data point includes the following information::

    (Datetime, Offset)
    ROOTNAME
    PROPOSID
    LIFE_ADJ
    OPT_ELEM
    CENWAVE
    DETECTOR

FGS Monitoring: V2V3 Offset Monitor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``AcqImageV2V3Monitor`` is used to monitor the performance of the FGS and determine if/when a realignment may be
necessary.

In order to keep track of the systematic trends that would be produced by the FGS, the ACQ/IMAGE data are filtered to
avoid outliers or trends that may be due to other factors, and to include only those acquisitions that can be used to
assess the observatories initial, or "blind" pointing, which can be affected by the FGS.

The filters are as follows:

To ensure that the data come from ACQ/IMAGE exposures that occur first in a sequence:

    - OBSTYPE = 'IMAGING'
    - LINENUM must end with a '1'; This is an indication that the ACQ/IMAGE occurred first in an observation sequence.

To ensure that the ACQ/IMAGE data being used come from more "typical" exposures:

    - NEVENTS >= 2000; filter out "dark" acquisitions or those with very dim sources.
    - Total slew distances < 2 arcseconds; Slews on ACQ/IMAGEs that are greater than 2 arcseconds usually have other
      issues such as user coordinate errors.
    - SHUTTER is 'Open' and ACQSTAT is 'Success'; to filter out failed acquisitions.
    - EXTENDED is 'No'; Extended targets may have less-reliable centering.
    - LAMPEVNT >= 500; The internal lamp is used to calculate the position of the object on the detector.

From these ACQ/IMAGE data, the ACQSLEWX and ACQSLEWY values are converted from detector coordinates to observatory
coordinates (V2/V3).

The conversion in Python is as follows:

.. code-block:: python

    rotation_angle = np.radians(45.0)  # rotation angle in degrees converted to radians

    x_conversion = slew_x * np.cos(rotation_angle)
    y_conversion = slew_y * np.sin(rotation_angle)

    v2 = x_conversion + y_conversion
    v3 = x_conversion - y_conversion

Tracking
++++++++
AcqImageV2V3Monitor tracks line-fit parameters/results for V2/V3 offset vs time (slope, value at the time of the first
data point, and value at the time of the last data point) for those data from the last FGS realignment until the current
time for each FGS.

Output
++++++
AcqImageV2V3Monitor plots the Offset (-SLEW) vs Datetime (EXPSTART, or the start time of the exposure converted from
``mjd`` to a date and time) along with line-fits for V2 and V3 in two subplots.

The plots are broken up by break points determined by important dates for each FGS such as realignments or guide-star
catalogue updates.
The break points are denoted by vertical, dashed lines, and each group that is created by those break points are labeled
as "Group (n)" where n is the group number (ordered by date).
Each group includes a V2 and V3 section (which itself includes a scatter and line plot for each).
In addition, there are other vertical lines that represent dates of note that are not break points that are denoted as
solid black vertical lines.
These groups can be selected or deselected via the legend.

The legend includes information about the line-fit of the Offset vs Time scatter.
In particular, it includes the slope in arcseconds per year and the offset at the time of of the first data point in the
fit.

The plots are also grouped by FGS via a drop-down button on the left side of the figure.

Hover text for each data point includes the following::

    (Datetime, Offset)
    ROOTNAME
    PROPOSID

.. note::

    This plot will come up empty at first.
    An FGS option must be selected before plots will be shown.

OSM Monitors
------------
The OSM monitors are designed to monitor the behavior of the two OSM components of COS (OSM1 and OSM2).

.. Probably need more of an explanation here.

OSM Shift Monitors
^^^^^^^^^^^^^^^^^^
The goal of the OSM shift monitors is to track any trends in the OSM shifts (measured by the CalCOS WAVECORR module) as
a function of time.

.. definitely need more about the goal or objective of the OSM shift monitors here

The OSM Shift monitors are broken up into FUV and NUV components and are also tracked both for the along-dispersion
(SHIFT1) and cross-dispersion (SHIFT2) shifts for a total of four individual monitors.

FUV OSM Shift Monitors
++++++++++++++++++++++
Tracking
........
For FUV, the OSM Shift monitors track the difference between the reported shift for the two FUV segments, FUVA and FUVB,
in the form of FUVA - FUVB.
Outliers for the SHIFT1 and SHIFT2 Monitors are those exposures with a segment difference (FUVA - FUVB) of greater than
10 pixels and greater than 5 pixels respectively.


Output
......
FUV output for both SHIFT1 and SHIFT2 monitors consist of two subplots:

- The shift measurement plotted as a function of time
- The segment difference plotted as a function of time

.. note::

    The FPPOS offset is *not* removed from the SHIFT1/SHIFT2 value.
    This is intentional so that trends per FPPOS can be directly compared against each other.

Each grating/cenwave combination is plotted as a different color, and each FPPOS is plotted with a different symbol
(these individual elements can be selected/deselected via the legend).
Exposures that occurred at LP3 after the move to LP4 are slightly enlarged.
Outliers are indicated with red.

There are button options to switch between viewing the shift vs time for all FPPOS and individual FPPOS.
Vertical lines are included to denote the beginning of each new Lifetime Position.

Hover text for each data point includes the following::

    # For the Shift vs Time subplot
    (x, y) or (Datetime, Shift)
    ROOTNAME
    LIFE_ADJ
    FPPOS
    PROPOSID
    SEGMENT
    CENWAVE

    # For the FUVA - FUVB vs Time subplot
    (x, y) or (Datetime, A - B)
    ROOTNAME
    LIFE_ADJ
    FPPOS
    PROPOSID
    CENWAVE

.. note::

    This figure will be empty at first.
    A FPPOS option must be selected before the plots will be displayed.

NUV OSM Shift Monitors
++++++++++++++++++++++
Tracking
........
The NUV OSM Shift Monitors track the difference between the NUV Stripes: NUVB - NUVC and NUVC - NUVA.
Outliers for both SHIFT1 and SHIFT2 are those stripe differences that are greater than or equal to 2sigma.

Output
......
Like the FUV OSM Shift Monitors, the NUV OSM Shift Monitors outputs are figures with subplots: Shift vs Time at the top
and B - C and C - A in two additional subplots.
For both Shift1 and Shift2, Shift vs Time is plotted per grating and outliers are marked with red.
In addition, a rolling average line is plotted per grating (with matching colors), where the rolling average is taken
over 180 days, or approximately 6 months.

For the NUV OSM Shift1 Monitor, the shift values are corrected for the appropriate FPPOS offset, and for each grating,
a box is drawn that indicates the search range used by CalCOS to determine the shift between the wavecal spectrum and
its template (the box color matches the grating colors.

There are button options available to switch between viewing the plots for all NUV Stripes, or for individual stripes.

Hover text for each data point includes the following::

    # For the Shift vs Time subplot
    (x, y) or (Datetime, Shift)
    ROOTNAME
    LIFE_ADJ
    FPPOS
    PROPOSID
    SEGMENT
    CENWAVE

    # For the Stripe difference vs Time subplots (B-C and C-A)
    (x, y) or (Datetime, B - C or C - A)
    ROOTNAME
    LIFE_ADJ
    FPPOS
    PROPOSID
    CENWAVE

OSM Drift Monitors
^^^^^^^^^^^^^^^^^^
The OSM Drift Monitors keep track of the drift rate vs the time since the last OSM movement in order to detect changes
in how the OSMs drift during observations.

The FUV monitor tracks the drift for SHIFT1 and SHIFT2 for OSM1 moves, while the NUV monitor tracks the drift for SHIFT1
and SHIFT2 for both OSM1 and OSM2 (since NUV settings can require the movement of both mechanisms).

Tracking
++++++++
``FUVOSMDriftMonitor`` tracks statistics for the SHIFT1 and SHIFT2 drifts for each Lifetime Position.

Statistics include:

- mean
- min
- max
- 25 :sup:`th` and 75 :sup:`th` percentiles
- standard deviation.

The same statistics are recorded for ``NUVOSMDriftMonitor``, however, they're recorded for each NUV Stripe.

Output
++++++
Both OSM Drift Monitors produce similar output, but with different groupings and button options.

FUV OSM Drift Output
....................
The output figure for FUVOSMDriftMonitor contains two subplots for SHIFT1 Drift and SHIFT2 Drift both vs Time since last
OSM1 move.
The subplots are grouped by grating, each of which can be selected/deselected via the legend and are colored by
observation start time.

The plots are grouped by Lifetime Position via the drop-down menu on the left side of the figure.

Hover text for each data point includes the following::

    (x, y) or (Datetime, Driftrate)
    ROOTNAME
    LIFE_ADJ
    FPPOS
    PROPOSID
    OPT_ELEM
    SEGMENT

NUV OSM Drift Output
....................
The figure for NUVOSMDriftMonoitor contains four subplots for the following:

- SHIFT1 Drift vs Time since last OSM1 move
- SHIFT2 Drift vs Time since last OSM1 move
- SHFIT1 Drift vs Time since last OSM2 move
- SHIFT2 Drift vs Time since last OSM2 move

The suplots are grouped by grating, each of which can be selected/deselected via the legend and are colored by
observation start time.

The plots are grouped by NUV Stripe via the drop-down menu on the left side of the figure.

Hover text for each data point includes the following::

    (x, y) (Datetime, Driftrate)
    ROOTNAME
    LIFE_ADJ
    FPPOS
    PROPOSID
    OPT_ELEM

Dark Rate Monitors
------------------

FUV Dark Rate Monitors
^^^^^^^^^^^^^^^^^^^^^^

.. note::

    This monitor is under construction.

NUV Dark Rate Monitors
^^^^^^^^^^^^^^^^^^^^^^

.. note::

    This monitor is under construction.
