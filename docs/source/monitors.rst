COS Monitors
============
The monitors in COSMO are designed to provide information on the health of the COS instrument, changes in behavior of
the detectors or mechanical components, and target acquisition statuses and trends.
These monitors allow the COS Branch at STScI to detect and resolve issues with the COS instrument or acquisitions
quickly as well as track the evolution of the instrument over time in order to provide the best science products and
scientific capabilities to the astronomical community.

Currently, COSMO supports monitoring in three areas of COS operations:

- Target Acquisitions
- Optics Select Mechanism (OSM) trends/evolution
- Dark rate trends/evolution

These monitors build off of a previous COS Branch monitoring program, an overview of which is given in
`Technical Instrument Report COS 2014-04 <https://innerspace.stsci.edu/download/attachments/166755094/TIR2014_04.pdf?version=1&modificationDate=1557948271236&api=v2>`_.

Target Acquisition Monitors
---------------------------
The goal of the Target Acquisition monitors is to assist in cases of failed acquisitions as well has keep track of slew
vs time in order to identify any systematic trends or issues with COS target acquisitions.

Each acquisition monitor is covers both slew directions (ACQSLEWX and ACQSLEWY) and according to which FGS was used.

ACQIMAGE Monitor
^^^^^^^^^^^^^^^^

.. Needs to be finalized

ACQPEAKD Monitor
^^^^^^^^^^^^^^^^

.. Needs to be finalized

ACQPEAKXD Monitor
^^^^^^^^^^^^^^^^^

.. Needs to be finalized

FGS Monitoring
^^^^^^^^^^^^^^

.. Needs an overview of how some of our monitors indirectly check how the FGSs are doing.
    This is primarily  for V2V3

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

Tracking
++++++++
For FUV, the OSM Shift monitors track the difference between the reported shift for the two FUV segments, FUVA and FUVB,
in the form of FUVA - FUVB.
NUV tracking TBD.

.. need to add more about why we want to look at A - B

Output
++++++
FUV output for both SHIFT1 and SHIFT2 monitors consist of the shift measurement (gathered from ``lampflash`` files)
plotted as a function of time.
Each grating/cenwave combination is plotted as a different color, and each FPPOS is plotted with a different symbol.
Additionally there are button options to switch between viewing the shift vs time for all FPPOS and individual FPPOS.
Vertical lines are included to denote the beginning of each new Lifetime Position.

.. NUV will be added once the output is finalized


OSM Drift Monitor
^^^^^^^^^^^^^^^^^

.. Needs to be finalized

Dark Rate Monitors
------------------

FUV Dark Rate Monitors
^^^^^^^^^^^^^^^^^^^^^^

NUV Dark Rate Monitors
^^^^^^^^^^^^^^^^^^^^^^

SMS Data Ingestion
------------------
