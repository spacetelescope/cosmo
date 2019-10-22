DataModels
==========
The ``monitorframe`` framework uses ``DataModel`` classes (subclasses of the ``BaseDataModel``) to find new data and
interact with the monitor data database.
Each monitor requires a DataModel to function and accesses data through the DataModel object.

DataModels can also be used independently from the Monitor that they were intended for for exploring the data or
ingesting new data outside of the monitoring workflow.

For more detailed information on the DataModel, see the
`monitorframe documentation <https://spacetelescope.github.io/monitor-framework/creating_monitors.html#defining-a-new-data-model>`_.

AcqDataModel
------------
The ``AcqDataModel`` is used with the Target Acquisition monitors.

AcqDataModel includes the following data:

- ACQSLEWX: Slew value in the X direction.
  Default of 0 for Acquisition types that don't record this keyword.
- ACQSLEWY: Slew value in the Y direction.
  Default of 0 for Acquisition types that don't record this keyword.
- EXPSTART: Time at the start of the exposure (mjd).
- ROOTNAME: Exposure rootname.
- PROPOSID: Proposal ID.
- OBSTYPE: Imaging or Spectroscopic.
- NEVENTS: Number of events recorded during the exposure.
- SHUTTER: Open or Closed.
- LAMPEVNT: Number of events recorded from the internal lamp exposure.
- ACQSTAT: Success or Failure.
- EXTENDED: Yes or No.
- LINENUM: Line number where the exposure occurs on the observation schedule.
- APERTURE: Aperture used during the observation.
- OPT_ELEM: Grating or Mirror used during the observation.
- LIFE_ADJ: Lifetime Position.
- CENWAVE: Cenwave of the observation.
- DETECTOR: NUV or FUV.
- EXPTYPE: Type of Acquisition.
  ACQ/IMAGE, ACQ/PEAKD, ACQ/PEAKXD, or ACQ/SEARCH
- DGESTAR: Dominant guide star used for the exposure.
- FGS: Fine guidance sensor used during the exposure.
  F1, F2, or F3

These data are gathered from *rawacq* and *spt* files.

OSMDataModel
------------
The ``OSMDataModel`` is used with the OSM Monitors.

OSMDataModel includes the following data:

- ROOTNAME: Exposure rootname.
- EXPSTART: Time at the start of the exposure (mjd).
- DETECTOR: NUV or FUV.
- LIFE_ADJ: Lifetime Position.
- OPT_ELEM: Grating or Mirror used during the observation.
- CENWAVE: Cenwave of the observation.
- FPPOS: FPPOS used for the observation.
- PROPOSID: Proposal ID.
- OBSET_ID: Observation set ID.
- TIME: The TIME column of a given exposure (s).
  Time of shift measurement (relative to the start of the exposure).
- SHIFT_DISP: The SHIFT_DISP column of a given exposure (pix).
  Shift values in the dispersion direction.
- SHIFT_XDISP: The SHIFT_XDISP column of a given exposure (pix).
  Shift values in the cross-dispersion direction.
- SEGMENT: The SEGMENT column of a given exposure.
- SEGMENT_LAMPTAB: The SEGMENT column of the matched row in the LAMPTAB file.
- FP_PIXEL_SHIFT: the FP_PIXEL_SHIFT column of the matched rows (all segments) in the LAMPTAB file.
  The FP_PIXEL_SHIFT is the offset in pixels for each FPPOS.
- XC_RANGE: The XC_RANGE column of the matched row in the WCPTAB file.
  XC_RANGE is the maximum pixel offset to use when doing a cross correlation between the observed data and the template
  wavecal.
- SEARCH_OFFSET: The SEARCH_OFFSET column of the matched row in the WCPTAB file.
  The SEARCH_OFFSET is the zero-point offset for the search range.


These data are gathered from *lampflash* files.
