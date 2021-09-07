#! /usr/bin/bash
# Where do the telemetry data files live? (unlikely to change this)
export TELEMETRY_DATADIR="/grp/hst/cos/Telemetry/"

# Where do you want the output plots to live? (likely to change this)
export TELEMETRY_PLOTSDIR="/user/nkerman/Projects/Monitors/telemetry_plots/"

# Where does the mnemonics excel file live? (likely to change this)
export TELEMETRY_MNEMONICS="/home/nkerman/Projects/Monitors/cosmo/cosmo/telemetry_support/COSMnemonics.xls"

#  Path to the (JSON) file where you specify the numerical value of each telemetry variable whose value is a text string (i.e. the OSM positions)
export TELEMETRY_TEXTS="/home/nkerman/Projects/Monitors/cosmo/cosmo/telemetry_support/telemetry_monitors_text.json"

# Where does the zoom-specifying csv file live? (likely to change this)
export TELEMETRY_ZOOMS="/home/nkerman/Projects/Monitors/cosmo/cosmo/telemetry_support/default_telemetry_zooms.csv"