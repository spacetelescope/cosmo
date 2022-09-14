#! /usr/bin/bash
# Where do the telemetry data files live? (unlikely to change this)
export TELEMETRY_DATADIR="/grp/hst/cos/Telemetry/"

# Where do you want the output plots to live? (likely to change this)
export TELEMETRY_PLOTSDIR="/grp/hst/cos2/monitoring/telemetry/latest"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
# Where does the mnemonics excel file live? (likely to change this)
export TELEMETRY_MNEMONICS="$SCRIPT_DIR/COSMnemonics.xls"

#  Path to the (JSON) file where you specify the numerical value of each telemetry variable whose value is a text string (i.e. the OSM positions)
export TELEMETRY_TEXTS="$SCRIPT_DIR/telemetry_monitors_text.json"

# Where does the zoom-specifying csv file live? (likely to change this)
export TELEMETRY_ZOOMS="$SCRIPT_DIR/default_telemetry_zooms.csv"