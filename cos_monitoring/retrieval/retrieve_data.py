from __future__ import print_function, absolute_import, division

import os
import yaml
import pdb

from cos_monitoring.database.db_tables import load_connection


# Need to get the config file. 
config_file = os.path.join(os.environ['HOME'], "configure.yaml")
with open(config_file, 'r') as f:
    SETTINGS = yaml.load(f)

    # Connect to the database.
    Session, engine = load_connection(SETTINGS['connection_string'])
    test = engine.execute("SELECT * FROM headers;")
    testkeys = test.keys()
    pdb.set_trace()
    results_null = engine.execute("SELECT rootname FROM headers WHERE asn_id IS NULL;")
    keys_null = results_null.keys()
    results_asn = engine.execute("SELECT asn_id FROM headers WHERE asn_id IS NOT NULL;")

    # Close connection
    engine.dispose()


# There are two types of data that will show up in my query of all MAST data_set_names:
# 1) association data (ending in 0 generally), which will also pick up jitter, 
#   lampflash, and acq data
# 2) single exposure data ending in Q (e.g. dark) 

# This could be a problem depending on Justin's DB setup. e.g. if he does has
# the subsets of an asn file listed individually, my cross-check will think those are
# new files even if they are already exist in /smov/cos/Data (because they are all
# listed under one name, the asn ID in the 'science' table.


