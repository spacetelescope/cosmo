#! /usr/bin/env python

import datetime

from cos_monitoring.retrieval.request_data import run_all_retrievals
from cos_monitoring.retrieval.find_new_cos_data import find_new_cos_data
from cos_monitoring.retrieval.calibrate_data import calibrate_data
from cos_monitoring.retrieval.set_permissions import set_user_permissions, set_grpid

t1 = datetime.datetime.now()
pkl_file = "cosmo_{}.p".format(t1.strftime("%Y%m%d_%M%S"))

# First, change permissions of the base directory so we can modify files.
set_user_permissions("open", prl=True)

all_missing_data = find_new_cos_data(pkl_it=True, pkl_file=pkl_file,
                                     use_cs=True, prl=True)
run_all_retrievals(prop_dict=all_missing_data, pkl_file=None,
                   prl=True)
calibrate_data(prl=True)

# Change permissions back to protect data, and change group ID based on 
# proprietary status.
set_grpid(prl=True)
set_user_permissions("close", prl=True) 

t2 = datetime.datetime.now()
print("executed in {}".format(t2-t1))
