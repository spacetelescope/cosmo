#! /usr/bin/env python

from .retrieval_info import BASE_DIR, CACHE
from .manualabor import work_laboriously, chmod, PERM_755, PERM_872
from .find_new_cos_data import find_new_cos_data
from .request_data import run_all_retrievals

# Change permissions on base directory.
chmod(basepath=BASE_DIR, mode=PERM_755, filetype=None, recursive=True)

# Run find_new_cos_data
find_new_cos_data(pkl_it=True, use_cs=True, run_labor=True, do_chmod=False)

# Run request_data
run_all_retrievals(prop_dict=None, pkl_file="filestoretrieve.p", prl=True, do_chmod=False)

# Run manualabor
work_laboriously(prl=True, do_chmod=False)

