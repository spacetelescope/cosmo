#! /usr/bin/env python
from __future__ import print_function, absolute_import, division
'''
Specify data to reprocess.
'''

import argparse
import yaml
import os

from .find_new_cos_data import janky_connect

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def query_proposal(proposal):
    '''
    Find all datasets of a given proposal ID.
    '''

    # Open the configuration file for the MAST database connection (TSQL).
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

    query0 = "SELECT DISTINCT ads_program_id FROM "\
    "archive_data_set_all WHERE ads_pep_id='{0}'".format(str(proposal))

    prop


#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # This is a required argument. To input multiple arguments, they must be
    # SPACE separated, not comma separated.
    parser.add_argument("data", nargs="+",  help="Reprocess programs or datasets")
    args = parser.parse_args()

    data = args.data
    isproposal = []
    data_dict = {}
    for item in data:
        try:
            prop = int(item)
            isproposal.append(True)
            prop_datasets = query_proposal(prop)
        except ValueError:
            isproposal.append(False)
            handle_dataset(item)        
    

    print(args.data)

