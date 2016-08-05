#! /usr/bin/env python
from __future__ import print_function, absolute_import, division
'''
Re-request and process COS data to keep products up to date.
'''

import argparse
import yaml
import os
import pdb

from .find_new_cos_data import janky_connect

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def query_proposal(proposal):
    '''
    Find all member datasets of a given proposal ID.

    Parameters:
    -----------
        proposal : int
            The proposal ID for which you wish to retrieve all data.
    
    Returns:
    --------
        prop_datasets : list
            List of all datasets given a proposal ID. 
    '''

    # Open the configuration file for the MAST database connection (TSQL).
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

    # Need to use opus_rep DB instead of dadsops_rep to get ALL datasets
    SETTINGS["database"] = "opus_rep"
    query0 = "SELECT DISTINCT program_id FROM executed "\
    "WHERE proposal_id='{0}'\ngo".format(str(proposal))
    vid = janky_connect(SETTINGS, query0)

    # Now we need to go back to dadsop_rep to get ads_data_set_name
    SETTINGS["database"] = "dadsops_rep"
    query1 = "SELECT DISTINCT ads_data_set_name,ads_pep_id FROM archive_data_set_all "\
    "WHERE ads_data_set_name LIKE '_{0}%'\ngo".format(str(vid[0]))
    prop_datasets = janky_connect(SETTINGS, query1)

    return prop_datasets

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def handle_datasets(datasets):
    '''
    Get proposal IDs and, if applicable, all member datasets given a visit
    '''



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
            handle_datasets(item)        
    
    print(args.data)

