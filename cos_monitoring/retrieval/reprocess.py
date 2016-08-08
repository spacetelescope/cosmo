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
    query1 = "SELECT DISTINCT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE ads_data_set_name "\
    "LIKE '_{0}%'\ngo".format(str(vid[0]))
    prop_datasets = janky_connect(SETTINGS, query1)

    return prop_datasets

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def handle_datasets(rootname):
    '''
    Get proposal IDs and, if applicable, all member datasets given a visit
    '''
    
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)
    
    query = "SELECT DISTINCT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE ads_data_set_name "\
    "LIKE '{0}%'\ngo".format(str(rootname))

    root_datasets = janky_connect(SETTINGS, query)
    pdb.set_trace()
    return root_datasets

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def parse_input(args):
    ind_args = []
    for item in args:
        if "," in item:
            tmp = (x for x in item.split(",") if x)
            for tmpitem in tmp:
                isprop = is_proposal(tmpitem)
                if len(tmpitem) != 9 and "*" not in tmpitem and not isprop:
                    tmpitem += "*"
                ind_args.append(tmpitem)
        else:
            isprop = is_proposal(item)
            if len(item) != 9 and "*" not in item and not isprop:
                item += "*"
            ind_args.append(item)

    return ind_args

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def is_proposal(mystring):
    try:
        prop = int(mystring)
        status = True
    except ValueError:
        status = False

    return status

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # This is a required argument. To input multiple arguments, they must be
    # SPACE separated, not comma separated.
    parser.add_argument("data", nargs="+",  help="Reprocess programs or datasets")
    args = parser.parse_args()

    data = parse_input(args.data)
    print(data)
    
    all_data = []
    for item in data:
        isprop = is_proposal(item)
        if isprop:
            prop_datasets = query_proposal(item)
            all_data += prop_datasets
        else:
            root_datasets = handle_datasets(item)        
            all_data += root_datasets

    to_retrieve = {row[0]:row[1] for row in all_data}

    pdb.set_trace()
#   retrieve(all_data)
