#! /usr/bin/env python
from __future__ import print_function, absolute_import, division
'''
Re-request and process COS data to keep products up to date.
'''
import atexit
import argparse
import yaml
import os
import pdb

from .find_new_cos_data import janky_connect, copy_cache
from .request_data import run_all_retrievals

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
    query1 = "SELECT DISTINCT ads_data_set_name FROM "\
    "archive_data_set_all WHERE ads_data_set_name "\
    "LIKE '_{0}%'\ngo".format(str(vid[0]))
    datasets = janky_connect(SETTINGS, query1)
    prop_datasets = [[x, proposal] for x in datasets]

    return prop_datasets

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def handle_datasets(rootname):
    '''
    Get proposal IDs and, if applicable, all member datasets given a visit or
    program.

    Parameters:
    -----------
        rootname : str
            The rootname (partial or full) to be retrieved.

    Returns:
    --------
        root_datasets : list
            List of all datasets for a given rootname.
    '''
    
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)
    
    query0 = "SELECT DISTINCT ads_data_set_name,ads_pep_id FROM "\
    "archive_data_set_all WHERE ads_data_set_name "\
    "LIKE '{0}%'\ngo".format(str(rootname))
    root_datasets = janky_connect(SETTINGS, query0)
    
    # If data do not have a PID in dadsops_rep, look at opus_rep
    if root_datasets[0][1] == "NULL":
        if root_datasets[0][0].startswith("L_"):
            prop = "CCI"
        else:
            program_id = rootname[1:4]
            query1 = "SELECT DISTINCT proposal_id FROM executed WHERE "\
            "program_id='{0}'\ngo".format(program_id)
            SETTINGS["database"] = "opus_rep"
            prop = janky_connect(SETTINGS, query1)
            if not prop:
                prop = "NULL"
        for i in range(len(root_datasets)):
            root_datasets[i][1] = prop
    
    return root_datasets

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#
def handle_cci(get_old=False):
    '''
    Retrieve all CCI datasets.

    Parameters:
    -----------
        get_old : Boolean
            In 2014, the CCI naming convention changed. Should the old CCIs
            be retrieved as well?

    Returns:
    --------
        cci_datasets : list
            List of all CCI datasets to be retrieved.. 
    '''
    
    config_file = os.path.join(os.environ['HOME'], "configure2.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)
    
    query0 = "SELECT ads_data_set_name FROM archive_data_set_all WHERE "\
    "ads_archive_class='CSI'\ngo"
    all_ccis = janky_connect(SETTINGS, query0)

    if not get_old:
        cci_datasets = [[x,"CCI"] for x in all_ccis if x.startswith("L_")]
    else:
        cci_datasets = [[x,"CCI"] for x in all_ccis]
    
    return cci_datasets

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def parse_input(args):
    '''
    Parse the input argument to remove commas or * if present.

    Parameters:
    -----------
        args : list
            argparse list of input parameters

    Returns:
    --------
        ind_args : list
            List, each element is a cleaned up parameter.
    '''

    ind_args = []
    for item in args:
        if "," in item:
            tmp = (x for x in item.split(",") if x)
            for tmpitem in tmp:
                ind_args.append(tmpitem.strip("*"))
        else:
            ind_args.append(item.strip("*"))

    return ind_args

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def is_proposal(mystring):
    '''
    Determine if an input is a proposal (e.g. 12046) or a rootname 
    (e.g. LD201020)

    Parameters:
    -----------
        mystring : str
            String which describes data to be retrieved.

    Returns:
    --------
        status : Boolean
            True if mystring is a proposal ID.
    '''

    try:
        prop = int(mystring)
        status = True
    except ValueError:
        status = False

    return status

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def exit_handler():
    print("The script is crashing for an unknown reason!")
    import pickle
    pickle.dump({"badness": 10000}, open("crash.p", "wb"))

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

#atexit.register(exit_handler)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # This is a required argument. To input multiple arguments, they must be
    # SPACE separated, not comma separated.
    parser.add_argument("data", nargs="+",  help="Reprocess programs or datasets")
    parser.add_argument("--prl", dest="prl", action="store_true",
                        default=False, help="Switch to arallellize manualabor")
    parser.add_argument("--labor", dest="run_labor", action="store_true",
                        default=False, help="Switch to run manualabor")
    parser.add_argument("--chmod", dest="do_chmod", action="store_true",
                        default=True, help="Switch to turn on chmod")
    args = parser.parse_args()

    data = parse_input(args.data)
    print("Requesting: {0}".format(data))
    
    all_data = []
    for item in data:
        if item.upper() == "CCI":
            all_data += handle_cci()
            continue
        isprop = is_proposal(item)
        if isprop:
            prop_datasets = query_proposal(item)
            all_data += prop_datasets
        else:
            root_datasets = handle_datasets(item)        
            all_data += root_datasets

    to_retrieve = { (row[0]):(row[1] if row[1]=="CCI" else int(row[1])) for row in all_data}
    prop_keys = list(set(to_retrieve.values()))
    prop_vals = [[] for x in range(len(prop_keys))]
    prop_dict = dict(zip(prop_keys, prop_vals))
    for key in to_retrieve.keys():
        prop_dict[to_retrieve[key]].append(key)

    copy_cache(prop_dict, args.prl, args.do_chmod)
    run_all_retrievals(prop_dict, None, args.prl, args.do_chmod)
