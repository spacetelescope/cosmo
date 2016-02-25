#! /usr/bin/env python
'''
Contains functions that facilitate the downloading of COS data
via XML requests to MAST. 

Use:
    This script is intended to be imported by other modules that 
    download COS data.
'''

__author__ = "Jo Taylor"
__date__ = "02-25-2016"
__maintainer__ = "Jo Taylor"
__email__ = "jotaylor@stsci.edu"

import re
import pickle
import os
import urllib
import string
import httplib
from datetime import datetime
import time
import pdb

from SignStsciRequest import SignStsciRequest

MAX_RETRIEVAL = 20

REQUEST_TEMPLATE = string.Template('\
<?xml version=\"1.0\"?> \n \
<!DOCTYPE distributionRequest SYSTEM \"http://dmswww.stsci.edu/dtd/sso/distribution.dtd\"> \n \
  <distributionRequest> \n \
    <head> \n \
      <requester userId = \"$archive_user\" email = \"$email\" source = "starview"/> \n \
      <delivery> \n \
        <ftp hostName = \"$ftp_host\" loginName = \"$ftp_user\" directory = \"$ftp_dir\" secure = \"true\" /> \n \
      </delivery> \n \
      <process compression = \"none\"/> \n \
    </head> \n \
    <body> \n \
      <include> \n \
        <select> \n \
          $suffix\n \
        </select> \n \
        $datasets \n \
      </include> \n \
    </body> \n \
  </distributionRequest> \n' )

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def build_xml_request(dest_dir, datasets):
    '''
    Build the xml request string.

    Parameters: 
        dest_dir : string
            The path fo the directory in which the data will be downloaded.
        datasets : list
            A list of datasets to be requested.

    Returns:
        xml_request : string
            The xml request string.
    '''
    archive_user = "jotaylor"
    email = "jotaylor@stsci.edu"
    host = "plhstins1.stsci.edu"
    ftp_user = "jotaylor"
    ftp_dir = dest_dir
    # Not currently using suffix dependence.
    suffix = "<suffix name=\"*\" />"
    dataset_str = "\n"
    for item in datasets:
        dataset_str = "{0}          <rootname>{1}</rootname>\n".format(dataset_str,item)

    request_str = REQUEST_TEMPLATE.safe_substitute(
        archive_user = archive_user,
        email = email,
        ftp_host = host,
        ftp_dir =ftp_dir,
        ftp_user = ftp_user,
        suffix = suffix,
        datasets = dataset_str)
    xml_request = string.Template(request_str)
    xml_request = xml_request.template
         
    return xml_request

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def submit_xml_request(xml_file):
    '''
    Submit the xml request to MAST.

    Parameters: 
        xml_file : string
            The xml request string.

    Returns:
        response : httplib object
            The xml request submission result.
    '''
    home = os.environ.get("HOME")
    user = os.environ.get("USER")
    
    signer = SignStsciRequest()
    request_xml_str = signer.signRequest("{0}/.ssh/privkey.pem".format(home), 
                                         xml_file)
    params = urllib.urlencode({
        "dadshost": "dmsops1.stsci.edu",
        "dadsport": 4703,
        "mission": "HST",
        "request": request_xml_str})
    headers = {"Accept": "text/html", 
               "User-Agent": "{0}PythonScript".format(user)}
    req = httplib.HTTPSConnection("archive.stsci.edu")
    req.request("POST", "/cgi-bin/dads.cgi", params, headers)
    response = req.getresponse().read()
    
    return response

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def retrieve_data(dest_dir, datasets):
    '''
    For a given list of datasets, submit an xml request to MAST
    to retrieve them to a specified directory.
    
    Parameters: 
        dest_dir : string
            The path to the directory to which the data will be downloaded.
        datasets : list
            A list of datasets to be requested.

    Returns:
        tracking_ids : list
            The tracking IDs for all submitted for a given program.
    '''
    dataset_lists = (datasets[i:i+MAX_RETRIEVAL] 
                     for i in xrange(0, len(datasets), MAX_RETRIEVAL))
    tracking_ids = []
    for item in dataset_lists:
        xml_file = build_xml_request(dest_dir, item)
        result = submit_xml_request(xml_file)
        tmp_id = re.search('(jotaylor[0-9]{5})', result).group()
        print tmp_id
        tracking_ids.append(tmp_id)
   
    return tracking_ids

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

def everything_retrieved(tracking_id):
    '''
    Check every 15 minutes to see if all submitted datasets have been 
    retrieved. Based on code from J. Ely.

    Parameters:
        tracking_id : string
            A submission ID string..

    Returns:
        done : bool
            Boolean specifying is data is retrieved or not.
        killed : bool
            Boolean specifying is request was killed.
    '''
    
    done = False
    killed = False
#    print tracking_id
    status_url = "http://archive.stsci.edu/cgi-bin/reqstat?reqnum=={0}".format(tracking_id)
    for line in urllib.urlopen(status_url).readlines():
        if "State" in line:
            if "COMPLETE" in line:
                done = True
            elif "KILLED" in line:
                killed = True
    return done, killed

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    # This is to test retrieval. It will be removed later.
    
    pstart = 0
    pend = 10
    prop_dict = pickle.load(open("filestoretrieve.p", "rb"))
#    prop_dict_keys = [12419, 12467,12510,12545,12607]
    prop_dict_keys = prop_dict.keys()
    all_tracking_ids = []

    while pend < len(prop_dict_keys): 
        for prop in prop_dict_keys[pstart:pend]:
            prop_dir = "/grp/hst/cos2/smov_testing/{0}".format(prop)
            if not os.path.exists(prop_dir):
                os.mkdir(prop_dir)
            os.chmod(prop_dir, 0755)
            print("I am retrieving %s datasets for %s" % (len(prop_dict[prop]),prop))
            ind_id = retrieve_data(prop_dir, prop_dict[prop])
            for item in ind_id:
                all_tracking_ids.append(item)
    
        counter = []
        num_ids = len(all_tracking_ids)
        while sum(counter) < (num_ids - 5):
            counter = []
            for tracking_id in all_tracking_ids:
                status,badness = everything_retrieved(tracking_id)
                counter.append(status)
                if badness:
                    print("!!!RUH ROH!!! Request {0} was KILLED!".format(tracking_id))
                    counter.append(badness)
            current_retrieved = [all_tracking_ids[i] for i in xrange(len(counter)) if not counter[i]]
            print datetime.now()
            print("Data not yet delivered for {0}. Checking again in 15 minutes".format(current_retrieved))
            time.sleep(900)
        else:
            print("\nData delivered, adding another program to queue")
            pstart = pend
            pend += 5


#    prop_dict = pickle.load(open("filestoretrieve.p", "rb"))
#    for prop in prop_dict.keys()[pstart:pend]:
#        prop_dir = "/grp/hst/cos2/smov_testing/{0}".format(prop)
#        if not os.path.exists(prop_dir):
#            os.mkdir(prop_dir)
#        os.chmod(prop_dir, 0755)
#        print("I am retrieving %s datasets for %s" % (len(prop_dict[prop]),prop))
#        retrieve_data(prop_dir, prop_dict[prop])
#


#    retrieve_data("/grp/hst/cos3/smov_testing/retrieval",data.values())

