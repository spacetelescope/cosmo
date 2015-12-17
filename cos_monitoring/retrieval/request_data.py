#! /usr/bin/env python
'''
Contains functions that facilitate the downloading of COS data
via XML requests to MAST. 

Authors:
    Jo Taylor, Dec. 2015

Use:
    This script is intended to be imported by other modules that 
    download COS data.
'''

import pickle
import os
import urllib
import string
import httplib
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
        Nothing
    '''
    dataset_lists = (datasets[i:i+MAX_RETRIEVAL] 
                     for i in xrange(0, len(datasets), MAX_RETRIEVAL))
    for item in dataset_lists:
        xml_file = build_xml_request(dest_dir, item[0])
        result = submit_xml_request(xml_file)
        print xml_file
        print result

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

if __name__ == "__main__":
    # This is to test retrieval. It will be removed later.
    data = pickle.load(open("test.p", "rb"))
    retrieve_data("/grp/hst/cos3/smov_testing/retrieval",data.values())
