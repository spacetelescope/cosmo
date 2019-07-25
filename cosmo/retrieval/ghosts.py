#! /usr/bin/env python

from __future__ import print_function, absolute_import, division

"""
This module is to store legacy functions that were meant to be implemented 
in the retrieval process but are not currently used.
"""


# --------------------------------------------------------------------------- #


# from find_new_cos_data.py

# def connect_cosdb():
#     """
#     Connect to the COS team's database, cos_cci, on the server greendev to
#     determine which COS datasets are currently in the local repository.
#
#     Parameters:
#     -----------
#         None
#
#     Returns:
#     --------
#         all_smov : list
#             All rootnames of all files in the COS greendev database.
#     """
#
#     # Open the configuration file for the COS database connection (MYSQL).
#     config_file = os.path.join(os.environ['HOME'], "configure.yaml")
#     with open(config_file, 'r') as f:
#         SETTINGS = yaml.load(f)
#
#         print("Querying COS greendev database for existing data...")
#         # Connect to the database.
#         Session, engine = load_connection(SETTINGS['connection_string'])
#         sci_files = list(engine.execute("SELECT DISTINCT rootname FROM
#         files "
#                                         "WHERE rootname IS NOT NULL;"))
#         cci_files = list(engine.execute("SELECT DISTINCT name FROM files "
#                                         "WHERE rootname IS NULL AND "
#                                         "LEFT(name,1)='l';"))
#
#         # Store SQLAlchemy results as lists
#         all_sci = [row["rootname"].upper() for row in sci_files]
#         all_cci = [row["name"].strip("_cci.fits.gz").upper() for row in
#         cci_files]
#         all_smov = all_sci + all_cci
#
#         # Close connection
#         engine.dispose()
#
#         return all_smov

# def ensure_no_pending():
#     """
#     Check for any pending archive requests, and if there are any, wait until
#     they finish.
#
#     Parameters:
#     -----------
#         None
#
#     Returns:
#     --------
#         Nothing.
#
#     """
#     num_requests, badness, status_url = check_for_pending()
#     while num_requests > 0:
#         print("There are still {0} requests pending from a previous COSMO "
#               "run, waiting 5 minutes...".format(num_requests))
#         assert (badness,
#                 f"Something went wrong during requests, check {status_url}")
#         time.sleep(300)
#         num_requests, badness, status_url = check_for_pending()
#     else:
#         print("All pending requests finished, moving on!")

# def copy_entire_cache(cos_cache):
#     """
#     In development.
#     """
#     prop_map = {}
#     for item in cos_cache:
#         filename = os.path.basename(item)
#         ippp = filename[:4]
#
#         if not ippp in prop_map.keys():
#             hdr0 = pf.getheader(item,0)
#             proposid = hdr0["proposid"]
#             prop_map[ippp] = proposid
#         else:
#             proposid = prop_map[ippp]
#
#         dest = os.path.join(BASE_DIR, proposid, filename)
#         # By importing pyfastcopy, shutil performance is automatically
#         enhanced
#         shutil.copyfile(item, dest)
