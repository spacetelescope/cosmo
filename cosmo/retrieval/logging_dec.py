#! /usr/bin/env python

import os
import logging
import inspect
from functools import wraps

def config_logging(logfile):
    '''
    Define the logging configuration including the output file and
    logging level.

    Parameters:
    -----------
        logfile : string
            The path and filename of the output logging file.

    Returns:
    --------
        Nothing
    '''
    logging.basicConfig(filename=logfile,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S %p',
                        level=logging.WARNING)

def log_function(func):
    '''
    This is a decorator to be used to log modules and retain important
    information and errors.

    Use:
    ----
        This should be imported and used as a decorator:

        from logging_dec import log_function

        @log_function
        def my_function():
            ...

    Parameters:
    -----------
        func : function
            The input function to decorate

    Returns:
    --------
        wrapper : function
            A wrapper to the modified function.
    '''
    @wraps(func)
    def wrapper(*args, **kwargs):
        funcname = str(func.__name__)
        scriptname = str(inspect.getmodule(func)).split("'")[-2]
        modname = scriptname.split(".")[0]
        logfile = os.path.join(os.getcwd(), modname+".log")
        config_logging(logfile)
        try:
            func(*args, **kwargs)
            logging.info("{0} completed successfully".format(funcname))
        except Exception as err:
            logging.exception("Error in function {0}.{1}: ".format(modname,funcname))

    return wrapper
