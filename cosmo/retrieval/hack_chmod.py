from __future__ import print_function
from distutils.spawn import find_executable

import os
import subprocess

"""
A simple script written to mimic the functionality of 'chmod -R'. os.chmod is 
time consuming when done recursively, this workaround provides even faster 
computation than a simple subprocess call to 'chmod -R'.

Written by Joe Hunkeler: 
https://gist.github.com/jhunkeler/7a7761d8ce0f66d36483d6a6038caa4d
and commented by Jo Taylor.
"""


def find(root='.', filetype=None):
    """
    Simple BSD/GNU find wrapper.

    Parameters:
    -----------
        root : str
            Name of directory to be modified.
        filetype : str
            "f" = traverse only files
            "d" = traverse only directories
            None = traverse both files and directories.

    Returns:
    --------
        Directory or filename to be modified.
    """

    accepted_types = ['f', 'd']
    cmd = ['find', root]

    if not find_executable('find'):
        raise OSError('Unable to locate "find" program.'
                      'Cannot continue.')

    if filetype is not None:
        assert isinstance(filetype, str)
        if len(filetype) > 1 or filetype not in accepted_types:
            raise ValueError('Unsupported file type: "{0}"'.format(filetype))

        selector = '-type {0}'.format(filetype).split()
        cmd += selector

    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as cpe:
        print('Failed to execute: "{0}" (exit: {1})'.format(cpe.cmd,
                                                            cpe.returncode))

        print('Failure message: {0}'.format(cpe.stderr or None))
        exit(cpe.returncode)

    for x in output.splitlines():
        yield os.path.abspath(x) or None


def chmod(basepath, mode, filetype=None, recursive=False):
    """
    Recursive-capable replacement for os.chmod.

    Parameters:
    -----------
        basepath : str
            Name of directory to be modified.
        mode : int (octal)
            Permission for directory/files to be set to.
        filetype : str
            "f" = traverse only files
            "d" = traverse only directories
            None = traverse both files and directories.
        recursive : Bool
            Switch to change permissions recursively.

    Returns:
    --------
        Nothing
    """
    
    assert(isinstance(basepath, str))
    assert(isinstance(mode, int))
    assert(bool(filetype is None or isinstance(filetype, str)))
    assert(isinstance(recursive, bool))

    if mode < 0 or mode > 0o7777:
        raise ValueError('Invalid mode: {0}'.format(oct(mode)))

    print("Changing permissions on {0} to {1}".format(basepath, mode)) 
    if recursive:
        for path in find(basepath, filetype):
            os.chmod(path, mode)
    else:
        os.chmod(basepath, mode)
