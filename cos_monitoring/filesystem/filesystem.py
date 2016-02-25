import os

#-------------------------------------------------------------------------------

def find_all_datasets(data_dir):
    """Iterator to yield all datasets recursively from the base.

    Only files that include the .fits extension will be returned.

    Parameters
    ----------
    data_dir : str
        directory to search over for files

    Yields
    ----------
    root, filename : tuple
        root path and filename of each found .fits file

    """

    for root, dirs, files in os.walk(data_dir):
        for filename in files:
            if not '.fits' in filename:
                continue

            yield (root, filename)

#-------------------------------------------------------------------------------
