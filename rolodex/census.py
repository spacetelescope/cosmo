import os

#-------------------------------------------------------------------------------

def find_all_datasets(data_dir):
    """Iterator to yield all datasets

    Parameters
    ----------
    data_dir : str
        directory to search over for files

    """

    for root, dirs, files in os.walk(data_dir):
        for filename in files:
            if not '.fits' in filename:
                continue

            yield (root, filename)

#-------------------------------------------------------------------------------
