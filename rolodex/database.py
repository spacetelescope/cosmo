from __future__ import print_function

import os
from sqlalchemy.engine import create_engine
import yaml

from census import find_all_datasets

#-------------------------------------------------------------------------------

def insert_files(**kwargs):
    """Create table of path, filename"""

    connection_string = kwargs['connection_string']
    data_location = kwargs.get('data_location', './')

    engine = create_engine(connection_string)
    with engine.connect() as connection:
        try:
            connection.execute("""CREATE TABLE files (path text, name text);""")
            connection.execute("""CREATE INDEX path ON files (path);""")
        except:
            connection.execute("""DELETE FROM files;""")

        for path, filename in find_all_datasets(data_location):
            print("Found {}".format(os.path.join(path, filename)))
            connection.execute("""INSERT INTO files VALUES ('{}', '{}');""".format(path,
                                                                            filename))

#-------------------------------------------------------------------------------

if __name__ == "__main__":
    with open('../configure.yaml', 'r') as f:
        settings = yaml.load(f)

    print(settings)
    insert_files(**settings)
