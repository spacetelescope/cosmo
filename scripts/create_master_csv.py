import os
import yaml
from cos_monitoring.database.db_tables import load_connection
from astropy.io import ascii
from astropy.table import Table

# add optional filename output to csv_generator
# add documentation to the csv_generator function

#-- genrate astropy table and output csv file.
def csv_generator(headers, keywords, filename):
    """
    Pulls headers and keywords from SQL query to make a csv table.

    Parameters
    ----------
    headers : class (<class 'sqlalchemy.engine.result.RowProxy'>)
        All of the header information returned from SQL query.
    keywords : list
        A list of column names returned from SQL query.
    filename : str
        The names of CSV output file.

    Returns
    -------
    None

    Outputs
    -------
    t : CSV file
        A CSV files that contains all of the queried header information.
    """
    datarows = []
    for item in headers:
        datarows.append(item)
    print(type(item))
    t = Table(rows = datarows, names = keywords, meta = {'Name':'COS HEADER TABLE'})
    ascii.write(t, filename + '.csv', format='csv')



def main():

    print("Querying the whole headers table...enjoy")

    #-- load the configuration settings from the config file
    config_file = os.path.join(os.environ['HOME'], "configure.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

    #-- setup a connection to the databse
    Session, engine = load_connection(SETTINGS['connection_string'])
    results = engine.execute("SELECT * FROM headers ORDER BY expstart;")
    keys = results.keys()
    file_name = 'all_file'
    csv_generator(results,keys,file_name)

    #-- close connections
    engine.dispose()


if __name__ == "__main__":
    main()
