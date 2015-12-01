import os
import yaml
from cos_monitoring.database.db_tables import load_connection
from astropy.io import ascii
from astropy.table import Table

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


    #-- genrate astropy table and output csv file.
    def csv_generator(headers, keywords):
        datarows = []
        for item in headers:
            datarows.append(item)
        t = Table(rows = datarows, names = keywords, meta = {'Name':'COS HEADER TABLE'})
        t.show_in_browser(jsviewer=True)
        ascii.write(t, 'all_header.csv', format='csv')

    csv_generator(results,keys)

    #-- close connections
    engine.dispose()


if __name__ == "__main__":
    main()
