import os
import yaml
from cos_monitoring.db_tables import load_connection

def main():

    print("Querying the whole headers table...enjoy")

    #-- load the configuration settings from the config file
    config_file = os.path.join(os.environ['HOME'], "configure.yaml")
    with open(config_file, 'r') as f:
        SETTINGS = yaml.load(f)

    #-- setup a connection to the databse
    Session, engine = load_connection(SETTINGS['connection_string'])
    results = engine.execute("SELECT * FROM headers;")
    for item in results:
        print(item)


    #-- your code goes here






    #-- close connections
    engine.dispose()


if __name__ == "__main__":
    main()
