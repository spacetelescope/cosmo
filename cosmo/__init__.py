import os
import yaml

with open(os.environ['COSMO_CONFIG']) as yamlfile:
    SETTINGS = yaml.safe_load(yamlfile)
