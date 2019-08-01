import os

TEST_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cosmoconfig_test.yaml')

# Check to make sure that the test config file is being used. If not, don't run the tests
if os.environ['COSMO_CONFIG'] != TEST_CONFIG:
    raise TypeError('Tests should only be executed with the testing configuration file')
