import os

SETTINGS = {
    'filesystem': {'source': os.environ['COSMO_FILES_SOURCE']},
    'output': os.environ['COSMO_OUTPUT'],
    'sms': {
        'source': os.environ['COSMO_SMS_SOURCE'],
        'db_settings': {
            'database': os.environ.get('COSMO_SMS_DB', 'sms.db'),
            'pragmas': {
                'journal_mode': os.environ.get('COSMO_SMS_DB_JOURNAL', 'wal'),
                'foreign_keys': os.environ.get('COSMO_SMS_DB_FORIEGN_KEYS', 1),
                'ignore_check_constraints': os.environ.get('COSMO_SMS_DB_IGNORE_CHECK_CONSTRAINTS', 0),
                'synchronous': os.environ.get('COSMO_SMS_DB_SYNCHRONOUS', 0)
            }
        }
    },
}
