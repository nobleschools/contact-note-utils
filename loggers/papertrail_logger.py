"""
papertrail_logger.py

Logging setup to push log messages to Papertrail and stdout. Adapted from
http://help.papertrailapp.com/kb/configuration/configuring-centralized-logging-from-python-apps/
"""

import logging
from logging.handlers import SysLogHandler
from os import path
import sys

PAPERTRAIL_LOG_FORMAT = '%(asctime)s %(hostname)s %(jobname)s: [%(levelname)s] %(message)s'
PAPERTRAIL_DATE_FORMAT = '%b %d %H:%M:%S'

SF_LOCAL_HOSTNAME = 'salesforce-local'
SF_LOG_LIVE = 'salesforce-live'
SF_LOG_SANDBOX = 'salesforce-sandbox'


class PapertrailContextFilter(logging.Filter):

    def __init__(self, hostname, jobname, *args, **kwargs):
        # To conform to log coloration on PT, which splits by whitespace
        self.hostname = hostname.replace(' ', '')
        self.jobname = jobname.replace(' ', '')
        super().__init__(*args, **kwargs)

    def filter(self, record):
        record.hostname = self.hostname
        record.jobname = self.jobname
        return True


def get_logger(destination_address, destination_port, jobname,
               hostname=SF_LOCAL_HOSTNAME):
    """
    Creates a logger with the `PapertrailContextFilter`, pointed at an
    address and port of a PT log destination.

    Positional arguments:

    * destination_address: target web address of logging destination
    * destination_port:    target port
    * jobname:             job name to display in the Papertrail log stream

    Available keyword arguments:

    * hostname: hostname to display in the Papertrail log stream. Also becomes
                a 'system' in PT within the particular destination
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG) # allow all by default
    pt_filter = PapertrailContextFilter(hostname, jobname)
    logger.addFilter(pt_filter)
    syslog = SysLogHandler(address=(destination_address, destination_port))
    formatter = logging.Formatter(
        PAPERTRAIL_LOG_FORMAT, datefmt=PAPERTRAIL_DATE_FORMAT
    )
    syslog.setFormatter(formatter)
    logger.addHandler(syslog)
    local_handler = logging.StreamHandler(sys.stdout)
    local_handler.setLevel(logging.DEBUG)
    logger.addHandler(local_handler)

    return logger

    #syslog.close() ?

if __name__ == '__main__':
    pass
