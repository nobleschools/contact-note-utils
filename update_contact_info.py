"""
update_contact_info.py

Try updating Contact fields, from a csv.

Takes a csv as input, using Safe_Id__c column for alum identity.
"""

import argparse
import csv
from datetime import datetime
from os import path

from noble_logging_utils.papertrail_logger import (
    get_logger,
    SF_LOG_SANDBOX,
    SF_LOG_LIVE,
)
from salesforce_utils import get_salesforce_connection
from salesforce_fields import contact as contact_fields
#from . import salesforce_secrets

# TODO parameterize
FIELDS_TO_UPDATE = (
    #contact_fields.EMAIL,
    contact_fields.MOBILE,
)


def update_contact_info(input_file, sf_connection):
    """
    Update Contact (alum) info in Salesforce from the input_file csv.
    """
    logger.info("Starting Contact updates..")

    num_updated = 0

    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            alum_safe_id = row[contact_fields.SAFE_ID]

            # to log change
            alum_before = sf_connection.Contact.get(alum_safe_id)
            alum_sf_name = alum_before['Name']

            # For the new_data, if a value is blank, remove the field
            # so as not to overwrite any existing data in Salesforce.
            new_data = _filter_data_dict(row, FIELDS_TO_UPDATE)
            new_data = {k:v for k,v in new_data.items() if v != ''}

            # only log the fields for which updates were sent
            old_data = _filter_data_dict(alum_before, new_data.keys())

            sf_connection.Contact.update(alum_safe_id, new_data)
            num_updated += 1

            logger.info("Updated Contact {} ({}): FROM {} TO {}".format(
                alum_sf_name, alum_safe_id, old_data, new_data
            ))

    logger.info("Updated {} Contacts.".format(num_updated))


def _filter_data_dict(data_dict, keys_iter):
    """
    Helper function to pare down data_dict to only include fields
    in the keys_iter.
    """
    return {k:v for k,v in data_dict.items() if k in keys_iter}


def parse_args():
    """
    *    infile: input csv file, formatted and ready to upload to Salesforce
    * --sandbox: if present, connects to the sandbox Salesforce instance.
                 Otherwise, connects to live
    """

    parser = argparse.ArgumentParser(description=\
        "Specify input csv file and campus"
    )
    parser.add_argument(
        "infile",
        help="Input file (in csv format)"
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        default=False,
        help="If True, uses the sandbox Salesforce instance. Defaults to False"
    )
    return parser.parse_args()


if __name__=="__main__":
    args = parse_args()
    log_job_name = __file__.split(path.sep)[-1]

    if args.sandbox:
        logger = get_logger(log_job_name, hostname=SF_LOG_SANDBOX)
        logger.info("Connecting to sandbox Salesforce instance..")
    else:
        logger = get_logger(log_job_name, hostname=SF_LOG_LIVE)
        logger.info("Connecting to live Salesforce instance..")

    sf_connection = get_salesforce_connection(sandbox=args.sandbox)
    update_contact_info(args.infile, sf_connection)

    # ??
    logger.handlers[0].close()
