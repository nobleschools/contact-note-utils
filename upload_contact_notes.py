"""
upload_contact_notes.py

Upload contact notes to Salesforce from a csv.

Checks for duplicates using Subject__c, Date_of_Contact__c and
Mode_of_Communication__c fields.

TODO Refactor with noble-salesforce-utils; confirm ID and name against Elastic.
"""

import argparse
import csv
from os import path

from common_date_formats import COMMON_DATE_FORMATS
from salesforce_utils import (
    get_salesforce_connection,
    make_salesforce_datestr,
)
from header_mappings import HEADER_MAPPINGS
from noble_logging_utils.papertrail_logger import (
    get_logger,
    SF_LOG_LIVE,
    SF_LOG_SANDBOX,
)
from salesforce_fields import contact_note as cn_fields


def upload_contact_notes(input_file, source_date_format):
    """Upload Contact Notes to Salesforce."""
    logger.info("Starting Contact Note upload..")

    COUNT_CONTACT_NOTES_QUERY = "SELECT COUNT() FROM Contact_Note__c"
    pre_uploads_count = \
        sf_connection.query(COUNT_CONTACT_NOTES_QUERY)['totalSize']

    skipped_count = created_count = 0

    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            # Date_of_Contact__c
            datestring = make_salesforce_datestr(
                row[cn_fields.DATE_OF_CONTACT], source_date_format
            )
            row[cn_fields.DATE_OF_CONTACT] = datestring

            # Contact__c
            # TODO handle in a way that allows easy retried of any failed
            safe_id = row[cn_fields.CONTACT]

            possible_dupe = check_for_existing_contact_note(
                datestring, safe_id, row[cn_fields.SUBJECT]
            )
            if possible_dupe:
                skipped_count += 1
                logger.warn("Found possible duplicate ({}) for {}".format(
                    possible_dupe, row
                ))
                continue

            # Initiated_by_alum__c; typical of Facebook note uploads
            try:
                row[cn_fields.INITIATED_BY_ALUM] = \
                    _string_to_bool(row[cn_fields.INITIATED_BY_ALUM])
            except KeyError:
                pass

            # grab only valid Contact Note fields
            contact_note_data = {}
            for field_name, value in row.items():
                if field_name in HEADER_MAPPINGS.values():
                    contact_note_data[field_name] = value
            was_successful = _upload_note(contact_note_data)
            if was_successful:
                created_count += 1

    logger.info("{} notes uploaded, {} skipped".format(
        created_count, skipped_count
    ))

    post_uploads_count = \
        sf_connection.query(COUNT_CONTACT_NOTES_QUERY)['totalSize']
    assert post_uploads_count == pre_uploads_count + created_count


def _upload_note(args_dict):
    """
    Upload the note. Assumes the following minimum kwargs:
    * ...
    ...
    Returns success status as a bool.
    """

    response = sf_connection.Contact_Note__c.create(args_dict)
    if response['success']:
        logger.info("Uploaded Contact Note {} successfully".format(response['id']))
    else:
        logger.warn("Upload failed: {}. Kwargs: {}".format(response['errors'], **kwargs))
    return response['success']


def _string_to_bool(boolstring):
    """Convert string 'True'/'False' to python bool for Salesforce API call."""
    boolstring = boolstring.lower()
    if boolstring == 'true':
        return True
    elif boolstring == 'false':
        return False


def check_for_existing_contact_note(datestring, alum_safe_id, subject):
    """
    Check for existing contact note by date, Contact, and Subject.

    If any exist, return ID for the first found Contact Note, otherwise
    return None.

    Arguments:
    * datestring: must be formatted 'YYYY-MM-DD'
    * alum_safe_id: alum's safe id
    * subject: str value for Subject__c field
    """

    contact_note_query = (
        "SELECT Id "
        "FROM Contact_Note__c "
        "WHERE Date_of_Contact__c = {} "
        "AND Contact__c = '{}' "
        "AND Subject__c = '{}'"
    )

    results = sf_connection.query(contact_note_query.format(
        datestring, alum_safe_id, subject
    ))
    if results['totalSize']:
        # may be more, but needs investigation either way
        return results['records'][0]['Id']
    return None


def _request_source_date_format():
    """Prompt user for the datestring format in the input."""
    enumerated_date_formats = dict(enumerate(COMMON_DATE_FORMATS))

    for position, date_format in enumerated_date_formats.items():
        print("{}) {}".format(position, date_format))
    input_num = input("Enter number that corresponds to source datestring format: ")

    return enumerated_date_formats[int(input_num)]


def parse_args():
    """
    *    infile: input csv file, formatted and ready to upload to Salesforce
    * --sandbox: if present, connects to the sandbox Salesforce instance.
                 Otherwise, connects to live
    """

    parser = argparse.ArgumentParser(description="Specify input csv file")
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

    source_date_format = _request_source_date_format()

    log_job_name = __file__.split(path.sep)[-1] # name of this file

    if args.sandbox:
        logger = get_logger(log_job_name, hostname=SF_LOG_SANDBOX)
        logger.info("Connecting to sandbox Salesforce instance..")
    else:
        logger = get_logger(log_job_name, hostname=SF_LOG_LIVE)
        logger.info("Connecting to live Salesforce instance..")

    sf_connection = get_salesforce_connection(sandbox=args.sandbox)
    upload_contact_notes(args.infile, source_date_format)

    # ??
    logger.handlers[0].close()
