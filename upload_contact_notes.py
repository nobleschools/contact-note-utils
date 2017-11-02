"""
upload_contact_notes.py

Upload contact notes to Salesforce from a csv in either the persistence reports
or Facebook notes format.

Verifies Network ID against alum name and class year using Elastic,
and checks for duplicates using Date_of_Contact__c and Mode_of_Communication__c.
"""

import argparse
import csv
from datetime import datetime
from os import path

from elasticsearch_dsl.connections import connections as es_connections
from elasticsearch_dsl import Search
from simple_salesforce import Salesforce

from common_date_formats import COMMON_DATE_FORMATS
from salesforce_utils.constants import (
    CAMPUS_SF_IDS,
    SALESFORCE_DATESTRING_FORMAT,
    ELASTIC_MATCH_SCORE,
)
from salesforce_utils.get_connection import get_salesforce_connection
from header_mappings import HEADER_MAPPINGS
from noble_logging_utils.papertrail_logger import (
    get_logger,
    SF_LOG_LIVE,
    SF_LOG_SANDBOX,
)
from salesforce_fields import contact_note as cn_fields
from secrets.elastic_secrets import ES_CONNECTION_KEY


campuses = CAMPUS_SF_IDS.keys()


def upload_contact_notes(input_file, campus, source_date_format):
    """
    Upload Contact Notes to Salesforce.
    """
    logger.info("Starting Contact Note upload..")

    COUNT_CONTACT_NOTES_QUERY = "SELECT COUNT() FROM Contact_Note__c"
    pre_uploads_count = \
        sf_connection.query(COUNT_CONTACT_NOTES_QUERY)['totalSize']

    skipped_count = created_count = 0

    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            # Date_of_Contact__c
            datestring = _convert_time_format(
                row[cn_fields.DATE_OF_CONTACT], source_date_format
            )
            row[cn_fields.DATE_OF_CONTACT] = datestring

            # Contact__c
            try:
                safe_id = row[cn_fields.CONTACT]
            except KeyError:
                safe_id = get_safe_id(campus, **row)
                if not safe_id:
                    logger.warn("No Safe ID found for {}".format(row))
                    continue
                row[cn_fields.CONTACT] = safe_id

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


def get_safe_id(campus, **kwargs):
    """
    Get safe_id for alum from their Network ID and full name (ensuring
    both match).

    Returns str safe_id or None
    """

    # assumed...
    full_name = kwargs['First Name'] + ' ' + kwargs['Last Name']
    network_id = kwargs['Network ID']

    safe_id_query = {
        "min_score": ELASTIC_MATCH_SCORE,
        "query": {
            "bool": {
                "must": [{
                    "match": {
                        "_id": {
                            "query": network_id,
                            "boost": 2,
                        },
                    },
                }],
                "should": [{
                    "match": {
                        "full_name": {
                            "query": full_name,
                            "fuzziness": 2,
                        }
                    }
                }],
            },
        }
    }
    s = Search().from_dict(safe_id_query)
    s = s.index(campus)
    results = s.execute()

    # no one match found; could return more info if multiple
    if not len(results) == 1:
        return None
    return results[0].safe_id


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

# TODO use salesforce_utils.make_salesforce_datestr
def _convert_time_format(source_datestring, source_date_format):
    """
    Convert source_datestring in source_format to Salesforce-ready '%Y-%M-%d'.

    If adding the year to the date, assumes we aren't uploading notes >1yr old,
    thus
        if contact month <= current month:
            use current year
        else:
            use last year
    """

    source_dateobj = datetime.strptime(source_datestring, source_date_format)

    if source_dateobj.year == 1900:
        # year was not specified
        today = datetime.today()
        current_month, current_year = today.month, today.year
        if source_dateobj.month <= current_month:
            # assume same year
            source_dateobj = source_dateobj.replace(year=current_year)
        else:
            # assume last year
            source_dateobj = source_dateobj.replace(year=current_year-1)

    return source_dateobj.strftime(SALESFORCE_DATESTRING_FORMAT)


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


def _request_campus():
    """
    If the relevant campus isn't passed as an argument to the script, prompt
    user.
    """
    enumerated_campuses = dict(enumerate(campuses))

    for position, campus_name in enumerated_campuses.items():
        print("{}) {}".format(position, campus_name))
    input_num = input("Enter number of campus to use to confirm alumni IDs: ")

    return enumerated_campuses[int(input_num)] # or go down in flames


def _request_source_date_format():
    """
    Prompt user for the datestring format in the input.
    """
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

    parser = argparse.ArgumentParser(description=\
        "Specify input csv file and campus"
    )
    parser.add_argument(
        "infile",
        help="Input file (in csv format)"
    )
    parser.add_argument(
        "--campus",
        default=None,
        help="Campus (index) the notes are for"
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

    if not args.campus or args.campus.lower() in campuses:
        campus = _request_campus()

    source_date_format = _request_source_date_format()

    log_job_name = __file__.split(path.sep)[-1] # name of this file

    if args.sandbox:
        logger = get_logger(log_job_name, hostname=SF_LOG_SANDBOX)
        logger.info("Connecting to sandbox Salesforce instance..")
    else:
        logger = get_logger(log_job_name, hostname=SF_LOG_LIVE)
        logger.info("Connecting to live Salesforce instance..")

    elastic_connection = es_connections.create_connection(
        hosts=[ES_CONNECTION_KEY], timeout=30
    )

    sf_connection = get_salesforce_connection(sandbox=args.sandbox)
    upload_contact_notes(args.infile, campus, source_date_format)

    # ??
    logger.handlers[0].close()
