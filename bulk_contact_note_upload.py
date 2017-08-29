"""
bulk_contact_note_upload.py

XXX !!! XXX !!!
Expected this will fail with an

AttributeError:'SFType' object has no attribute 'Contact_Note__c'

(whatever object the bulk operation is working on). Will work in the IDLE,
however.
!!! XXX !!! XXX

Upload contact notes to Salesforce from a csv using the
`simple_salesforce.Salesforce.bulk` API.

This implementation accommodates an input format for the 'Subject' and
'Comments' fields, where the column header is the 'Subject' text and the
column data will be placed in the 'Comments' field.

TODO: Verify Network ID against alum name and class year using Elastic,
and checks for duplicates using Date_of_Contact__c, etc.
"""

import argparse
import csv
from datetime import datetime
import sys
from os import pardir, path
filepath = path.abspath(__file__)
parent_dir = path.abspath(path.join(filepath, pardir))
package_dir = path.abspath(path.join(parent_dir, pardir))
sys.path.insert(0, package_dir)

from elasticsearch_dsl.connections import connections as es_connections
from elasticsearch_dsl import Search
from simple_salesforce import Salesforce

from common_date_formats import COMMON_DATE_FORMATS
from constants import (
    CAMPUS_SF_IDS,
    SALESFORCE_DATESTRING_FORMAT,
    ELASTIC_MATCH_SCORE,
)
from header_mappings import HEADER_MAPPINGS
from loggers.papertrail_logger import get_logger, SF_LOG_LIVE, SF_LOG_SANDBOX
from salesforce_fields import contact_note as cn_fields
from secrets.logging import SF_LOGGING_DESTINATION
from secrets.elastic_secrets import ES_CONNECTION_KEY
from secrets import salesforce_secrets


campuses = CAMPUS_SF_IDS.keys()

BULK_THRESHOLD = 495 # number of objects to upload at a time

SUBJECT_HEADERS = (
    "Diploma",
    "Immunization",
    "Test Scores",
    "IEPs",
    "Letters of Recommendation",
)

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
            # XXX take for granted
            safe_id = row[cn_fields.CONTACT]

            # XXX ...particular
            note_dicts = []
            for field_name, value in row.items():
                for subject in SUBJECT_HEADERS:
                    contact_note_data = {
                        cn_fields.CONTACT: safe_id,
                        cn_fields.DATE_OF_CONTACT: datestring,
                        cn_fields.SUBJECT: subject,
                        cn_fields.COMMENTS: row[subject]
                    }
                    note_dicts.append(contact_note_data)

                if len(note_dicts) >= BULK_THRESHOLD:
                    number_successful = _upload_notes(note_dicts)
                    created_count += number_successful
                    skipped_count += len(note_dicts) - created_count

                    note_dicts = []

            # send any remaining
            if note_dicts:
                #XXX wet
                number_successful = _upload_notes(note_dicts)
                created_count += number_successful
                skipped_count += len(note_dicts) - created_count

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


def _upload_notes(note_dicts):
    """
    Upload the note. Assumes the following minimum kwargs:
    * ...
    ...
    Returns integer number of notes uploaded successfully.
    """
    responses = sf_connection.bulk.Contact_Note__c.insert(note_dicts)
    successful_count = 0
    # I _think_ these come back in order..
    for index, result in enumerate(responses):
        if result['success']:
            logger.info("Uploaded Contact Note {} successfully".format(result['id']))
            successful_count += 1
        else:
            logger.warn("Upload failed: {}. Kwargs: {}".format(
                result['errors'], note_dicts[index]
            ))

    return successful_count


def _string_to_bool(boolstring):
    """Convert string 'True'/'False' to python bool for Salesforce API call."""
    boolstring = boolstring.lower()
    if boolstring == 'true':
        return True
    elif boolstring == 'false':
        return False


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

    log_addr, log_port = SF_LOGGING_DESTINATION
    log_job_name = __file__.split(path.sep)[-1] # name of this file

    if args.sandbox:
        logger = get_logger(
            log_addr, log_port, log_job_name, hostname=SF_LOG_SANDBOX,
        )
        logger.info("Connecting to sandbox Salesforce instance..")
        sf_username = salesforce_secrets.SF_SANDBOX_USERNAME
        sf_token = salesforce_secrets.SF_SANDBOX_TOKEN
    else:
        logger = get_logger(
            log_addr, log_port, log_job_name, hostname=SF_LOG_LIVE,
        )
        logger.info("Connecting to live Salesforce instance..")
        sf_username = salesforce_secrets.SF_LIVE_USERNAME
        sf_token = salesforce_secrets.SF_LIVE_TOKEN

    elastic_connection = es_connections.create_connection(
        hosts=[ES_CONNECTION_KEY], timeout=30
    )

    sf_connection = Salesforce(
        username=sf_username,
        password=salesforce_secrets.SF_PASSWORD,
        security_token=sf_token,
        sandbox=args.sandbox,
    )
    upload_contact_notes(args.infile, campus, source_date_format)

    # ??
    logger.handlers[0].close()
