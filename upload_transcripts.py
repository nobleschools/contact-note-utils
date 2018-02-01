"""
upload_transcripts.py

Upload (transcript) documents to a Contact object in Salesforce.
"""

import argparse
import base64
import csv
import json
from os import path

import requests

from salesforce_fields import account, contact, program
from salesforce_utils.get_connection import get_salesforce_connection
from noble_logging_utils.papertrail_logger import (
    get_logger,
    SF_LOG_LIVE,
    SF_LOG_SANDBOX,
)

SF_ID_HEADER = "Contact__c"
FILE_NO_HEADER = "File number"
TRANSCRIPTS_DIR = path.abspath("NLU Transcripts")


def upload_transcripts(input_filename, sf_connection):
    """Upload NLU transcript attachments to Salesforce Contacts.
    """
    #logger.info("Starting Attachment uploads..")
    print("Starting Attachment uploads..")

    with open(input_filename, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            alum_sf_id = row[SF_ID_HEADER]
            file_number = row[FILE_NO_HEADER]
            attachment_filename = f"Noble 012018[1] {file_number}.pdf"
            filepath = path.join(TRANSCRIPTS_DIR, attachment_filename)

            result = push_attachment(
                sf_connection, alum_sf_id, filepath,
                target_filename="NLU Transcript JAN-2018.pdf"
            )

            print(result, alum_sf_id)


def push_attachment(sf_connection, object_id, filepath, target_filename=None):
    """Pushes an attachment (filename) to the object.

    TODO move to module, test

    :param sf_connection: ``simple_salesforce.Salesforce`` connection object
    :param object_id: str Safe ID of object to push attachment to
    :param filepath: str fully qualified path to the file to upload
    :param target_filename: str desired name of the file in Salesforce. If
        none is given, will get the filename from ``filepath``
    """
    if target_filename is None:
        target_filename = path.split(filepath)[1]

    url = f"{sf_connection.base_url}sobjects/Attachment/"
    session_id = sf_connection.session_id

    with open(filepath, "rb") as fhand:
        body = base64.b64encode(fhand.read()).decode()

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer %s" % session_id,
    }
    data = json.dumps({
        "ParentId": object_id,
        "Name": target_filename,
        "body": body,
    })

    return requests.post(url, headers=headers, data=data)


def parse_args():
    """
    *    infile: input csv file, with Contact SF IDs and file number reference.
    * --sandbox: if present, connects to the sandbox Salesforce instance.
                 Otherwise, connects to live.
    """

    parser = argparse.ArgumentParser(description="Specify input csv file")
    parser.add_argument("infile", help="Input file (in csv format)")
    parser.add_argument(
        "--sandbox",
        action="store_true",
        default=False,
        help="If True, uses the sandbox Salesforce instance. Defaults to False"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    log_job_name = __file__.split(path.sep)[-1] # name of this file

    if args.sandbox:
        logger = get_logger(log_job_name, hostname=SF_LOG_SANDBOX)
        logger.info("Connecting to sandbox Salesforce instance..")
    else:
        logger = get_logger(log_job_name, hostname=SF_LOG_LIVE)
        logger.info("Connecting to live Salesforce instance..")

    sf_connection = get_salesforce_connection(sandbox=args.sandbox)
    upload_transcripts(args.infile, sf_connection)

    # ??
    logger.handlers[0].close()
