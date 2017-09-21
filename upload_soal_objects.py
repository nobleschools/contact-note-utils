"""
upload_soal_objects.py

Upload SoaL data to Salesforce from a csv.
"""

import argparse
import csv

import sys
from os import pardir, path
filepath = path.abspath(__file__)
parent_dir = path.abspath(path.join(filepath, pardir))
package_dir = path.abspath(path.join(parent_dir, pardir))
sys.path.insert(0, package_dir)

from salesforce_fields import account, contact, program
from salesforce_utils.get_connection import get_salesforce_connection
from loggers.papertrail_logger import get_logger, SF_LOG_LIVE, SF_LOG_SANDBOX
from secrets.logging import SF_LOGGING_DESTINATION


PROGRAM_NAME = "Right Angle"

def upload_program_objects(input_filename):
    """
    Upload Program objects to Salesforce.
    """
    logger.info("Starting Program upload..")

    COUNT_PROGRAM_OBJECTS_QUERY = f"SELECT COUNT() FROM {program.API_NAME}"
    pre_uploads_count = \
        sf_connection.query(COUNT_PROGRAM_OBJECTS_QUERY)["totalSize"]

    skipped_count = created_count = 0

    alumni_sf_ids, college_sf_ids = _make_safe_id_lookups(input_filename)

    with open(input_filename, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:

            # Contact__c
            network_id = row["Network_ID"]
            alum_sf_id = alumni_sf_ids.get(network_id, None)
            if alum_sf_id is None:
                logger.warn(
                    f"No alum found with Network ID {network_id}; skipping"
                )
                continue

            # College SF ID
            nces_id = row["NCES_ID"]
            college_sf_id = college_sf_ids.get(nces_id, None)
            if not college_sf_id:
                # some programs are hosted by groups that aren't academic
                # institutions, so no NCES ID. Enter these manually, without
                # a college
                logger.warn(
                    f"No College found for this entry; "
                    f"you should enter it manually! Alum is {alum_sf_id}"
                )
                continue

            # Program Description/Notes
            program_notes = row["Program"]

            possible_dupe = check_for_existing_program(
                program_name=PROGRAM_NAME,
                program_notes=program_notes,
                alum_sf_id=alum_sf_id,
                college_sf_id=college_sf_id,
            )
            if possible_dupe:
                skipped_count += 1
                logger.warn("Found possible duplicate ({}) for {}".format(
                    possible_dupe, row
                ))
                continue

            successful = _upload_program(
                program_name=PROGRAM_NAME,
                program_notes=program_notes,
                alum_sf_id=alum_sf_id,
                college_sf_id=college_sf_id,
            )
            if successful:
                created_count += 1

    logger.info(
        f"{created_count} Program objects uploaded, "
        f"{skipped_count} skipped"
    )

    post_uploads_count = \
        sf_connection.query(COUNT_PROGRAM_OBJECTS_QUERY)["totalSize"]
    # assertion could fail if other objects uploaded by someone else
    assert post_uploads_count == pre_uploads_count + created_count


def _make_safe_id_lookups(input_filename):
    """
    Get Safe IDs for alumni and colleges, return lookups for each

    <network_id>:<alumni_safe_id>
    <college_nces_id>:<college_safe_id>
    """
    alumni_lookup = dict()
    college_lookup = dict()
    network_ids = set()
    nces_ids = set()
    with open(input_filename, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # assumed...
            network_ids.add(row["Network_ID"])
            nces_ids.add(str(row["NCES_ID"]))
        # tuple drops easily in query
        nces_ids = tuple(nces_ids)
        network_ids = tuple(network_ids)

        colleges_query = (
            f"SELECT {account.ID}, {account.NCES_ID} "
            f"FROM {account.API_NAME} "
            f"WHERE {account.NCES_ID} IN {nces_ids} "
        )

        for r in sf_connection.query(colleges_query)["records"]:
            college_lookup[r[account.NCES_ID]] = r[account.ID]

        alumni_query = (
            f"SELECT {contact.ID}, {contact.NETWORK_ID} "
            f"FROM {contact.API_NAME} "
            f"WHERE {contact.NETWORK_ID} IN {network_ids} "
        )

        for r in sf_connection.query(alumni_query)["records"]:
            # some network ids in spreadsheet may not be in salesforce
            network_id = r.get(contact.NETWORK_ID, None)
            if network_id:
                alumni_lookup[network_id] = r[contact.ID]

    return alumni_lookup, college_lookup


def _upload_program(program_name, program_notes, alum_sf_id, college_sf_id):
    """
    Upload the Program.
    Returns success status as a bool.
    """
    kwargs_dict = {
        program.NAME: program_name,
        program.NOTES: program_notes,
        program.STUDENT_SF_ID: alum_sf_id,
        program.COLLEGE_SF_ID: college_sf_id,
    }

    response = sf_connection.Program__c.create(kwargs_dict)
    if response["success"]:
        logger.info("Uploaded Program {} successfully".format(response["id"]))
    else:
        logger.warn("Upload failed: {}. Kwargs: {}".format(
            response["errors"], **kwargs
        ))
    return response["success"]


def check_for_existing_program(program_name, program_notes,
                                 alum_sf_id, college_sf_id):
    """
    Check for existing Program.

    If any exist, return ID for the first found Program, otherwise
    return None.

    Arguments:
    * alum_sf_id: alum's safe id
    * program_name ...
    """

    existing_program_query = (
        f"SELECT {program.ID} "
        f"FROM {program.API_NAME} "
        f"WHERE {program.NAME} = '{program_name}' "
        f"AND {program.STUDENT_SF_ID} = '{alum_sf_id}' "
        f"AND {program.COLLEGE_SF_ID} = '{college_sf_id}' "
    )

    results = sf_connection.query(existing_program_query)
    if results["totalSize"]:
        # may be more, but needs investigation either way
        return results["records"][0][program.ID]
    return None


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


if __name__ == "__main__":
    args = parse_args()

    log_addr, log_port = SF_LOGGING_DESTINATION
    log_job_name = __file__.split(path.sep)[-1] # name of this file

    if args.sandbox:
        logger = get_logger(
            log_addr, log_port, log_job_name, hostname=SF_LOG_SANDBOX,
        )
        logger.info("Connecting to sandbox Salesforce instance..")
    else:
        logger = get_logger(
            log_addr, log_port, log_job_name, hostname=SF_LOG_LIVE,
        )
        logger.info("Connecting to live Salesforce instance..")

    sf_connection = get_salesforce_connection(sandbox=args.sandbox)
    upload_program_objects(args.infile)

    # ??
    logger.handlers[0].close()
