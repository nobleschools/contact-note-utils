"""
add_contact_fields.py

Add Salesforce fields to a csv of alumni using Safe ID column.
Also add # of Alumni Career Office Interaction objects.
"""

import csv

from simple_salesforce import Salesforce

from salesforce_fields import contact as contact_fields
import salesforce_secrets as sf_secrets


CONTACT_UNKNOWN_STRING = "None"

SAFE_ID_HEADER = "Contact__c"

INPUT_FILENAME = "launch_u.csv"

# TODO move to salesforce_fields
CAREER_OFFICE_R = "Career_Office_Interactions__r"

FIELDS_TO_ADD = (
    contact_fields.SIMPLE_ENROLLMENT_STATUS,
    contact_fields.COLLEGE_ATTAINMENT,
    contact_fields.ENROLLED_COLLEGE,
    contact_fields.GRADUATED_FROM,
    contact_fields.COLLEGE_GPA,
    contact_fields.COLLEGE_MAJOR,
    contact_fields.EMAIL,
    contact_fields.CAMPUS_EMAIL,
    contact_fields.MOBILE,
)


def add_alumni_data(sf_con):
    """

    :param sf_con: ``simple_salesforce.Salesforce`` connection
    """

    with open(INPUT_FILENAME) as csvfile:
        reader = csv.DictReader(csvfile)

        outfile_name = "supplemented_{}".format(INPUT_FILENAME)
        with open(outfile_name, "w") as outfile:
            fieldnames = reader.fieldnames
            fieldnames.extend(FIELDS_TO_ADD)
            fieldnames.append(CAREER_OFFICE_R)
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                if row[SAFE_ID_HEADER] != CONTACT_UNKNOWN_STRING:
                    row = add_extra_data(row, sf_con)
                writer.writerow(row)

        print(f"Saved file with added data to {outfile_name}")


def add_extra_data(row, sf_con):
    """Use row[SAFE_ID_HEADER] to query for FIELDS_TO_ADD, adding them back
    to the row. Also add # of Career Office Interactions.
    """
    added_fields = ",".join(FIELDS_TO_ADD)
    query = (
        f"SELECT {added_fields}, "
        f"(SELECT Date__c FROM {CAREER_OFFICE_R}) "
        f"FROM {contact_fields.API_NAME} "
        f"WHERE {contact_fields.SAFE_ID} = '{row[SAFE_ID_HEADER]}'"
    )

    results = sf.query(query)["records"][0]
    for field in FIELDS_TO_ADD:
        row.update({field: results[field]})

    career_office_interactions = 0
    if results[CAREER_OFFICE_R] is not None:
        career_office_interactions = results[CAREER_OFFICE_R]["totalSize"] 
    row.update({CAREER_OFFICE_R: career_office_interactions})

    return row


if __name__ == "__main__":
    sf = Salesforce(
        username=sf_secrets.SF_LIVE_USERNAME,
        password=sf_secrets.SF_LIVE_PASSWORD,
        security_token=sf_secrets.SF_LIVE_TOKEN
    )
    add_alumni_data(sf)
