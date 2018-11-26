"""
contactnotes_reports_mduran.py

A custom report of Contact Notes for M Duran. cf. HelpDesk ticket 6135605
"""

from collections import OrderedDict
import csv
from datetime import datetime, timedelta

import pytz

from salesforce_fields import contact_note as cn_fields
from salesforce_utils import (
    get_salesforce_connection,
    salesforce_gen,
)
from salesforce_utils.constants import SALESFORCE_DATESTRING_FORMAT

MDURAN_SFID = "005E0000001e8qLIAQ"
SEMESTER_START_DATE = "2018-08-26" # for counting current semester contact notes

CONTACT_MODES = [
    "Call",
    "Email",
    "IM",
    "In Person",
    "Mail (letter/postcard)",
    "School Visit",
    "Social Networking",
    "Text",
    "Parent contact (any medium)",
    "College staff contact",
    "None",
]

REPORT_HEADERS = [
    "HS Class",
    "Last",
    "First",
    "SFID",
    "College",
    "Contact Notes, 0-2 Weeks",
    "Contact Notes, 2-4 Weeks",
    "Contact Notes, Semester",
    "Contact Notes, All Time",
]
REPORT_HEADERS.extend(CONTACT_MODES)


class ReportRow:

    def __init__(self, safe_id=None, last_name=None, first_name=None,
                 college=None, hs_class=None):
        self.safe_id = safe_id
        self.last_name = last_name
        self.first_name = first_name
        self.college = college
        self.hs_class = hs_class

        self.contacts_0_2_weeks = 0
        self.contacts_2_4_weeks = 0
        self.contacts_semester  = 0
        self.contacts_total     = 0

        self.contact_modes = dict.fromkeys(CONTACT_MODES, 0)


    def as_dict(self):
        row_dict = {
            "HS Class": self.hs_class,
            "Last": self.last_name,
            "First": self.first_name,
            "SFID": self.safe_id,
            "College": self.college,
            "Contact Notes, 0-2 Weeks": self.contacts_0_2_weeks,
            "Contact Notes, 2-4 Weeks": self.contacts_2_4_weeks,
            "Contact Notes, Semester": self.contacts_semester,
            "Contact Notes, All Time": self.contacts_total,
        }
        row_dict.update(self.contact_modes)
        return row_dict


def generate_report():
    sf_conn = get_salesforce_connection(sandbox=False)

    two_weeks_datestr, four_weeks_datestr = _two_four_week_datestrings()

    # <safe id>: <ReportRow obj, one per alum>
    report_rows_lookup = OrderedDict()

    notes_query = (
        f"SELECT {cn_fields.CONTACT}, "
        "Contact__r.LastName, "
        "Contact__r.FirstName, "
        "Contact__r.HS_Class__c, "
        "Contact__r.Currently_Enrolled_At__c, "
        # f"{cn_fields.SUBJECT}, "
        # f"{cn_fields.COMMENTS}, "
        f"{cn_fields.DATE_OF_CONTACT}, "
        # f"{cn_fields.COMMUNICATION_STATUS}, "
        f"{cn_fields.MODE_OF_COMMUNICATION} "
        f"FROM {cn_fields.API_NAME} "
        f"WHERE Contact__r.OwnerId = '{MDURAN_SFID}' "
        # f"WHERE {cn_fields.DATE_OF_CONTACT} > {self.SEMESTER_START_DATE} "
        # f"AND Contact__r.AccountId IN ({campus_ids_str}) "
        "ORDER BY Contact__r.HS_Class__c,Contact__r.LastName,Contact__r.FirstName ASC "
    )
    records_gen = salesforce_gen(sf_conn, notes_query)

    for record in records_gen:
        alum_safe_id = record[cn_fields.CONTACT]
        try:
            report_row = report_rows_lookup[alum_safe_id]
        except KeyError:
            contact_record = record["Contact__r"]
            report_row = ReportRow(
                safe_id=alum_safe_id,
                last_name=contact_record["LastName"],
                first_name=contact_record["FirstName"],
                hs_class=contact_record["HS_Class__c"],
                college=contact_record["Currently_Enrolled_At__c"]
            )
            report_rows_lookup[alum_safe_id] = report_row

        contact_mode = record[cn_fields.MODE_OF_COMMUNICATION]
        if contact_mode is None:
            contact_mode = "None"
        report_row.contact_modes[contact_mode] += 1

        # update contact note time period counts
        contact_date_str = record[cn_fields.DATE_OF_CONTACT]

        if contact_date_str > two_weeks_datestr:
            report_row.contacts_0_2_weeks += 1
        elif contact_date_str > four_weeks_datestr:
            report_row.contacts_2_4_weeks += 1

        if contact_date_str > SEMESTER_START_DATE:
            report_row.contacts_semester += 1

        report_row.contacts_total += 1

    with open("mduran_contactnotes_report.csv", "w") as fhand:
        writer = csv.DictWriter(fhand, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        for report_row in report_rows_lookup.values():
            writer.writerow(report_row.as_dict())


def _two_four_week_datestrings():
    """
    Return tuple of of datestrings for the dates falling two and four
    weeks prior to today.

    Formatted as Salesforce-ready datestrings; eg. '2017-07-16'.
    """
    chicago_tz = pytz.timezone("US/Central")
    today = datetime.now()
    today_localized = chicago_tz.localize(today).date()

    two_weeks_date = today - timedelta(weeks=2)
    two_weeks_datestr = \
        two_weeks_date.strftime(SALESFORCE_DATESTRING_FORMAT)
    four_weeks_date = today - timedelta(weeks=4)
    four_weeks_datestr = \
        four_weeks_date.strftime(SALESFORCE_DATESTRING_FORMAT)

    return (two_weeks_datestr, four_weeks_datestr)


if __name__ == "__main__":
    generate_report()
