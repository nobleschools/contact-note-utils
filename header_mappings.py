"""
header_mappings.py

A mapping of common colloquial headers from source contact note csv files,
mapped to the related Salesforce field name.
"""

from salesforce_fields import contact_note as cn_fields

# input headers will compare header.lower() to the below
HEADER_MAPPINGS = {
    "comments"                 : cn_fields.COMMENTS,
    "communication status"     : cn_fields.COMMUNICATION_STATUS,
    "discussion category"      : cn_fields.DISCUSSION_CATEGORY,
    "successful?"              : cn_fields.COMMUNICATION_STATUS,
    "contact"                  : cn_fields.CONTACT,
    "safe_id"                  : cn_fields.CONTACT,
    "safe id"                  : cn_fields.CONTACT,
    "date"                     : cn_fields.DATE_OF_CONTACT,
    "date of contact"          : cn_fields.DATE_OF_CONTACT,
    "date of communication"    : cn_fields.DATE_OF_CONTACT,
    "initiated by alum"        : cn_fields.INITIATED_BY_ALUM,
    "initiated by alum?"       : cn_fields.INITIATED_BY_ALUM,
    "method"                   : cn_fields.MODE_OF_COMMUNICATION,
    "mode"                     : cn_fields.MODE_OF_COMMUNICATION,
    "mode of communication"    : cn_fields.MODE_OF_COMMUNICATION,
    "mode of contact"          : cn_fields.MODE_OF_COMMUNICATION,
    "subject"                  : cn_fields.SUBJECT,
    # from persistence reports
    "sfid"                     : cn_fields.CONTACT,
    "cn: subject"              : cn_fields.SUBJECT,
    "cn: comments"             : cn_fields.COMMENTS,
    "cn: date of communication": cn_fields.DATE_OF_CONTACT,
    "cn: communication status" : cn_fields.COMMUNICATION_STATUS,
    "cn: mode of communication": cn_fields.MODE_OF_COMMUNICATION,
}

FB_NOTE_HEADERS = (
    "Facebook ID",
    "Facebook Name",
    "Salesforce Name",
    cn_fields.CONTACT,
    "OwnerId", # AC's Safe ID, for when campus has multiple
    "Network_Student_ID__c", # TODO with above, move to a contact_fields consts
    cn_fields.DATE_OF_CONTACT,
    cn_fields.COMMENTS,
    cn_fields.COMMUNICATION_STATUS,
    cn_fields.MODE_OF_COMMUNICATION,
    cn_fields.SUBJECT,
    cn_fields.INITIATED_BY_ALUM,
)
