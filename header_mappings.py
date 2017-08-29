"""
header_mappings.py

A mapping of common colloquial headers from source contact note csv files,
mapped to the related Salesforce field name.
"""

from salesforce_fields import contact_note as cn_fields


HEADER_MAPPINGS = {
    'comments'             : cn_fields.COMMENTS,
    'communication status' : cn_fields.COMMUNICATION_STATUS,
    'successful?'          : cn_fields.COMMUNICATION_STATUS,
    'contact'              : cn_fields.CONTACT,
    'safe_id'              : cn_fields.CONTACT,
    'date'                 : cn_fields.DATE_OF_CONTACT,
    'date of contact'      : cn_fields.DATE_OF_CONTACT,
    'initiated by alum'    : cn_fields.INITIATED_BY_ALUM,
    'method'               : cn_fields.MODE_OF_COMMUNICATION,
    'mode of communication': cn_fields.MODE_OF_COMMUNICATION,
    'subject'              : cn_fields.SUBJECT,
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
