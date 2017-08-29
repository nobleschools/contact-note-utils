"""
common_date_formats.py

A tuple of common date formats, as seen in input csv files. These datestring
formats need to be converted to the Salesforce API-ready "%Y-%m-%d".
"""

COMMON_DATE_FORMATS = (
    "%m/%d",    # from persistence reports sheet
    "%m/%d/%y", # from Facebook notes
    "%m/%d/%Y",
    "%Y-%m-%d", # Salesforce API-ready
)
