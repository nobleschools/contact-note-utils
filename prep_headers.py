"""
prep_headers.py

Changes the headers of a contact note input csv to Salesforce-ready
field names. Saves the changes back out as 'prepped_<input filename>' in the
working directory.

Converts the relevant headers (see salesforce_fields.contact_note).
"""

import argparse
import csv

import sys
from os import pardir, path
filepath = path.abspath(__file__)
parent_dir = path.abspath(path.join(filepath, pardir))
package_dir = path.abspath(path.join(parent_dir, pardir))
sys.path.insert(0, package_dir)

from header_mappings import HEADER_MAPPINGS


def clean_headers(source_file):
    """
    Convert headers to Salesforce-ready field names.
    """
    source_filename = path.split(source_file)[1]
    output_filename = 'prepped_' + source_filename

    with open(source_file, 'r') as source_csv:
        reader = csv.DictReader(source_csv)
        source_headers = [f.strip() for f in reader.fieldnames]
        target_headers = list(map(
            lambda x: HEADER_MAPPINGS.get(x.lower(), x), source_headers
        ))
        reader.fieldnames = target_headers # else it can't map back out

        with open(output_filename, 'w') as output_csv:
            writer = csv.DictWriter(output_csv, target_headers)
            writer.writeheader()

            for row in reader:
                writer.writerow(row)

    print("Created {}".format(output_filename))
    return output_filename


def parse_args():
    """
    infile: name of input csv file. Assumes the following headers in input:
        * ALL OF THE (APPLICABLE) HEADERS
    """

    parser = argparse.ArgumentParser(description="Specify input csv file")
    parser.add_argument(
        "infile",
        help="Input file (in csv format)"
    )
    return parser.parse_args()


if __name__=="__main__":
    args = parse_args()
    clean_headers(args.infile)
