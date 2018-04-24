"""
"""

import argparse
import csv

import sys
from os import pardir, path
filepath = path.abspath(__file__)
parent_dir = path.abspath(path.join(filepath, pardir))
package_dir = path.abspath(path.join(parent_dir, pardir))
sys.path.insert(0, package_dir)

from elasticsearch_dsl import Search
from elasticsearch_dsl.connections import connections as es_connections
import requests

from salesforce_fields import contact_note as cn_fields
from secrets.elastic_secrets import ES_CONNECTION_KEY

CONTACT_UNKNOWN_STRING = "StillNotFound"
FB_IGNORES_INDEX = "fb-ignore"


def write_fb_ignores(csv_filename):
    """
    """
    with open(csv_filename) as csvfile:
        reader = csv.DictReader(csvfile)

        outfile_name = "fbignores_{}".format(csv_filename)
        with open(outfile_name, 'w') as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                if row[cn_fields.CONTACT] == CONTACT_UNKNOWN_STRING:
                    ignore_found = find_known_ignore(row["Facebook ID"])
                    if ignore_found:
                        row["Salesforce Name"] = row[cn_fields.CONTACT] = "N/A"
                writer.writerow(row)


def find_known_ignore(facebook_id):
    """
    Query Elasticsearch using Facebook ID to check if contact should be
    ignored for contact upload purposes (ie. if contact is not an alum).

    Parameters:
    * facebook_id: str facebook id

    Returns True if a known non-alum-contact is found, else False.
    """
    s = Search().from_dict({
        "query": {
            "match": {
                "facebook_id": facebook_id,
            }
        }
    })
    s = s.index(FB_IGNORES_INDEX)

    results = s.execute()
    if len(results) == 0:
        return False
    elif len(results) == 1:
        return True
    # >1 .. ?


def parse_args():
    """Get input filename."""

    parser = argparse.ArgumentParser(description="Specifiy input file")
    parser.add_argument(
        'infile',
        help="Input csv file",
    )

    return parser.parse_args()


if __name__=='__main__':
    args = parse_args()
    es_connection = es_connections.create_connection(
        hosts=[ES_CONNECTION_KEY]
    )
    write_fb_ignores(args.infile)
