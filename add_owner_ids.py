"""
add_owner_ids.py

Query Elasticsearch using Safe ID and campus to write back out with OwnerId.
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


def write_owner_ids(csv_filename, campus):
    """
    Query Elasticsearch using Safe ID and campus to write back out with OwnerId.
    """

    with open(csv_filename) as csvfile:
        reader = csv.DictReader(csvfile)

        outfile_name = "ownerids_{}".format(csv_filename)
        with open(outfile_name, 'w') as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                if row[cn_fields.CONTACT] != CONTACT_UNKNOWN_STRING:
                    ac_safe_id = get_owner_id(campus, row[cn_fields.CONTACT])
                    row["OwnerId"] = ac_safe_id
                writer.writerow(row)


def get_owner_id(campus, safe_id):
    """
    Query Elasticsearch using alum's Salesforce ID to get the OwnerId.

    Parameters:
    * campus: str campus name (Elastic index to query)
    * safe_id: str alum's Salesforce ID

    Returns the Safe Id for the AC/Owner of the alum if found.
    If no results, returns "Unavailable".
    """

    s = Search().from_dict({
        "query": {
            "match": {
                "safe_id": safe_id,
            }
        }
    })
    s = s.index(campus)

    results = s.execute()
    if len(results) == 0:
        return "Unknown"
    elif len(results) == 1:
        return results[0].ac_safe_id


def parse_args():
    """Get input filename."""

    parser = argparse.ArgumentParser(description="Specifiy input file")
    parser.add_argument(
        'infile',
        help="Input csv file",
    )
    parser.add_argument(
        'campus',
        default=None,
        help="Campus (index) to query against",
    )

    return parser.parse_args()


if __name__=='__main__':
    args = parse_args()
    es_connection = es_connections.create_connection(
        hosts=[ES_CONNECTION_KEY]
    )
    write_owner_ids(args.infile, args.campus)
