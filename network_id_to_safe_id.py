"""
network_id_to_safe_id.py

Query Elasticsearch using Network ID and campus to write back out with Safe ID.
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
from simple_salesforce import Salesforce

from full_name_to_sf_ids import _query_for_safe_id # TODO SF libs
from salesforce_fields import contact_note as cn_fields
from salesforce_fields import contact as contact_fields
from salesforce_gen import salesforce_gen
from secrets.elastic_secrets import ES_CONNECTION_KEY
from secrets import salesforce_secrets


NETWORK_ID_HEADER = "Network Student ID" # column header in the csv


def write_safe_ids(csv_filename, campus):
    """
    Query Elasticsearch using Safe ID and campus to write back out with Safe ID.
    """
    network_ids = []

    with open(csv_filename) as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            #if row['HS Class'] < '2011':
            network_ids.append(row[NETWORK_ID_HEADER])

    mid_point = len(network_ids)//2
    network_ids_front = network_ids[:mid_point]
    network_ids_back  = network_ids[mid_point:]

    # build lookup for pre-2011s
    network_to_safe_ids = dict()
    _add_network_and_safe_ids(network_ids_front, network_to_safe_ids)
    _add_network_and_safe_ids(network_ids_back, network_to_safe_ids)

    es_connection = es_connections.create_connection(
        hosts=[ES_CONNECTION_KEY]
    )

    with open(csv_filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        outfile_name = "safe_ids_{}".format(csv_filename)
        with open(outfile_name, 'w') as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                network_id = row[NETWORK_ID_HEADER]
#                if int(row['HS Class']) == 2016:
#                    full_name = row['Full Name']
#                    safe_id = _query_for_safe_id(
#                        full_name, campus, es_connection=es_connection
#                    )
#                else:
                safe_id = network_to_safe_ids[network_id]

                row[cn_fields.CONTACT] = safe_id
                writer.writerow(row)


def _add_network_and_safe_ids(network_ids, lookup):
    """Query SF for Safe IDs that match network_ids, add them the lookup"""

    query = ("SELECT Network_Student_ID__c, Safe_Id__c "
             "FROM Contact "
             "WHERE Network_Student_ID__c in {}")
    query = query.format(tuple(network_ids))

    results_gen = salesforce_gen(sf_connection, query)
    total_results = next(results_gen)
    #assert total_results == len(network_ids)

    for record in results_gen:
        network_id = record[contact_fields.NETWORK_ID]
        safe_id = record[contact_fields.SAFE_ID]
        lookup[network_id] = safe_id

def parse_args():
    """Get input filename and campus (Elasticsearch alias)."""

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
    parser.add_argument(
        '--sandbox',
        action='store_true',
        default=False,
        help="If True, uses the sandbox Salesforce instance. Defaults to False"
    )

    return parser.parse_args()


if __name__=='__main__':
    args = parse_args()
    if args.sandbox:
        sf_username = salesforce_secrets.SF_SANDBOX_USERNAME
        sf_token = salesforce_secrets.SF_SANDBOX_TOKEN
    else:
        sf_username = salesforce_secrets.SF_LIVE_USERNAME
        sf_token = salesforce_secrets.SF_LIVE_TOKEN

        sf_connection = Salesforce(
            username=sf_username,
            password=salesforce_secrets.SF_PASSWORD,
            security_token=sf_token,
            sandbox=args.sandbox,
        )

    write_safe_ids(args.infile, args.campus)
