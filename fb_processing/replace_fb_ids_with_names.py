"""
replace_fb_ids_with_names.py

Replace Facebook IDs in the 'Comments' column with either 'Salesforce Name' or
'Facebook Name', whichever is first available in that order.
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

from salesforce_fields import contact_note as cn_fields
from secrets.elastic_secrets import ES_CONNECTION_KEY

es_connections.create_connection(hosts=[ES_CONNECTION_KEY], timeout=120)


def replace_ids(input_filename, output_filename, campus_index):

    with open(input_filename) as infile:
        reader = csv.DictReader(infile)

        with open(output_filename, 'w') as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                # skip header
                if reader.line_num == 1:
                    continue

                fb_id = row['Facebook ID']

                if row[cn_fields.CONTACT] == 'StillNotFound':
                    # Salesforce name unknown; use Facebook name
                    fb_name = row['Facebook Name']
                    row[cn_fields.COMMENTS] = row[cn_fields.COMMENTS].replace(fb_id, fb_name)
                elif row[cn_fields.CONTACT] != 'N/A':
                    if row['Salesforce Name']:
                        sf_name = row['Salesforce Name']
                    else:
                        sf_name = _get_sf_name(row[cn_fields.CONTACT], campus_index)
                        row['Salesforce Name'] = sf_name
                    row[cn_fields.COMMENTS] = row[cn_fields.COMMENTS].replace(fb_id, sf_name)
                writer.writerow(row)


def _get_sf_name(safe_id, campus_index):
    """Try to get alumni name from elastic, otherwise return safe_id"""
    es_search = Search()
    es_search = es_search.from_dict({
        "query": {
            "match": {
                "safe_id": safe_id,
            },
        },
    })
    es_search = es_search.index(campus_index)
    es_search.execute()

    # assert es_search.count() == 1 # TODO sanity check
    display_name = safe_id
    if es_search.count() == 1:
        for hit in es_search:
            display_name = hit.full_name
    else:
        print("Multiple or no matches found for {}".format(safe_id))
    return display_name


def parse_args():
    """Get input and output file names"""

    parser = argparse.ArgumentParser(description=\
        "Specify input filename and output filename"
    )
    parser.add_argument(
        "infile",
        #type=argparse.FileType('r'),
        help="Input file (in csv format)"
    )
    parser.add_argument(
        "outfile",
        #type=argparse.FileType('w'),
        help="Name of csv file to output"
    )
    parser.add_argument(
        "campus",
        help="Name of campus index to use"
    )
    return parser.parse_args()

if __name__=='__main__':
    args = parse_args()
    replace_ids(args.infile, args.outfile, args.campus)
