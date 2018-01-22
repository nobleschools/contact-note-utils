"""
full_name_to_sf_ids.py

Try to get Salesforce IDs from Elastic by querying for the alum full names.
Write matches back out to csv.
"""

import argparse
import csv

import sys
from os import pardir, path
filepath = path.abspath(__file__)
parent_dir = path.abspath(path.join(filepath, pardir))
package_dir = path.abspath(path.join(parent_dir, pardir))
sys.path.insert(0, package_dir)

from elasticsearch.helpers import scan as es_scan
from elasticsearch_dsl.connections import connections as es_connections

from salesforce_fields import contact_note as cn_fields
from secrets.elastic_secrets import ES_CONNECTION_KEY

ACCEPT_MATCH_SCORE = 7.5 # score at which matches are "blindly" accepted
MIN_SCORE_THRESHOLD = 2 # only return results from ES with this score or higher


def write_salesforce_ids(input_filename, campus, name_headers):

    output_filename = "with_ids_{}".format(input_filename)

    with open(input_filename) as infile:
        reader = csv.DictReader(infile)

        with open(output_filename, 'w') as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                if not row[cn_fields.CONTACT]:
                    if len(name_headers) > 1:
                        full_name = _make_full_name(row, name_headers)
                    else:
                        full_name = row[name_headers[0]]
                    row[cn_fields.CONTACT] = _query_for_safe_id(full_name, campus)

                writer.writerow(row)


def _make_full_name(row_dict, headers_list):
    """
    Helper function to put together a full name to use for Elastic query.
    """
    name_bits = []
    for name_header in headers_list:
        name_bits.append(row_dict[name_header])

    return " ".join(name_bits)


# TODO SF/Elastic libs
def _query_for_safe_id(full_name, campus, es_connection=None):
    """
    Query Elastic for alum's Safe ID, using full_name and campus (index).
    """
    es_connection = es_connections.create_connection(hosts=[ES_CONNECTION_KEY])

    full_name_search = {
        "min_score": MIN_SCORE_THRESHOLD,
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "full_name": {
                            "query": full_name,
                            "fuzziness": 2,
                        },
                    },
                },
                # can comment out this filter clause to search across all
                # campuses in a pinch
                "filter": { # es_scan doesn't respect aliases as faux indices
                    "term": {
                        "campus": campus,
                    },
                },
            },
        },
    }

    results = es_scan(es_connection, query=full_name_search,
        scroll='1m', preserve_order=True
    )

    strong_matches = [] # most likely matches, occasionally multiple
    weak_matches = [] # < ACCEPT_MATCH_SCORE

    # split results into strong matches and weak matches
    for hit in results:
        if hit['_score'] >= ACCEPT_MATCH_SCORE:
            strong_matches.append(hit)
        else:
            weak_matches.append(hit)
    if len(strong_matches) > 1: # choose correct match
        print("Multiple strong matches found for {}.".format(full_name))
        match_index = _elicit_match(strong_matches)
        if match_index is not None: # could be 0th item
            match = strong_matches[match_index]
            matched_sf_id = match['_source']['safe_id']
            print("Matched name '{}' with {}, {} ({}, Network ID: {})".format(
                full_name, match['_source']['last_name'],
                match['_source']['first_name'], match['_index'], match['_id']
            ))
            return matched_sf_id
        else:
            print("No strong matches match? Skipping {}".format(full_name))
            return "Multiple"
    elif len(strong_matches) == 1: # one strong match found; accept
        match = strong_matches[0]
        matched_sf_id = match['_source']['safe_id']
        print("Matched name '{}' with {}, {} ({}, Network ID: {})".format(
            full_name, match['_source']['last_name'],
            match['_source']['first_name'], match['_index'], match['_id']
        ))
        return matched_sf_id
    elif weak_matches: # check weak matches
        print("Multiple weak matches found for {}.".format(full_name))
        match_index = _elicit_match(weak_matches)
        if match_index is not None: # could be 0th item
            match = weak_matches[match_index]
            matched_sf_id = match['_source']['safe_id']
            print("Matched name '{}' with {}, {} ({}, Network ID: {})".format(
                full_name, match['_source']['last_name'],
                match['_source']['first_name'], match['_index'], match['_id']
            ))
            return matched_sf_id
        else:
            print("No matches found for {}".format(full_name))
            return "None"
    else: # no weak or strong matches found
        print("No matches found for {}".format(full_name))
        return "None"


def _elicit_match(candidates):
    """
    Iterate through `candidates` and return the index in the iterable indicated
    by the user, or an empty string if none indicated.

    Each item in `candidates` should be a dictionary of search results, as
    returned by elasticsearch query.
    """
    for i, candidate in enumerate(candidates):
        print("{:>2}) {:<9} {:>15}, {:<15} ({}) {} ({})".format(
            i, candidate['_score'], candidate['_source']['last_name'],
            candidate['_source']['first_name'],
            candidate['_source']['class_year'], candidate['_id'],
            candidate['_index']
        ))

    match_index = input("Enter number of matched identity, or press Enter to pass on these options: ")
    if match_index:
        return int(match_index)


def parse_args():
    """
    Get input file name, campus index to use for search.

    Optionally can provide --nameheaders string.
    """

    parser = argparse.ArgumentParser(description=\
        "Specify input filename, output filename, and campus index to use"
    )
    parser.add_argument(
        'infile',
        help="Input file (in csv format)"
    )
    parser.add_argument(
        'index',
        help="Name of campus index to use"
    )
    parser.add_argument(
        '--nameheaders',
        default="Name",
        nargs="+",
        help=("Name of header(s) to use for full name lookup. If "
              "multiple - eg. 'First Name' and 'Last Name' columns - "
              "arguments are expected to in first-to-last order "
              "(though the search can generally match well in any order). "
              "Defaults to a single header 'Name'."
        ),
    )
    return parser.parse_args()

if __name__=='__main__':
    args = parse_args()
    write_salesforce_ids(args.infile, args.index, args.nameheaders)
