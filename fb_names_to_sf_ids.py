"""
fb_name_to_sf_ids.py

Try to get Salesforce IDs from the Facebook names by querying Elasticsearch
for the (full) Facebook name. Write matches back out to csv.
"""

import argparse
import csv
from collections import namedtuple

from elasticsearch.helpers import scan as es_scan
from elasticsearch_dsl.connections import connections as es_connections

from salesforce_fields import contact_note as cn_fields
from secrets.elastic_secrets import ES_CONNECTION_KEY

ACCEPT_MATCH_SCORE = 7.5 # score at which matches are "blindly" accepted
MIN_SCORE_THRESHOLD = 2 # only return results from ES with this score or higher

FACEBOOK_NAME_HEADER = "alum_fb_name"
SALESFORCE_NAME_HEADER = "Salesforce Name"

AlumContact = namedtuple("AlumContact", ["sf_name", "sf_id"])


def write_salesforce_ids(input_filename, output_filename, campus_index):
    """
    """
    fb_names = _get_fb_names_set(input_filename)
    fb_name_to_alum_contact = _match_sf_ids(fb_names, campus_index)

    with open(input_filename) as infile:
        reader = csv.DictReader(infile)

        with open(output_filename, "w") as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                if row[cn_fields.CONTACT] == "":
                    current_fb_name = row[FACEBOOK_NAME_HEADER]
                    row[SALESFORCE_NAME_HEADER] = fb_name_to_alum_contact[current_fb_name].sf_name
                    row[cn_fields.CONTACT] = fb_name_to_alum_contact[current_fb_name].sf_id
                writer.writerow(row)


def _get_fb_names_set(input_filename):
    """Get the set of Facebook names from the input_filename file.

    Uses FACEBOOK_NAME_HEADER to find Facebook names.
    """
    fb_names = set()

    with open(input_filename) as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            # 'N/A' are known ignores
            if not row[cn_fields.CONTACT]:
                fb_names.add(row[FACEBOOK_NAME_HEADER])

    return fb_names


def _match_sf_ids(fb_names_set, campus):
    """Build a dict of facebook_name: AlumContact
    """
    es_connection = es_connections.create_connection(
        hosts=[ES_CONNECTION_KEY], timeout=20
    )
    fb_name_to_alum_contact = dict() # facebook name: AlumContact

    for fb_name in fb_names_set:
        alum_contact = _match_fb_name_to_sf_id(fb_name, campus, es_connection)
        fb_name_to_alum_contact[fb_name] = alum_contact

    return fb_name_to_alum_contact


def _match_fb_name_to_sf_id(fb_name, campus, es_connection):
    """Search for salesforce id, salesforce name in Elastic by fb_name.

    :param fb_name: str name from facebook notes dump
    :param campus: str campus name ES index to search
    :param es_connection: elasticsearch_dsl.Connection
    :return: AlumContact namedtuple
    """
    full_name_search = {
        "min_score": MIN_SCORE_THRESHOLD,
        "query": {
            "match": {
                "full_name": {
                    "query": fb_name,
                    "fuzziness": 2,
                    #"prefix_length": 2,
                }
            }
        }
    }
    results = es_scan(es_connection, query=full_name_search,
        scroll="1m", index=campus, preserve_order=True
    )

    strong_matches = [] # most likely matches, occasionally multiple
    weak_matches = [] # < ACCEPT_MATCH_SCORE

    # split results into strong matches and weak matches
    for hit in results:
        if hit["_score"] >= ACCEPT_MATCH_SCORE:
            strong_matches.append(hit)
        else:
            weak_matches.append(hit)

    if len(strong_matches) > 1: # choose correct match
        print("Multiple strong matches found for {}.".format(fb_name))
        match_index = _elicit_match(strong_matches)
        if match_index is not None: # could be 0th item
            match = strong_matches[match_index]
            matched_sf_id = match["_source"]["safe_id"]
            matched_sf_name = match["_source"]["full_name"]
            print("Matched FB '{}' with {}, {} ({}, Safe ID: {})".format(
                fb_name, match["_source"]["last_name"],
                match["_source"]["first_name"], match["_index"], match["_id"]
            ))
            return AlumContact(sf_name=matched_sf_name,sf_id=matched_sf_id)
        else:
            print("No strong matches match? Skipping {}".format(fb_name))
            return AlumContact(sf_name="???", sf_id="StillNotFound")

    elif len(strong_matches) == 1: # one strong match found; accept
        match = strong_matches[0]
        matched_sf_id = match["_source"]["safe_id"]
        matched_sf_name = match["_source"]["full_name"]
        print("Matched FB '{}' with {}, {} ({}, Safe ID: {})".format(
            fb_name, match["_source"]["last_name"],
            match["_source"]["first_name"], match["_index"], match["_id"]
        ))
        return AlumContact(sf_name=matched_sf_name, sf_id=matched_sf_id)

    elif weak_matches: # check weak matches
        print("Multiple weak matches found for {}.".format(fb_name))
        match_index = _elicit_match(weak_matches)
        if match_index is not None: # could be 0th item
            match = weak_matches[match_index]
            matched_sf_id = match["_source"]["safe_id"]
            matched_sf_name = match["_source"]["full_name"]
            print("Matched FB '{}' with {}, {} ({}, Safe ID: {})".format(
                fb_name, match["_source"]["last_name"],
                match["_source"]["first_name"], match["_index"], match["_id"]
            ))
            return AlumContact(sf_name=matched_sf_name, sf_id=matched_sf_id)
        else:
            print("No matches found for {}".format(fb_name))
            return AlumContact(sf_name="???", sf_id="StillNotFound")

    else: # no weak or strong matches found
        print("No matches found for {}".format(fb_name))
        return AlumContact(sf_name="???", sf_id="StillNotFound")


def _elicit_match(candidates):
    """
    Iterate through `candidates` and return the index in the iterable indicated
    by the user, or an empty string if none indicated.

    Each item in `candidates` should be a dictionary of search results, as
    returned by elasticsearch query.
    """
    for i, candidate in enumerate(candidates):
        print("{:>2}) {:<9} {:>15}, {:<15} ({}) {} ({})".format(
            i, candidate["_score"], candidate["_source"]["last_name"],
            candidate["_source"]["first_name"],
            candidate["_source"]["class_year"], candidate["_id"],
            candidate["_index"]
        ))

    match_index = input("Enter number of matched identity, or press Enter to pass on these options: ")
    if match_index:
        return int(match_index)


def parse_args():
    """
    Get input and output file names, campus index to use for search.
    """

    parser = argparse.ArgumentParser(description=\
        "Specify input filename, output filename, and campus index to use"
    )
    parser.add_argument(
        "infile",
        help="Input file (in csv format)"
    )
    parser.add_argument(
        "outfile",
        help="Name of csv file to output"
    )
    parser.add_argument(
        "index",
        help="Name of campus index to use"
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    write_salesforce_ids(args.infile, args.outfile, args.index)
