"""
add_facebook_names.py

Query Facebook API to get names from Facebook ID, write back out to csv.
"""

import argparse
import csv

import sys
from os import pardir, path
filepath = path.abspath(__file__)
parent_dir = path.abspath(path.join(filepath, pardir))
package_dir = path.abspath(path.join(parent_dir, pardir))
sys.path.insert(0, package_dir)

import requests

from secrets.facebook_secrets import FB_API_TOKEN


def get_facebook_names(csv_filename):
    """
    Query Facebook API to get current Facebook name from Facebook ID,
    write back out to csv.
    """

    with open(csv_filename) as csvfile:
        reader = csv.DictReader(csvfile)

        outfile_name = "fbnames_{}".format(csv_filename)
        with open(outfile_name, 'w') as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            with requests.Session() as fb_session:
                params = {'access_token': FB_API_TOKEN}
                fb_session.params.update(params)

                for row in reader:
                    if reader.line_num == 1:
                        continue
                    if row['Facebook ID']:
                        fb_id = row['Facebook ID'].split('@')[0]
                        # TODO cache known ids..
                        row['Facebook Name'] = _get_facebook_name(fb_id, fb_session)
                    writer.writerow(row)


def _match_fb_ids(ids_iterable):
    """
    Build and return a dict of Facebook ID: Facebook Names.
    """
    ids_dict = dict()

    with requests.Session() as fb_session:
        params = {'access_token': FB_API_TOKEN}
        fb_session.params.update(params)

        for fb_id in ids_iterable:
            ids_dict[fb_id] = _get_facebook_name(fb_id, fb_session)

    return ids_dict


def _get_facebook_name(facebook_id, session):
    """
    Query Facebook API to Get the current name from a Facebook ID.

    Parameters:
    * facebook_id: Facebook ID as a string
    * session: `requests.Session` object with FB credentials loaded
    """

    api_url = 'https://graph.facebook.com/v2.9/{}'
    response = session.get(api_url.format(facebook_id))
    #response.raise_for_status() # will raise if resource unavailable

    try:
        matched_name = response.json()['name']
        print("Matched {:>30} to {}".format(facebook_id, matched_name))
        return matched_name
    except KeyError:
        # some are unavailble from account closing, privacy settings?, etc.
        return("Unavailable")


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
    get_facebook_names(args.infile)
