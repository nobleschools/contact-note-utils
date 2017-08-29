"""
filter_owner_id.py

Filters rows so that only those under a particular AC's caseload remain.
"""

import csv
import time

import requests

from secrets.facebook import FB_API_TOKEN


def pull_facebook_names(csv_filename):
    """
    Query Facebook API to get current Facebook name from Facebook ID,
    write back out to csv.
    """

    with open(csv_filename) as csvfile:
        reader = csv.DictReader(csvfile)

        with open('matched_ignores.csv', 'w') as outfile:
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
                        row['Facebook Name'] = _get_facebook_name(fb_id, fb_session)
                        time.sleep(0.1)
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
            time.sleep(0.2)

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
        return response.json()['name']
    except KeyError:
        # some are unavailble from account closing, privacy settings?, etc.
        return("Unavailable")

if __name__=='__main__':
    pull_facebook_names('known_ignores.csv')
