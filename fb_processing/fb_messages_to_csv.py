"""
fb_messages_to_csv.py

Process a directory of html files of Facebook messages, saving them out to
a csv to be uploaded by upload_contact_notes.py.
"""

import argparse
from collections import namedtuple
import csv
from datetime import datetime
import os
import sys

from bs4 import BeautifulSoup as bs

from header_mappings import FB_NOTE_HEADERS
from salesforce_utils.constants import SALESFORCE_DATESTRING_FORMAT
from salesforce_fields import contact_note as cn_fields

# eg. Tuesday, November 22, 2016 at 10:36am CST
SOURCE_DATETIME_FMT = "%A, %B %d, %Y at %I:%M%p %Z" # from FB dump
CONVERSATION_TIME_FMT = "%I:%M%p"                   # for chat log

# Mode of Communication for all Facebook exchanges
SOCIAL_NETWORKING_MOC = "Social Networking"

facebook_note_keys = [
    "alum_fb_name",
    "contact",
    "comments",
    "date_of_contact",
    "mode_of_communication",
    "communication_status",
    "initiated_by_alum",
    "subject",
]
Message = namedtuple("Message", ["participant", "datetime", "content"])
FacebookNote = namedtuple("FacebookNote", facebook_note_keys)

def process_fb_dump(messages_dir):
    """
    Create a csv of Facebook messages from html files in messages_dir,
    grouped by alum by day.
    """
    with open("fb_messages.csv", "w") as fhand:
        writer = csv.DictWriter(fhand, fieldnames=facebook_note_keys)
        writer.writeheader()
        # writer.writerow(facebook_note_keys)

        for messages_file in os.listdir(messages_dir):
            filepath = os.path.abspath(os.path.join(messages_dir, messages_file))
            # print(filepath)
            alum_fb_name, messages = parse_messages(filepath)
            if not messages:
                continue
            for facebook_note in group_messages_into_notes(messages, alum_fb_name):
                # print(contact_note)
                writer.writerow(facebook_note._asdict())


def group_messages_into_notes(messages, alum_fb_name):
    """Group messages into facebook contact notes by date of message.

    :param messages: list of Message namedtuples
    :param alum_fb_name: 
    :return: list of Contact Note namedtuples, to be written to output file
    :rtype: list
    """
    messages = sorted(messages, key=lambda x: x.datetime)
    contact_notes = []
    same_day_batch = []
    last_msg_date = messages[0].datetime.date()
    for message in messages:
        if message.datetime.date() == last_msg_date:
            same_day_batch.append(message)
            continue
        contact_notes.append(make_contact_note(same_day_batch, alum_fb_name))
        same_day_batch = [message]
        last_msg_date = message.datetime.date()

    # flush remaining
    contact_notes.append(make_contact_note(same_day_batch, alum_fb_name))

    return contact_notes


def make_contact_note(messages, alum_fb_name):
    """Make a single FacebookNote namedtuple from messages.

    :param messages: list of Message namedtuples (all from the same day)
    :param alum_fb_name: 
    :return: FacebookNote namedtuple
    :rtype: FacebookNote
    """
    messages = sorted(messages, key=lambda x: x.datetime)
    message_lines = []
    for message in messages:
        message_lines.append(
            f"{message.participant}@{message.datetime}: {message.content}"
        )
    initiated_by_alum, comm_status, subject = \
        get_nature_of_exchange(messages, alum_fb_name)
    note = FacebookNote(
        alum_fb_name=alum_fb_name,
        contact="", # to be queried for later, from alum name
        comments="\n".join(message_lines),
        date_of_contact=messages[0].datetime.strftime(SALESFORCE_DATESTRING_FORMAT),
        mode_of_communication=SOCIAL_NETWORKING_MOC,
        communication_status=comm_status,
        initiated_by_alum=initiated_by_alum,
        subject=subject,
    )

    return note


def parse_messages(message_html_file):
    """Open message_html_file and parse out messages.

    Returns None if a single, valid conversation participant is not found.

    :param message_html_file: str abs path to an html message file
    :return: str (presumed alumni's) name on Facebook, list of Message namedtuples
    :rtype: tuple
    """
    messages = []

    with open(message_html_file, "r") as fhand:
        soup = bs(fhand.read(), "html.parser")

    # participants string is between elements in the .thread div
    thread_elements = soup.select(".thread")[0].children
    participants = list(thread_elements)[1]
    # ignore where >1 participant (group chat) or participant data not present
    try:
        if len(participants.split(",")) != 1:
            return None, messages
    except TypeError:
        # rarely, files will be missing a participant altogether
        return None, messages

    alum_fb_name = participants.split(":")[1].strip()
    # print(alum)

    message_divs = soup.select(".message")
    for m_div in message_divs:
        user = m_div.select(".user")[0].text.strip()
        msg_datetime = m_div.select(".meta")[0].text.strip()
        msg_datetime = convert_fb_time(msg_datetime)

        messages.append(Message(
            participant=user, datetime=msg_datetime,
            content=m_div.nextSibling.text
        ))
        print(user, " @ ", msg_datetime,  " : ", m_div.nextSibling.text, "\n")

    return alum_fb_name, messages


def get_nature_of_exchange(messages, alum_fb_name):
    """
    :param messages: list of Message namedtuples
    :param alum_fb_name: str name of alum on Facebook

    Depending on the order and source of messages in a conversation, determine
    and return a tuple of the following:
        - initiated_by_alum (bool as a str): is first message from the alum
        - communication_status ('Successful Communication' when there is at
            least one message from the alum, 'Outreach Only' when AC sends
            message(s) that doesn't receive a response)
        - subject ('Facebook outreach transcript' when communication_status is
            'Outreach only', 'Facebook transcript' otherwise)
    """
    # assume Outreach Only
    found_alum_message = False
    initiated_by_alum = False

    #print("messages passed to get_nature.. : {}".format(messages_list))
    if messages[0].participant.startswith(alum_fb_name):
        found_alum_message = True
        initiated_by_alum = True

    if not found_alum_message:
        # check the rest for message by alum
        for message in messages[1:]:
            if message.participant.startswith(alum_fb_name):
                found_alum_message = True
                break

    if found_alum_message:
        # TODO consts
        communication_status = "Successful communication"
        subject = "Facebook transcript"
    else:
        communication_status = "Outreach only"
        subject = "Facebook outreach transcript"

    return (str(initiated_by_alum), communication_status, subject)


def convert_fb_time(datetime_str):
    """
    Converts the datetime string from facebook to a python datetime object.
    """
    return datetime.strptime(datetime_str, SOURCE_DATETIME_FMT)


def parse_args():
    """
    """

    parser = argparse.ArgumentParser(description="Specify messages directory")
    parser.add_argument(
        "messages_dir",
        help="Directory containing messages in .html files"
    )
    return parser.parse_args()


if __name__=="__main__":
    args = parse_args()
    process_fb_dump(args.messages_dir)
