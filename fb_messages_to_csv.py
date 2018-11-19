"""
fb_messages_to_csv.py

Process a directory (of directories) of json  files of Facebook messages,
saving them out to a csv to be uploaded by upload_contact_notes.py.
Works for Facebook message dump format as of October 2018.

TODO: tests, ignore blank exchanges, tweak output for compatability with
fb_names_to_sf_ids script, fb_utils repo?, etc.
"""

import argparse
from collections import namedtuple
import csv
from datetime import datetime
import json
import os

from salesforce_utils.constants import SALESFORCE_DATESTRING_FORMAT
from salesforce_fields import contact_note as cn_fields


MESSAGES_FILENAME = "message.json"
SENDER_NAME = "sender_name"
TIMESTAMP_MS = "timestamp_ms"
CONTENT = "content"

# These are just the irrelevant ones to ignore. There are others that we'll
# leave in for now (eg. "You sent a photo", "You created the reminder Meeteup")
# as they may provide context to the conversation
LOWERCASE_FB_META_MESSAGES = (
    "you can now call each other and see information like active status and when you've read messages",
    "say hi to your new facebook friend,", # {first name}
    "say hello to", # {first name}
    "sent you an invite to join messenger",
    "sent an attachment", # anecdotally, seen with the 'invite to join messenger' message
)

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
    """Create a csv of Facebook messages from json files in messages_dir,
    grouped by alum by day.
    """
    with open("fb_messages.csv", "w") as fhand:
        writer = csv.DictWriter(fhand, fieldnames=facebook_note_keys)
        writer.writeheader()

        for conversation_folder in os.listdir(messages_dir):
            msgs_filepath = os.path.abspath(os.path.join(
                messages_dir, conversation_folder, MESSAGES_FILENAME
            ))
            alum_fb_name, messages = parse_messages(msgs_filepath)

            if not messages:
                continue
            for facebook_note in group_messages_into_notes(messages, alum_fb_name):
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
    last_seen_msg_date = messages[0].datetime.date()
    for message in messages:
        if message.datetime.date() == last_seen_msg_date:
            same_day_batch.append(message)
            continue
        contact_notes.append(make_contact_note(same_day_batch, alum_fb_name))
        same_day_batch = [message]
        last_seen_msg_date = message.datetime.date()

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


def parse_messages(message_json_file):
    """Open message_json_file and parse out messages.

    Filters out LOWERCASE_FB_META_MESSAGES that aren't relevant to the
    interaction. Others that could provide context will be kept
    (eg. "You sent a photo", "You created the reminder Meetup", etc.).

    :param message_json_file: str abs path to an json message file
    :return: str (presumed alumni's) name on Facebook, list of Message namedtuples
    :rtype: tuple
    """
    messages = []

    with open(message_json_file, "r") as fhand:
        msgs_dict = json.loads(fhand.read())

    num_participants = len(msgs_dict["participants"])
    if num_participants != 2:
        # haven't seen this yet but it was possible in previous formats...
        print(f"WARNING: More than two participants found in a conversation: {msgs_dict['participants']}")

    # seems reliable that first participant is the 'other'; ie. not the account
    # that generated the messages download. Meaning that the second should
    # always be the AC
    alum_fb_name = msgs_dict["participants"][0]["name"]

    for message in msgs_dict["messages"]:
        message_sender = message[SENDER_NAME]
        msg_datetime = convert_fb_time(message[TIMESTAMP_MS])

        msg_content = message[CONTENT]
        is_meta_message = False
        for message_to_ignore in LOWERCASE_FB_META_MESSAGES:
            if message_to_ignore in msg_content.lower():
                is_meta_message = True

        if not is_meta_message:
            messages.append(Message(
                participant=message_sender, datetime=msg_datetime,
                content=msg_content
            ))

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


def convert_fb_time(ms_timestamp):
    """Converts the millisecond timestamp from facebook to a datetime object
    after converting to POSIX seconds timestamp.
    """
    msg_datetime = datetime.fromtimestamp(ms_timestamp//1000)
    # weak sanity check; this will happen after new years but I'd like to see
    # if/when this occurs
    if msg_datetime.year != datetime.today().year:
        print(f"WARNING: FB message year doesn't match current year: {msg_datetime}")

    return msg_datetime


def parse_args():
    """
    """

    parser = argparse.ArgumentParser(description="Specify messages directory")
    parser.add_argument(
        "messages_dir",
        help="Directory containing messages in .html files"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    process_fb_dump(args.messages_dir)
