"""
process_fb_dump.py

Process the html dump of Facebook messages, saving them out to a csv.
"""

import argparse, csv, os, sys
from datetime import datetime
filepath = os.path.abspath(__file__)
parent_dir = os.path.abspath(os.path.join(filepath, os.pardir))
package_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))
sys.path.insert(0, package_dir)

from bs4 import BeautifulSoup as bs

from header_mappings import FB_NOTE_HEADERS
from constants import SALESFORCE_DATESTRING_FORMAT
from salesforce_fields import contact_note as cn_fields

# eg. Tuesday, November 22, 2016 at 10:36am CST
SOURCE_DATETIME_FMT = "%A, %B %d, %Y at %I:%M%p %Z" # from FB dump
CONVERSATION_TIME_FMT = "%I:%M%p"                   # for chat log

# selector for getting the AC's Facebook account name
AC_ACCOUNT_SELECTOR = '.contents > h1'
BULLS_FB_ID = '100005891346250@facebook.com'

# Mode of Communication for all Facebook exchanges
SOCIAL_NETWORKING_MOC = "Social Networking"


def process_fb_dump(source_file):
    """
    Create a csv of Facebook messages from an html dump of the latter,
    grouped by alum by day.
    """
    source_filename = os.path.split(source_file)[1]
    output_filename = 'prepped_' + source_filename

    with open(source_file, 'r') as infile:
        soup = bs(infile.read(), 'html.parser')

    #ac_account_name = soup.select(AC_ACCOUNT_SELECTOR)[0].text

    # only record conversations with two participants (AC and alum)
    threads_iter = filter(
        lambda x: len(x.contents[0].split(',')) == 2, soup.select('.thread')
    )

    with open('fb_notes.csv', 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=FB_NOTE_HEADERS)
        writer.writeheader()
        for thread in threads_iter:
            # each thread contains all messages between a unique group of people
            print('-'*40)
            children = list(thread.children)
            people = children.pop(0)
            person1_id, person2_id = people.split(',')
            person2_id.strip()
            # XXX cheating
            alum_fb_id = person1_id if person1_id != BULLS_FB_ID else person2_id

            #print("Convo between {} and {}".format(person1_id, person2_id))

            # message and metadata come from FB ordered latest to earliest,
            # but meta data always comes first; eg.
            # [meta-A, msg-A, (earlier) meta-B, (earlier) msg-B, ...]
            messages = children[::-2]  # now in time ascending order
            messages = [m.text for m in messages]
            #print("Messages length in thread: {}".format(len(messages)))
            #print("Messages: {}".format(messages))
            meta_data = children[-2::-2]

            # group messages from same date into one thread
            date_last_message = None
            days_messages = []
            for entry in zip(meta_data, messages):
                # check each message exchanged between the two people
                #print("Meta: {}\nMessage: {}\n".format(entry[0].text, entry[1]))

                datetime_string = entry[0].select('.meta')[0].text
                datetime_obj = convert_fb_time(datetime_string)
                message_date = datetime_obj.date()
                # XXX cheating
                if message_date.year < 2017:
                    continue

                message_time = datetime_obj.time()
                user = entry[0].select('.user')[0].text
                message = entry[1]
                print("{}@{}: {}".format(user, datetime_obj, message))

                if date_last_message == None:
                    # first message seen for that day
                    date_last_message = message_date
                    days_messages.append("{}@{}) {}".format(
                        user, message_time.strftime(CONVERSATION_TIME_FMT), message
                    ))
                    continue
                elif date_last_message == message_date:
                    days_messages.append("{}@{}) {}".format(
                        user, message_time.strftime(CONVERSATION_TIME_FMT), message
                    ))
                    continue

                # new day; write row
                if days_messages:
                    msg_row_dict = row_dict(alum_fb_id, days_messages, date_last_message)
                    #print(msg_row_dict)
                    writer.writerow(msg_row_dict)

                    date_last_message = message_date
                    days_messages = []
                    days_messages.append("{}@{}) {}".format(
                        user, message_time.strftime(CONVERSATION_TIME_FMT), message
                    ))

            # push last message group...
            if days_messages:
                msg_row_dict = row_dict(alum_fb_id, days_messages, date_last_message)
                writer.writerow(msg_row_dict)

                #print("user {} @ {}:".format(user, datetime_string))
                #print(entry[1], "\n")


def row_dict(alum_fb_id, messages, message_date):
    """
    Creates a dictionary with FB_NOTE_HEADERS as keys, ready to
    write out as a csv row.

    Populates the following columns:
        Facebook ID
        cn_fields.COMMENTS
        cn_fields.DATE_OF_CONTACT
        cn_fields.SUBJECT
        cn_fields.MODE_OF_COMMUNICATION
        cn_fields.INITIATED_BY_ALUM
        cn_fields.COMMUNICATION_STATUS
    """
    row_dict = dict.fromkeys(FB_NOTE_HEADERS)
    row_dict["Facebook ID"] = alum_fb_id
    row_dict[cn_fields.COMMENTS] = '\n'.join(messages)
    row_dict[cn_fields.DATE_OF_CONTACT] = \
        message_date.strftime(SALESFORCE_DATESTRING_FORMAT)
    row_dict[cn_fields.MODE_OF_COMMUNICATION] = SOCIAL_NETWORKING_MOC

    initiated_by_alum, communication_status, subject = \
        get_nature_of_exchange(messages, BULLS_FB_ID) # TODO cheating
    row_dict[cn_fields.INITIATED_BY_ALUM] = initiated_by_alum
    row_dict[cn_fields.COMMUNICATION_STATUS] = communication_status
    row_dict[cn_fields.SUBJECT] = subject
    return row_dict


def get_nature_of_exchange(messages_list, ac_facebook_id):
    """
    Arguments:
    - messages list:  list of strings, each like
                      '(<facebook_id>@facebook.com@<time info>) <message>'
    - ac_facebook_id: AC's Facebook ID (without '@facebook.com')

    Depending on the order and source of messages in a conversation, determine
    and return a tuple of the following:
        - initiated_by_alum (bool as a str): is first message from the alum
        - communication_status ('Successful Communication' when there is at
            least one message from the alum, 'Outreach Only' when AC sends
            message(s) that doesn't receive a response)
        - subject ('Facebook outreach transcript' when communication_status is
            'Outreach only', 'Facebook transcript' otherwise)
    """
    # match message formatting
    #ac_facebook_id = "({}".format(ac_facebook_id)

    # assume Outreach Only
    found_alum_message = False

    #print("messages passed to get_nature.. : {}".format(messages_list))
    if messages_list[0].startswith(ac_facebook_id):
        initiated_by_alum = False
    else:
        initiated_by_alum = True
        found_alum_message = True

    if not found_alum_message:
        # check the rest for message by alum
        for message in messages_list[1:]:
            if message.startswith(ac_facebook_id):
                continue

            found_alum_message = True
            break

    if found_alum_message:
        # TODO contsts
        communication_status = "Successful Communication"
        subject = "Facebook transcript"
    else:
        communication_status = "Outreach Only"
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

    parser = argparse.ArgumentParser(description="Specify input csv file")
    parser.add_argument(
        "infile",
        help="Input Facebook messages html file."
    )
    return parser.parse_args()


if __name__=="__main__":
    args = parse_args()
    process_fb_dump(args.infile)
