"""
combine_prs_for_contact_notes.py

Combine *_Details csv worksheets from the Persistence Reports into one file
containing all relevant Contact Note information.
"""

import argparse
import csv
from datetime import datetime

FIELDS_TO_KEEP = (
    "Campus",
    "Network ID",
    "SFID",
    "Last",
    "First",
    "CN: Subject",
    "CN: Comments",
    "CN: Date of Communication",
    "CN: Communication Status",
    "CN: Mode of Communication",
)

def combine_files(input_files):
    """Pull FIELDS_TO_KEEP columns from each file and save to one combined
    for upload_contact_notes.py
    """
    today = datetime.today()
    output_filename = f"contactnotes_{today.year}{today.month}{today.day}.csv"
    with open(output_filename, "w") as outhand:
        outfile_writer = csv.DictWriter(
            outhand, FIELDS_TO_KEEP, restval="MISSING", extrasaction="ignore"
        )
        outfile_writer.writeheader()

        for source_file in input_files:
            with open(source_file, "r") as inhand:
                reader = csv.DictReader(inhand)
                # strip byte-order marking
                reader.fieldnames[0] = reader.fieldnames[0].strip("\ufeff")
                print(reader.fieldnames)

                for row in reader:
                    outfile_writer.writerow(row)

    print(f"Saved to {output_filename}")


def parse_args():
    """
    infiles: input csv files, in the format of *_Details worksheets
             from the Persistence Reports
    """

    parser = argparse.ArgumentParser(description="Specify input csv files")
    parser.add_argument(
        "infiles",
        nargs="+",
        help="Input file (in csv format)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    combine_files(args.infiles)

