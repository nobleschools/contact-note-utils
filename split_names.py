"""
split_names.py

For those times when the 'Last' name column contains 'LastName, FirstName',
despite there also being a 'First' name column, which is also (sparingly) used.

Write back out to csv; assumes a 'Full Name' column has been added already.
"""

import argparse
import csv

# TODO parameterize
FIRST_NAME_HEADER = 'First Name'
LAST_NAME_HEADER = 'Last Name'
FULL_NAME_HEADER = 'Full Name'


def write_full_name(input_filename):

    output_filename = "with_fullnames_{}".format(input_filename)

    with open(input_filename) as infile:
        reader = csv.DictReader(infile)

        with open(output_filename, 'w') as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                if not row[FIRST_NAME_HEADER]:
                    # Last name col contains full name; split it
                    last_name, first_name = row[LAST_NAME_HEADER].split(',')
                    first_name = first_name.strip()
                    row[LAST_NAME_HEADER] = last_name
                    row[FIRST_NAME_HEADER] = first_name
                    row[FULL_NAME_HEADER] = " ".join((first_name, last_name))
                else:
                    first_name = row[FIRST_NAME_HEADER]
                    last_name = row[LAST_NAME_HEADER]
                    row[FULL_NAME_HEADER] = " ".join((first_name, last_name))

                writer.writerow(row)


def parse_args():
    """Get input file name."""

    parser = argparse.ArgumentParser(description=\
        "Specify input filename"
    )
    parser.add_argument(
        'infile',
        help="Input file (in csv format)"
    )
    return parser.parse_args()


if __name__=='__main__':
    args = parse_args()
    write_full_name(args.infile)
