"""
strip_salesforce_url.py

Clean a row of data containing full Salesforce URLs, replacing them with
only the Salesforce ID; eg. https://salesforce.com/0AA123 --> 0AA123
"""

import argparse
import csv

URL_COLUMN = 0


def strip_url(input_filename):
    """Strip out Salesforce ID from URL in URL_COLUMN in input_filename, and
    save back to stripped_<input_filename>.
    """

    with open(input_filename, "r") as infile:
        reader = csv.reader(infile)
        headers = next(reader)

        output_filename = f"stripped_{input_filename}"

        with open(output_filename, "w") as outfile:
            writer = csv.writer(outfile)
            headers[URL_COLUMN] = "Salesforce ID"
            writer.writerow(headers)

            for row in reader:
                url = row[URL_COLUMN]
                sf_id = url.split("/")[-1].strip()
                row[URL_COLUMN] = sf_id
                writer.writerow(row)

    print(f"Saved to {output_filename}.")


def parse_args():
    parser = argparse.ArgumentParser(description="Input csv file")
    parser.add_argument("infile", help="Input (csv) filename")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    strip_url(args.infile)
