"""
summarize_contact_notes.py

Combine data from multiple cells in a csv into one for Contact Note Comments
field.
"""

import csv


with open("Lawson_contact.csv", "r") as infile:
    reader = csv.reader(infile)
    headers = next(reader)

    with open("summarized_Lawson_contact.csv", "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)

        for row in reader:
            comments = []
            for i in range(8, len(row)):
                comments.append(headers[i])
                comments.append(row[i])
            comments_blob = "\n".join(comments)
            row.append(comments_blob)
            row[0] = row[0].split()[0]
            writer.writerow(row)
