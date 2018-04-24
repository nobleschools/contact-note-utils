"""
summarize_contact_notes.py

Combine data from multiple cells in a csv into one for Contact Note Comments
field.
"""

import csv


with open("rivas_contactnotes.csv", "r") as infile:
    reader = csv.reader(infile)
    headers = next(reader)

    with open("summarized_rivas_contactnotes.csv", "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)

        for row in reader:
            comments = []
            for i in range(3, 7):
                comments.append(f"{headers[i]}\n{row[i]}")
            comments_blob = "\n\n".join(comments)
            row[7] = comments_blob
            writer.writerow(row)
