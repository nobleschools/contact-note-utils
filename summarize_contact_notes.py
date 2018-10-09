"""
summarize_contact_notes.py

Combine data from multiple cells in a csv into one for Contact Note Comments
field.
"""

import csv

INPUT_FILENAME = "PUT_FILENAME_HERE"
# will range(FIRST_COL.., LAST_COL..+1)
FIRST_COL_TO_SUMMARIZE = 4 # 0-indexed
LAST_COL_TO_SUMMARIZE = 10 # "

with open(INPUT_FILENAME, "r") as infile:
    reader = csv.reader(infile)
    headers = next(reader)

    with open("summarized_"+INPUT_FILENAME, "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)

        for row in reader:
            comments = []
            for i in range(FIRST_COL_TO_SUMMARIZE, LAST_COL_TO_SUMMARIZE+1):
                comments.append(f"{headers[i]}\n{row[i]}")
            comments_blob = "\n\n".join(comments)
            row[LAST_COL_TO_SUMMARIZE+1] = comments_blob
            writer.writerow(row)
