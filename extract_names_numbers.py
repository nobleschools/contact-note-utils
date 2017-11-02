"""
extract_names_numbers.py

Extract names and phone numbers from a Facebook dump of contact info.
"""
import csv
import re

from bs4 import BeautifulSoup as bs

INPUT_FILENAME = "contact_info.htm"

with open(INPUT_FILENAME, "r") as fhand:
    contact_soup = bs(fhand.read(), "html.parser")

contact_soup = contact_soup.select("table")[1]
number_re = re.compile("\+(\d+)")

with open("names_numbers.csv", "w") as fhand:
    writer = csv.writer(fhand)
    # add a row for adding Safe ID later
    writer.writerow(("Name", "Contact__c", "Mobile"))
    for row in contact_soup.select("tr")[1:]: # first is header
        cells = row.select("td")
        name = cells[0].text.strip()
        number = number_re.search(cells[1].text.strip()).groups()[0]
        writer.writerow((name, "", number))

