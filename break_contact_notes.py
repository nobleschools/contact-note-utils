"""
break_contact_notes.py

This script takes any csv file that has several columns with the same name but different data in
its corresponding cell. The script creates a new file that breaks the row into several rows 
that only has one set of data for each of the duplicate columns in the original file. 
"""
import argparse
import csv

def handle_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file",
        help="The base file that will be used for the output file. Must be in csv format."
    )
    return parser.parse_args()

def break_notes(filename):
    """substitute docstring because this function does way too much right now.
    """

    output_filename = 'merged_' + filename
    column_index = 0
    duplicate_col_start = 0
    duplicate_col_end = 0
    duplicate_col_dict = {}
    common_col_list = []
    duplicate_col_list = []
    header_row_list = []

    with open(filename, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        csv_w_file = open(output_filename, "w", newline='') 
        csv_writer = csv.writer(csv_w_file)
        header_row_list = csv_reader.__next__()

        for row_item in header_row_list:
            if row_item not in duplicate_col_dict:
                duplicate_col_dict[row_item] = column_index
                column_index += 1
            else:
                duplicate_col_start = duplicate_col_dict[row_item]
                duplicate_col_end = column_index - 1
                break
        column_index = 0
        header_row_list = remove_duplicate_list_items(header_row_list)

        for row in csv_reader:
            if header_row_list:
                csv_writer.writerow(header_row_list)
                header_row_list = []
            for row_item in row:
                if column_index < duplicate_col_start:
                    common_col_list.append(row_item)
                    column_index += 1
                elif column_index <= duplicate_col_end:
                    duplicate_col_list.append(row_item)
                    column_index += 1
                else:                    
                    csv_writer.writerow(common_col_list + duplicate_col_list)
                    duplicate_col_list = []
                    duplicate_col_list.append(row_item)
                    column_index = duplicate_col_start + 1
            csv_writer.writerow(common_col_list + duplicate_col_list)
            duplicate_col_list = []    
            common_col_list = []
            column_index = 0
        csv_w_file.close()


def remove_duplicate_list_items(dup_list):
    dup_list = list(dict.fromkeys(dup_list))
    return dup_list


if __name__ == "__main__":
    args = handle_args()
    break_notes(args.file)