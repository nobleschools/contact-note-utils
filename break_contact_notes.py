import argparse
import csv
from os import path

def handle_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file",
        help="The file to be modified. Must be in csv format"
    )
    return parser.parse_args()

def break_notes(filename):
    source_file = filename
    output_filename = 'merged_' + source_file
    column_index = 0
    duplicate_col_start = 0
    duplicate_col_end = 0
    duplicate_col_dict = {}
    duplicate_col_start_name = ""
    duplicate_col_end_name = ""
    common_col_list = []
    duplicate_col_list = []
    header_row_list = []
    header_row = True

    with open(source_file, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        csv_w_file = open(output_filename, "w", newline='') 
        csv_writer = csv.writer(csv_w_file)
        for row in csv_reader:
            if header_row:
                header_row = False
                header_row_list = row
                for item in row:
                    if item not in duplicate_col_dict:
                        duplicate_col_dict[item] = column_index
                        duplicate_col_end_name = item
                        column_index += 1
                    else:
                        duplicate_col_start = duplicate_col_dict[item]
                        duplicate_col_end = column_index - 1
                        break
                column_index = 0
                continue

            if header_row_list:
                csv_writer.writerow(header_row_list[:9])
                header_row_list = []
            for item in row:
                if column_index < duplicate_col_start:
                    common_col_list.append(item)
                    column_index += 1
                elif column_index <= duplicate_col_end:
                    duplicate_col_list.append(item)
                    column_index += 1
                else:                    
                    csv_writer.writerow(common_col_list + duplicate_col_list)
                    duplicate_col_list = []
                    duplicate_col_list.append(item)
                    column_index = duplicate_col_start + 1
            csv_writer.writerow(common_col_list + duplicate_col_list)
            duplicate_col_list = []    
            common_col_list = []
            column_index = 0
        csv_w_file.close()


if __name__ == "__main__":
    args = handle_args()
    break_notes(args.file)