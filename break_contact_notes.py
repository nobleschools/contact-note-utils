"""
break_contact_notes.py

This script takes any csv file that has several columns with the same name but different data in
its corresponding cell. The script creates a new file that breaks the row into several rows 
that only has one set of data for each of the duplicate columns in the original file. 

For example, if the contents of 'filename' were:
    Name, SFID, Subject, Comments, Subject, Comments
    Bob, ABC123, Outreach A, Comments A, Outreach B, Comments B

then this would result in an output file with:
    Name, SFID, Subject, Comments
    Bob, ABC123, Outreach A, Comments A
    Bob, ABC123, Outreach B, Comments B

This script is mainly meant for spreadsheets that have mutliple Contact Notes on each row.
"""
import argparse
import csv


def handle_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file",
        help="The base file that contains sections of duplicate columns. Must be in csv format.",
    )
    return parser.parse_args()


def break_notes(filename):
    """The main function in this program.

    This function takes a file and puts it through the process described in the
    documentation at the beginning of the file. In the end a new file is created using the filename
    of the input file as a base. 

    Args:
        filename: the name of the file to be used in the script.
    """
    output_filename = "merged_" + filename
    input_row_count = 0
    with open(filename, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        csv_w_file = open(output_filename, "w", newline="")
        csv_writer = csv.writer(csv_w_file)
        header_row_list = next(csv_reader)
        duplicate_col_start, duplicate_col_end = get_endpoints_of_duplicate_columns(
            header_row_list
        )
        header_row_list = remove_duplicate_list_items(header_row_list)
        csv_writer.writerow(header_row_list)
        for row in csv_reader:
            write_rows(row, csv_writer, duplicate_col_start, duplicate_col_end)
            input_row_count += 1
        csv_w_file.close()
        dup_col_num = len(header_row_list) - duplicate_col_start
        check_that_output_rows_are_correct(
            input_row_count, output_filename, dup_col_num
        )
        print(f"Success! Your new file is called {output_filename}.")


def get_endpoints_of_duplicate_columns(header_row_list):
    """Gets the endpoints of the first set of duplicate columns.
    
    The positions of the first set of duplicate columns in the input file will be used while iterating through
    the output file, as well as to judge how long each row will be. Therefore it's important they these 
    endpoints are calculated early.

    Args:
        header_row_list: A list of strings that are the names of the header rows in the input file.

    Returns:
        duplicate_col_start and duplicate_col_end, two integers that represent both the start and end
        of the first set of duplicate columns in the input file.
    """
    column_index = 0
    duplicate_col_start = 0
    duplicate_col_end = 0
    duplicate_col_dict = {}
    for header in header_row_list:
        if header not in duplicate_col_dict:
            duplicate_col_dict[header] = column_index
            column_index += 1
        else:
            duplicate_col_start = duplicate_col_dict[header]
            duplicate_col_end = column_index - 1
            break
    return duplicate_col_start, duplicate_col_end


def write_rows(row, csv_writer, duplicate_col_start, duplicate_col_end):
    """Write rows to the output file.
    
    This function will write a variable amount of rows to an output file for each singular row in the input.
    The number of rows written is dependent on the amount of duplicate columns in the input. The amount 
    of columns in each rows is dependent on the duplicate column endpoints that were calculated earlier.

    Args:
        row: A row of data taken from the input file.
        csv_writer: The CSV object representing the output file to be written to.
        duplicate_col_start: The starting point of the duplicate columns in the input file.
        duplicate_col_end: The ending point of the duplicate columns in the input file.
    """
    column_index = 0
    common_col_list = []
    duplicate_col_list = []
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


def remove_duplicate_list_items(dup_list):
    """Creating a dictionary made of keys from a list and then converting those keys back
    to a list will remove any duplicate elements from a list while keeping the elements in
    the same order.
    """

    dup_list = list(dict.fromkeys(dup_list))
    return dup_list


def check_that_output_rows_are_correct(input_rows, output_filename, dup_col_num):
    """Checks to make sure that the output file has the correct number of rows based on the input.

    The output file should have a row number equal to the number of rows in the input file multiplied by the 
    number of times the duplicate columns were repeated. (So in the example at the top of this file, this number 
    would be two since the Subject and Comments columns were repeated twice.) This function will check to
    see if the output file has a number of rows that meets this criteria, and will throw an error if
    it doesn't.

    Args:
        input_rows: The number of rows in the input file.
        output_filename: The filename of the output file.
        dup_col_num: The number of times the duplicate columns in the input file were repeated.
    """
    output_rows = -1  # starts at -1 to account for the header row
    output = open(output_filename, "r")
    output_check = csv.reader(output)
    for row in output_check:
        output_rows += 1
    assert output_rows == (
        input_rows * dup_col_num
    ), f"Error! The number of rows in the output file should be {input_rows * dup_col_num}, but it is {output_rows} instead."
    output.close()


if __name__ == "__main__":
    args = handle_args()
    break_notes(args.file)
