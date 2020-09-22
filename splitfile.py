import os
import csv


def split(filehandler, keep_headers=True):
    reader = csv.reader(filehandler, delimiter=',')

    """
        Function split the file on row # basics
    """

    # Variable declartion:
    row_limit = 100
    output_name_template = 'output_%s.csv'
    output_path = r'C:\Users\Nenad\Desktop\Data'

    current_piece = 1
    current_out_path = os.path.join(
        output_path,
        output_name_template % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=',')
    current_limit = row_limit
    if keep_headers:
        headers = next(reader)
        current_out_writer.writerow(headers)
    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
                output_path,
                output_name_template % current_piece
            )
            current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=',')
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)


if __name__ == "__main__":
    print("file split Begins")
    split(open(r"C:\Users\Nenad\PycharmProjects\untitled15\bigtable_py.csv"))
    print("File split Ends")