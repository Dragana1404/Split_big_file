import os
import glob
import csv
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

#This script shows how we can split a large file into smaller files and create a function that will insert those smaller files into one Snowflake table.
#This can be one of the solutions for inserting large files with a large number of rows into the Snowflake table.
#The essence of solving the problem in this way is to first split a large file into smaller files with a smaller number of rows and to go through those files with a for loop and place each of them in the same table on the Snowflake platform.
#This procedure ensures that no row in the file is lost, and this can be checked when a large file is printed and the output shows the number of rows, and later when the files are inserted into Snowflake you can see how many rows that table has.

ctx = snowflake.connector.connect( #This function connects Python and Snowflake using user data
   user='<username>',
   password='<password>',
   account='<account>'
)
cs= ctx.cursor() #Making a constructor for creating a Cursor object, to use data from Snowflake

sql = "USE role ACCOUNTADMIN" #Function1 which selects a role account
cs.execute(sql) #Execute sql query
sql = "SELECT CURRENT_ROLE()" #Function2 which sets a role from Function1 to be current
cs.execute(sql) #Execute sql query


def split(filehandler, keep_headers=True): #Defines a function that will split a large file into smaller files
    reader = csv.reader(filehandler, delimiter=',') #Reading csv file
    #Function split the file on row # basics

    # Variable declartion:
    row_limit = 1000 #Number of rows
    output_name_template = 'output_%s.csv' #The name of small file
    output_path = r'C:\Users\Nenad\Desktop\Data\proba' #The path where smaller files will be stored

    current_piece = 1 #The current counter to be incremented in each file
    current_out_path = os.path.join(#A function that associates files with a specific name in a specific path
        output_path,
        output_name_template % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=',') #creating files
    current_limit = row_limit #The current number of rows to increase

    if keep_headers: #The condition that makes the header
        headers = next(reader)
        current_out_writer.writerow(headers) #File headers are created

    for i, row in enumerate(reader): #A function that makes rows in all smaller files
        if i + 1 > current_limit:
            current_piece += 1 #The counter is incremented
            current_limit = row_limit * current_piece #The number of rows increases
            current_out_path = os.path.join(#A function that associates files with a specific name in a specific path
                output_path,
                output_name_template % current_piece
            )
            current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=',') #Files are created by merging headers and rows
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)


if __name__ == "__main__": #Spliting a large file using the defined split function
    print("file split Begins")
    split(open(r"C:\Users\Nenad\PycharmProjects\untitled15\BIGFILE.csv"))
    print("File split Ends")


os.chdir(r'C:\Users\Nenad\Desktop\Data\proba') #Path to the folder..
file_extension=".csv" #..file extension..
all_filenames = [i for i in glob.glob(f"*{file_extension}")] #..and read all files from that folder with the csv extension


for file in all_filenames: #A loop that places all files in a table
 df=pd.read_csv(file, delimiter=',') #Files are converted to a DataFrame
 cs.execute("USE DRAGANA").fetchall() #Execute sql query which use Database DRAGANA and fetch result from query
 write_pandas(ctx, df, 'BIGTABLE') #Function that inserts dataframes into a table named 'BIGTABLE' into the Database 'DRAGANA' using connector



