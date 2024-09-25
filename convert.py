#! /bin/python3
from os.path import exists, isdir, join
from os import mkdir, unlink, access
import os
import requests
from sqlite3 import connect, Connection
import csv
from base64 import b64encode
from typing import Optional, Iterable, Any

DOWNLOAD_DIR  = 'download'
NORTHWIND_URL = 'https://raw.githubusercontent.com/jpwhite3/northwind-SQLite3/main/dist/northwind.db'
NORTHWIND_DB  = 'northwind.db'
NORTHWIND_PATH  = join(DOWNLOAD_DIR, NORTHWIND_DB)

# CSV does not support nulls.  So, all nulls will be converted to 
# NULL_CONVERT (an empty string).  Change NULL_CONVERT to convert nulls
# to something else.
NULL_CONVERT=''

# BLOBS contains information about which tables and columns are photo blobs.
# The keys is the table name and the value is the column containing blobs.
BLOBS = { 'Categories' : 'Picture', 
          'Employees'  : 'Photo' }

def download(remoteUrl : str, localPath : str, /) -> None:
    r"""Downloads file from remoteUrl and saves it to localPath.
    
    This uses the response library to download a file from remoteUrl and save
    it to localPath.  If there is an exception during the download it attempts
    to delete localPath to avoid a partial transfer.

    This will print status and progress messages to sttdout.
    """
    r = requests.get(remoteUrl, stream=True)
    if r.status_code != 200:
        raise RuntimeError(f"Cannot download {remoteUrl!r}")
    contentLength = int(r.headers.get('Content-Length', 0))

    try:
        with open(localPath, "wb") as f:
            print(f"Downloading to {localPath}")
            bytesRead = 0
            for chunk in r.iter_content(chunk_size=8192 * 16):
                f.write(chunk)
                bytesRead += len(chunk)
                megsRead = round(bytesRead / (1024 * 1024), 1)
                percent = f"{round(bytesRead * 100 / contentLength, 1)}%" if contentLength else ""
                print(f"Progress: {megsRead}MB {percent}          ", end='\r')
            print()
    except:
        # If and exception was raised, remove the local copy of the file to prevent
        # a partial download.
        try:
            if exists(localPath):
                unlink(localPath)
        except:
            print(f"Wanring: could not delete partial transfer at {localPath}")
        raise
 
def downloadNorthwind():
    r"""Downloads the SQLite Northwind database.
     
    This downloads the Northwind database from GitHub into the download
    directory.  If the database is already there, it returns without
    downloading anything.  If the download directory does not exist, it is
    created.
    """

    if not exists(DOWNLOAD_DIR):
        mkdir(DOWNLOAD_DIR)
    elif not isdir(DOWNLOAD_DIR):
        raise RuntimeError(f"{DOWNLOAD_DIR!r} is not a directory.")
    elif not access(DOWNLOAD_DIR, os.W_OK | os.R_OK | os.X_OK):
        raise RuntimeError(f"Need access to {DOWNLOAD_DIR!r}.")
    if not exists(NORTHWIND_PATH):
        download(NORTHWIND_URL, NORTHWIND_PATH)

def convertTable(conn : Connection, tableName : str, csvPath : str, keepBlobs : bool = False) -> None:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM '{tableName}'")

    header = [ d[0] for d in cur.description ]

    blobColumn = None
    if tableName in BLOBS:
        blobColumn = header.index(BLOBS[tableName])

    if blobColumn is not None and not keepBlobs:
        del header[blobColumn]

    def filterRows(rows : Iterable[list[Any]]):
        if blobColumn is not None:
            if keepBlobs:
                rows = [ [ attr if i != blobColumn else b64encode(attr).decode('utf-8') for i, attr in enumerate(row) ] 
                         for row in rows ]
            else:
                rows = [ [ attr for i, attr in enumerate(row) if i != blobColumn ] 
                         for row in rows ]
    
        rows = (  [  str(attr) if attr is not None else NULL_CONVERT 
                     for attr in row  ]
                  for row in rows )
        
        return rows


    # Build a table from the query.  sqlite3 module converts SQL null to 
    # Python None. The comprehension below will convert None to NULL_CONVERT
    table = list(filterRows(cur))
    
    # Create CSV file from the data in the query.  The first row
    # in the CSV file will be the column names from the SQL SELECT.
    with open(csvPath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in table:
            writer.writerow(row)

    # Double check that we get the same data back when the CSV File is read
    with open(csvPath, "r", newline="") as f:
        reader = csv.reader(f)
        header2 = next(reader)
        assert header == header2, "Read back of CSV header to not match query."
        for i, row in enumerate(reader):
            assert table[i] == row, ( f"Read back of CSV file did not match query.\n"
                                      f"Table: {tableName}\n"
                                      f"Row number: {i}\n"
                                      f"Original: {table[i]!r}\n"
                                      f"Read back: {row!r}\n"
                                    )

if __name__ == '__main__':
    downloadNorthwind()

    conn = connect(NORTHWIND_PATH)
    cur = conn.cursor()

    for tableName, in cur.execute("""SELECT name FROM sqlite_schema WHERE type='table'"""):
        if tableName.startswith('sqlite_'):
            continue
        # if table in { 'Categories', 'Employees'}:
        #     continue
        csvPath = f"{tableName.lower()}.csv"
        print(f"Generating: {csvPath}")
        convertTable(conn, tableName, csvPath, False)

    # Create a xxx_blob.csv for tables that contain blobs.
    for tableName in BLOBS:
        csvPath = f"{tableName.lower()}_base64.csv"
        print(f"Generating: {csvPath}")
        convertTable(conn, tableName, csvPath, True)
