"""
Transforms and Loads data into the local SQLite3 database

"""
import sqlite3
import csv
import os
from prettytable import PrettyTable
import requests

#EXTRACT


def extract(
    url="https://raw.githubusercontent.com/jjsantos01/aire_cdmx/master/datos/contaminantes_2019-05-16.cvs",
    file_path="ET/data/my_air_cont.csv",
):
    """ "Extract a url to a file path"""
    with requests.get(url, timeout=10) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path

extract()

#TRANSFORM and LOAD : load the csv file and insert into a new sqlite3 database
def load(dataset="ET/data/my_air_cont.csv"):
    """ "Transforms and Loads data into the local SQLite3 database"""

    # prints the full working directory and path
    print(os.getcwd())
    with open(dataset, newline="", encoding='utf-8') as file:
        payload = csv.reader(file, delimiter=",")
        conn = sqlite3.connect("ET/data/my_airDB.db")
        c = conn.cursor()

        c.execute("DROP TABLE IF EXISTS my_airDB")
        c.execute("CREATE TABLE my_airDB (Fecha,Hora,ZP,imecas,zona,contaminante,color)")
        # insert
        c.executemany("INSERT INTO my_airDB VALUES (?,?, ?, ?, ?, ?,?)", payload)
        conn.commit()
        conn.close()
        return "my_airDB.db"

#load()

def print_table(cursor, data):
    table = PrettyTable()
    table.field_names = [i[0] for i in cursor.description]
    for row in data:
        table.add_row(row)
    print(table)


def queryCountIMECAS():
    conn = sqlite3.connect("ET/data/my_airDB.db")
    cursor = conn.cursor()
    print("Zones in dataset:\n")
    cursor.execute("SELECT zona, COUNT(*) AS total FROM my_airDB GROUP BY zona")
    print_table(cursor, cursor.fetchall())
    print("\n IMECAS in this dataset:\n")
    cursor.execute("SELECT imecas, COUNT(*) AS total FROM my_airDB GROUP BY imecas")
    print_table(cursor, cursor.fetchall())
    print("\n IMECAS per zone in this dataset:\n")
    cursor.execute(
        "SELECT zona,imecas, COUNT(*) type_of_IMECAS_by_zone FROM my_airDB GROUP BY zona,imecas ORDER BY zona DESC"
    )
    print_table(cursor, cursor.fetchall())
    conn.close()
    return "Success"

def main():
    #Extract
    print("Extracting data...")
    extract()

    # Transform and load
    print("Transforming data...")
    load()

    # Query
    print("Querying data...")
    queryCountIMECAS()


if __name__ == "__main__":
    main()