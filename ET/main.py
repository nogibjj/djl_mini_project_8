"""
Transforms and Loads data into the local SQLite3 database

"""
import sqlite3
from prettytable import PrettyTable
import psutil
import time


#data processing with sqllite

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
    start_time = time.time()
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
    end_time = time.time() 
    process = psutil.Process()
    memory_usage = process.memory_info().rss / (1024 * 1024)  # in MB

    return "Success", end_time - start_time, memory_usage


def main():
    # Query
    print("Querying data...")
    queryCountIMECAS()
    result, runtime, memory = queryCountIMECAS()
    print(f"\nRuntime: {runtime:.2f} seconds")
    print(f"Memory Usage: {memory:.2f} MB")


if __name__ == "__main__":
    main()