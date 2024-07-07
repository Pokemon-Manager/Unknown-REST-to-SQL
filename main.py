import os
import sqlite3
from sqlite3 import Error
import csv
import argparse


# Initialize parser
parser = argparse.ArgumentParser()

# Adding optional argument
parser.add_argument("-i", "--input", help = "Input CSV Folder Path")

# Read arguments from command line
args = parser.parse_args()

def create_connection(path):
    connection = None
    try:
        print(f"Connecting to SQLite DB: {path}")
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection
    
class RESTToSQL:
    foreign_identifiers : list = []
    headers_in_files : dict = {}
    def __init__(self, cursor: sqlite3.Cursor):
        self.cursor = cursor

    def main(self):
        pass

    def parse(self, args):
        if not args.input:
            print("Nothing to parse")
            return
        list = os.listdir(args.input)
        for filename in list: # Gets all of the filenames from the input folder, for foregin key references.
            self.foreign_identifiers.append(str(filename).split('.')[0])
        for filename in list: # Gets all of the headers for each file, for foregin key references.
            if not filename.endswith('.csv'):
                continue
            with open(args.input + filename, newline='', encoding='utf-8') as file:
                headers = file.readline().split(',')
                self.headers_in_files[str(filename).split('.')[0]] = headers
        for filename in list:
            print(filename)
            if not filename.endswith('.csv'):
                continue
            with open(args.input + filename, newline='', encoding='utf-8') as file:
                headers = file.readline().split(',')
                self.createTableWithHeaders(table_name=str(filename).split('.')[0], headers=self.headers_in_files[str(filename).split('.')[0]])
                # self.deleteData(table_name=str(filename).split('.')[0])
                csvreader = csv.reader(file, delimiter=',', quotechar='"')
                query = f'INSERT INTO {str(filename).split(".")[0]} VALUES({("?," * (len(headers)))[:-1]});'
                for row in csvreader:
                    try:
                        self.cursor.execute(query, row)
                    except Exception as e:
                        print(row)
                        print(e)
        self.cursor.connection.commit()
    
    def deleteData(self, table_name: str):
        query = f'DELETE FROM {table_name};'
        self.cursor.execute(query)
        return

    def createTableWithHeaders(self, table_name: str, headers):
        self.cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
        query = f'CREATE TABLE {table_name}('
        primary_key = None
        foreign_keys = []
        for header in headers:
            additional_parameters = ""
            if header == "id":
                additional_parameters = "INT NOT NULL"
                primary_key = header
            if self.checkIfForeignKeyExists(header):
                additional_parameters = "INT NOT NULL"
                foreign_keys.append(header)
            query += f'{header} {additional_parameters}, '
        if primary_key != None:
            query += f'PRIMARY KEY({primary_key}), '
        for foreign_key in foreign_keys:
            query += f'FOREIGN KEY({foreign_key}) REFERENCES {self.getVaildForeignTable(foreign_key)}(id), '
        query = query.removesuffix(", ")
        query = query.removesuffix(",") + ");"
        print(query)
        self.cursor.execute(self.validityCheckForTableCreation(query))
        return
    
    def validityCheckForTableCreation(self, query: str):
        query = query.replace("order", "sequence")
        return query

    def checkIfForeignKeyExists(self, key_name: str):
        if not key_name.endswith("_id"):
            print("Did not end with _id")
            return False
        if not self.foreign_identifiers.__contains__(self.changeSingularToPlural(key_name.removesuffix("_id"))):
            print("Not in foreign identifiers.")
            print("key: " + key_name)
            print("singular: " + self.changeSingularToPlural(key_name.removesuffix("_id")))
            print("plural: " + self.changePluralToSingular(key_name.removesuffix("_id")))
            return False
        if not list(self.headers_in_files[self.changeSingularToPlural(key_name.removesuffix("_id"))]).__contains__("id"):
            print("Did not contain id")
            return False
        return True

    def getVaildForeignTable(self, key_name: str):
        key = key_name.removesuffix("_id")
        return self.changeSingularToPlural(key)
    
    def changePluralToSingular(self, filename: str):
        if filename.endswith("ies") and not filename.endswith("species"):
            return filename.removesuffix("ies") + "y"
        if filename.endswith("s"):
            return filename.removesuffix("s")
        return filename

    def changeSingularToPlural(self, filename: str):
        if filename.endswith("y"):
            return filename.removesuffix("y") + "ies"
        else:
            return filename + "s"
def main():
    print("Hello World")
    connection = create_connection("Global.db")
    cursor = connection.cursor()
    rest_to_sql = RESTToSQL(cursor)
    rest_to_sql.parse(args)

main()