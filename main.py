import csv
import mysql.connector


class TextProcessor:
    """ Classe che processa diversi tipi di file e salva i dati in una lista

    list_rows (list): Lista dei dati processati
    """

    def read_csv(self, file):
        self.list_rows = []
        with open(file, 'r') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            for row in reader:
                print(row)


class MariaDBManagement:

    def connect_db(self, database=None):
        if database:
            self.cnx = mysql.connector.connect(
                user='root',
                password='password',
                host='172.17.0.2',
                database=database
            )
        else:
            self.cnx = mysql.connector.connect(
                user='root',
                password='password',
                host='172.17.0.2'
            )

    def create_tables(self):
        self.connect_db()