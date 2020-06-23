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

    def create_database(self):
        _cursor = self.cnx.cursor()
        _cursor.execute(
            "CREATE DATABASE IF NOT EXISTS complaints"
        )
        print("database creato!")

    def disconect_db(self):
        self.cnx.close()

    def create_table(self):
        _cursor = self.cnx.cursor()
        _cursor.execute(
            """CREATE TABLE IF NOT EXISTS Clienti(
                Id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
                Nome varchar(100) NOT NULL,
                Indirizzo varchar(255),
                Eta tinyint,
                Data_registrazione datetime NOT NULL
            )
            """
        )

    def modify_table(self):
        _cursor = self.cnx.cursor()
        _cursor.execute(
            """ALTER TABLE Reclami ADD COLUMN Ditta_id int
            """
        )

        _cursor.execute(
            """ALTER TABLE Reclami 
            ADD CONSTRAINT FOREIGN KEY (Ditta_id) REFERENCES Ditte(Id)
            """
        )

    def add_columns(self):
        _cursor = self.cnx.cursor()
        _cursor.execute(
            """ALTER TABLE Reclami
                ADD COLUMN Problema varchar(225),
                ADD COLUMN Sottoproblema varchar(225),
                ADD COLUMN Narrativa varchar(225),
                ADD COLUMN Risposta_publica boolean,
                ADD COLUMN Tags varchar(225),
                ADD COLUMN consenso_cliente boolean,
                ADD COLUMN Inviato_via varchar(225),
                ADD COLUMN Data_invio date,
                ADD COLUMN Risposta_cliente varchar(225),
                ADD COLUMN Risposta_tempestiva boolean,
                ADD COLUMN Cliente_contestato boolean,
                ADD COLUMN Cliente_id int
            """
        )

        _cursor.execute(
            """ALTER TABLE Reclami 
            ADD CONSTRAINT FOREIGN KEY (Cliente_id) REFERENCES Clienti(Id)
            """
        )

    def insert(self, table, data):
        columns = ",".join(data.keys())
        values_str = list(map(lambda x: "'"+str(x)+"'", data.values()))
        values = ",".join(values_str)
        insert_statement = "INSERT {table} ({columns}) VALUE ({values}) ".format(
            table=table,
            columns=columns,
            values=values
        )
        print(insert_statement)
        _cursor = self.cnx.cursor()
        _cursor.execute(insert_statement)
        self.cnx.commit()
