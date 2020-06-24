import csv
import mysql.connector
import re


class TextProcessor:
    """ Classe che processa diversi tipi di file e salva i dati in una lista

    list_rows (list): Lista dei dati processati
    """

    def __init__(self):
        self.mapping = {
            "Date received": "Data_ricevuta",
            "Product": "Prodotto",
            "Sub-product": "Sottoprodotto",
            "Issue": "Problema",
            "Sub-issue": "Sottoproblema",
            "Consumer complaint narrative": "Narrativa",
            "Company public response": "Risposta_publica",
            "Company": "Nome",
            "State": "Paese",
            "ZIP code": "Cap",
            "Tags": "Tags",
            "Consumer consent provided?": "consenso_cliente",
            "Submitted via": "Inviato_via",
            "Date sent to company": "Data_invio",
            "Company response to consumer": "Risposta_cliente",
            "Timely response?": "Risposta_tempestiva",
            "Consumer disputed?": "Cliente_contestato",
            "Complaint ID": "Id"
        }

    def read_csv(self, file):
        self.list_rows = []
        with open(file, 'r') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            for row in reader:
                self.process_row(row)

    def process_row(self, row):
        ditta_fields = [
            'Company',
            'State',
            'ZIP code'
        ]
        ditta = list(filter(lambda x: x[0] in ditta_fields, row.items()))
        reclami = list(filter(lambda x: x[0] not in ditta_fields, row.items()))
        ditta_json = {}
        for key, value in ditta:
            name = self.get_db_field(key)
            if name == 'Cap':
                value = re.sub('([a-z]|[A-Z])+', '0', value)
            if value == '':
                value = 'null'
            ditta_json[name] = value

        reclami_json = {}
        for key, value in reclami:
            name = self.get_db_field(key)
            if name in ('consenso_cliente', 'Risposta_tempestiva', 'Cliente_contestato'):
                if value.lower() == 'yes':
                    value = '1'
                if value.lower() == 'no':
                    value = '0'
                if value.lower() == 'n/a' or value == '':
                    value = 'null'
                else:
                    value = '1'
            reclami_json[name] = value

        conection = MariaDBManagement()
        conection.connect_db('complaints')
        ditta_id = conection.insert('Ditte', ditta_json)
        reclami_json['ditta_id'] = ditta_id
        conection.insert('Reclami', reclami_json)
        conection.disconect_db()

    def get_db_field(self, field):
        # TODO: Gestire i possibili errori o null
        return self.mapping[field]


class MariaDBManagement:

    """ Global functions
    """
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
    
    def insert(self, table, data):
        columns = ",".join(data.keys())
        values_str =[]
        for value in data.values():
            if value == 'null':
                values_str.append(str(value))
            else:
                values_str.append(self._scape_string(value))
        values = ",".join(values_str)
        insert_statement = "INSERT {table} ({columns}) VALUE ({values}) ".format(
            table=table,
            columns=columns,
            values=values
        )
        #print(insert_statement)
        _cursor = self.cnx.cursor()
        try:
            _cursor.execute(insert_statement)
        except Exception as e:
            print("Error executing statement: {}".format(insert_statement))
            print(e)
        self.cnx.commit()
        return _cursor._insert_id

    def _scape_string(self, input):
        new_value = str(input).replace("'", "\\'")
        return "'" + new_value + "'"

    def disconect_db(self):
        self.cnx.close()

    """ Create db functions
    """
    def create_database(self):
        _cursor = self.cnx.cursor()
        _cursor.execute(
            "CREATE DATABASE IF NOT EXISTS complaints"
        )
        print("database creato!")

    def create_tables(self):
        _cursor = self.cnx.cursor()
        _cursor.execute(
            """CREATE TABLE IF NOT EXISTS Clienti(
                Id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
                Nome varchar(100) NOT NULL,
                Indirizzo varchar(255),
                Eta tinyint,
                Data_registrazione datetime DEFAULT NOW()
            )
            """
        )
        _cursor.execute(
            """CREATE TABLE IF NOT EXISTS Ditte(
                Id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
                Paese varchar(100) NOT NULL,
                Cap int,
                Nome varchar(100)
            )
            """
        )
        _cursor.execute(
            """CREATE TABLE IF NOT EXISTS Reclami(
                Id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
                Data_ricevuta date NOT NULL,
                Prodotto varchar(255),
                Sottoprodotto varchar(255)
            )
            """
        )

    def modify_table_reclami(self):
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

    def add_columns_reclami(self):
        _cursor = self.cnx.cursor()
        _cursor.execute(
            """ALTER TABLE Reclami
                ADD COLUMN Problema varchar(225),
                ADD COLUMN Sottoproblema varchar(225),
                ADD COLUMN Narrativa text,
                ADD COLUMN Risposta_publica varchar(225),
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

    def create_indirizzi(self):
        _cursor = self.cnx.cursor()
        _cursor.execute(
            """CREATE TABLE IF NOT EXISTS Indirizzi(
                id int(11) NOT NULL AUTO_INCREMENT,
                strada varchar(255) NOT NULL,
                numero int(11),
                PRIMARY KEY (id)
            )
            """
        )

        _cursor.execute(
            """CREATE TABLE IF NOT EXISTS Clienti_Indirizzi(
                Cliente_id int NOT NULL,
                Indirizzo_id int NOT NULL,
                PRIMARY KEY (Cliente_id,Indirizzo_id),
                CONSTRAINT clienti_indirizzi_FK FOREIGN KEY (Cliente_id) REFERENCES Clienti (Id),
                CONSTRAINT clienti_indirizzi_FK_1 FOREIGN KEY (Indirizzo_id) REFERENCES Indirizzi (id)
            )
            """
        )

        _cursor.execute("ALTER TABLE Clienti DROP COLUMN Indirizzo")