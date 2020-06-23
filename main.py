import csv


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
