### Struttura DB
Per prima cosa, dobbiamo creare la struttura del database:
`python db_fill`

* Per i dati del DB guardare dentro alla funzione: `MariaDBManagement.connect_db()`

### Documentazione
https://mariadb.com/kb/it/documentazione-di-mariadb/
https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html

### Regex python
https://docs.python.org/3/library/re.html


### Select utilizatta durante la lezione con JOIN
SELECT c.*, i.* FROM complaints.Clienti c
	INNER JOIN complaints.clienti_indirizzi ci on c.Id = ci.Cliente_id 
		INNER JOIN complaints.Indirizzi i on ci.Indirizzo_id =i.id; 
        
https://i.stack.imgur.com/VQ5XP.png

