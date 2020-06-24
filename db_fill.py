from .main import MariaDBManagement


if __name__ == "__main__":
    conection = MariaDBManagement()
    conection.connect_db()

    conection.create_database()
    conection.create_tables()
    conection.modify_table_reclami()
    conection.add_columns_reclami()
    conection.create_indirizzi()

    conection.disconect_db()