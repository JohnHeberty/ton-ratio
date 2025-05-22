# pylint: disable=C0114

import os
# os.chdir("../")

from modules.database.postgres_connection import PostgresConnection # pylint: disable=C0413
from modules.database.database_manager import DatabaseManager   # pylint: disable=C0413

def run(config):
    """
    Função principal para executar o script de criação de tabelas.
    """

    # Conexão com o banco de dados
    conn = PostgresConnection(
        config["DB_HOST"],
        config["DB_NAME"],
        config["DB_USER"],
        config["DB_PASSWORD"],
        config["DB_PORT"]
    )
    conn.connect()

    # Gerenciador de banco de dados
    db_manager = DatabaseManager(conn)

    # Cria as tabelas no banco de dados
    path_create_table = os.path.join("repository", "querys", "create_table.sql")
    with open(path_create_table, "r", encoding="utf-8") as file:
        create_tables_sql = file.read()
        db_manager.execute_raw(create_tables_sql)
