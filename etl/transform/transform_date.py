
# pylint: disable=C0114

import os # pylint: disable=C0411
# os.chdir("../")

from modules.database.postgres_connection import PostgresConnection # pylint: disable=C0413
from modules.database.database_manager import DatabaseManager   # pylint: disable=C0413

def get_db(config: dict):
    """
    Establishes a connection to the PostgreSQL 'comexstat' database and returns a DatabaseManager instance.
    Returns:
        DatabaseManager: An instance of DatabaseManager connected to the specified PostgreSQL database.
    Raises:
        Exception: If the connection to the database fails.
    Note:
        Update the 'pwd' variable with the actual password before use.
    """

    # Certifique-se de definir as variáveis host, db, user, pwd antes de usar
    conn = PostgresConnection(
        config["DB_HOST"],
        config["DB_NAME"],
        config["DB_USER"],
        config["DB_PASSWORD"],
        config["DB_PORT"]
    )
    conn.connect()
    db_manager = DatabaseManager(conn)
    return db_manager

def run(config: dict):
    """
    Executes a SQL transformation by reading a query from a file and running 
    it against the database.
    Args:
        config (dict): Configuration dictionary containing database connection parameters.
    Side Effects:
        - Connects to the database using parameters from `config`.
        - Reads a SQL query from 'repository/querys/transform_ncm_vigentes.sql'.
        - Executes the SQL query on the connected database.
        - Commits the transaction and closes the database connection.
    """

    # Conecta ao banco de dados
    db_manager = get_db(config)

    # Insere os dados no banco de dados
    sql_path   = os.path.join("repository","querys","transform_ncm_vigentes.sql")

    with open (sql_path, "r", encoding="latin1") as file:
        query = file.read()
        db_manager.execute_raw(query)
        db_manager.connection.commit()
        db_manager.connection.close()
