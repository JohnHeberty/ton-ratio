
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
    Executes the process of creating a database view by running a SQL script.
    This function connects to the database using the provided configuration,
    reads a SQL script from the 'repository/querys/create_view_ratio.sql' file,
    executes the script to create or update a database view, commits the transaction,
    and closes the database connection.
    Args:
        config (dict): A dictionary containing database configuration parameters.
    Raises:
        FileNotFoundError: If the SQL script file does not exist.
        Exception: If there is an error executing the SQL script or committing the transaction.
    """
    
    # Conecta ao banco de dados
    db_manager = get_db(config)

    # Insere os dados no banco de dados
    sql_path   = os.path.join("repository","querys","create_view_ratio.sql")

    with open (sql_path, "r", encoding="latin1") as file:
        query = file.read()
        db_manager.execute_raw(query)
    
    db_manager.connection.commit()
    db_manager.connection.close()
