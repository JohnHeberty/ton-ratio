# pylint: disable=C0114

import os
# os.chdir("../")

from modules.database.postgres_connection import PostgresConnection # pylint: disable=C0413
from modules.database.database_manager import DatabaseManager   # pylint: disable=C0413

def get_db(config: dict):
    """
    Establishes a connection to the PostgreSQL 'comexstat' database and returns a 
    DatabaseManager instance.
    Returns:
        DatabaseManager: An instance of DatabaseManager connected to the specified 
        PostgreSQL database.
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
    Executes the process of creating database tables using SQL scripts.
    This function connects to the database using the provided configuration,
    reads the SQL statements for table creation from a file, executes them,
    and then commits and closes the database connection.
    Args:
        config (dict): A dictionary containing database configuration parameters.
    Raises:
        FileNotFoundError: If the SQL file for creating tables does not exist.
        Exception: If there is an error during database operations.
    """

    # Conecta ao banco de dados
    db_manager = get_db(config)

    # Cria as tabelas no banco de dados
    path_create_table = os.path.join("repository", "querys", "create_table.sql")
    with open(path_create_table, "r", encoding="utf-8") as file:
        create_tables_sql = file.read()
        db_manager.execute_raw(create_tables_sql)

    db_manager.connection.commit()
    db_manager.connection.close()
