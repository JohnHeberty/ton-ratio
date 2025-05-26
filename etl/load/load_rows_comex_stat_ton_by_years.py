# pylint: disable=C0114

import os # pylint: disable=C0411
# os.chdir("../")

from modules.database.postgres_connection import PostgresConnection # pylint: disable=C0413
from modules.database.database_manager import DatabaseManager   # pylint: disable=C0413
from jinja2 import Template # pylint: disable=C0411, C0413, E0401
import pandas as pd # pylint: disable=C0411, E0411, E0401, C0413

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

def run(config: dict, batch_size: int, years: list):
    """
    Loads and inserts CSV data into a database in batches, filtered by specified years.
    This function scans predefined directories for CSV files, reads their contents,
    and inserts the data into corresponding database tables in batches of a given size.
    Only files whose names contain a year present in the `years` list are processed.
    Args:
        config (dict): Configuration dictionary for database connection.
        batch_size (int): Number of rows to insert per batch.
        years (list): List of years (as strings) to filter which files to process.
    Side Effects:
        - Connects to the database using the provided configuration.
        - Commits all inserted data and closes the database connection.
        - Prints progress messages to the console.
    """

    # Conecta ao banco de dados
    db_manager = get_db(config)

    # Insere os dados no banco de dados
    base_paths = [os.path.join("data","external","ncm"), os.path.join("data","external","mun")]

    for base_path in base_paths:
        for paths in os.listdir(base_path):
            path_end                = os.path.join(base_path, paths)
            for file_path in os.listdir(path_end):
                if file_path.endswith(".csv"):

                    year = "".join([n for n in os.path.basename(file_path) if n.isdigit()])
                    if year not in years:
                        continue

                    # Lê o arquivo CSV
                    file_path       = os.path.join(path_end, file_path)
                    df              = pd.read_csv(
                        file_path,
                        sep=";",
                        encoding="latin1",
                        low_memory=False
                    )
                    columns         = df.columns
                    print(f"Arquivo {file_path} lido com sucesso.")

                    # Cria o cabeçalho da query de inserção
                    columns_lower   = [col.lower() for col in columns.to_list()]
                    placeholders    = ", ".join(["%s"] * len(columns_lower))
                    tabela          = f"{os.path.basename(base_path).lower()}_{os.path.basename(path_end).lower()}" # pylint: disable=C0301
                    template        = Template(
                        "INSERT INTO {{ tabela }} ({{ cols|join(', ') }}) VALUES ({{ placeholders }})"
                    )
                    query_header    = template.render(
                        tabela=tabela,
                        cols=columns_lower,
                        placeholders=placeholders
                    )

                    # Inserção em batch de x em x linhas
                    rows            = df.to_numpy()
                    for i in range(0, len(rows), batch_size):
                        batch       = rows[i:i+batch_size]
                        params      = [tuple([str(item) for item in row]) for row in batch]
                        db_manager.create_batch(query_header, params)
                        print(f"Lote de linhas {i} a {i+len(batch)-1} inserido com sucesso.")

    db_manager.connection.commit()
    db_manager.connection.close()
