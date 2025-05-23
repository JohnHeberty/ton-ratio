
# pylint: disable=C0114

import os # pylint: disable=C0411
# os.chdir("../")

from modules.database.postgres_connection import PostgresConnection # pylint: disable=C0413
from modules.database.database_manager import DatabaseManager   # pylint: disable=C0413
from jinja2 import Template # pylint: disable=C0411, C0413, E0401
from datetime import datetime # pylint: disable=C0411, C0413
import pandas as pd # pylint: disable=C0411, E0411, E0401, C0413
import json  # pylint: disable=C0411, C0413

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

def run(config: dict, batch_size: int):
    """
    Loads NCM (Nomenclatura Comum do Mercosul) data from a JSON file and inserts it into the 
    'ncm_vigentes' table in the database in batches.
    Args:
        config (dict): Configuration dictionary containing database connection parameters.
        batch_size (int): Number of rows to insert per batch.
    Workflow:
        1. Connects to the database using the provided configuration.
        2. Reads the NCM data from a JSON file located at 
        'data/external/ncm_vigentes/Tabela_NCM_Vigente_20250515.json'.
        3. Converts the 'Nomenclaturas' section of the JSON into a pandas DataFrame.
        4. Prepares an SQL INSERT statement dynamically based on the DataFrame columns.
        5. Identifies columns containing date information and formats them to 'YYYY-MM-DD'.
        6. Inserts the data into the database in batches of the specified size.
        7. Commits the transaction and closes the database connection.
    Raises:
        FileNotFoundError: If the JSON file does not exist.
        KeyError: If the expected 'Nomenclaturas' key is missing in the JSON.
        Exception: For any database or data processing errors.
    """

    # Conecta ao banco de dados
    db_manager = get_db(config)

    # Insere os dados no banco de dados
    file_path   = os.path.join("data","external","ncm_vigentes","Tabela_NCM_Vigente_20250515.json")

    with open (file_path, "r", encoding="latin1") as file:
        json_data = json.load(file)
        nomenclaturas = json_data["Nomenclaturas"]
        df = pd.DataFrame(nomenclaturas)

        # Lê o arquivo CSV
        columns         = df.columns
        print(f"Arquivo {file_path} lido com sucesso.")

        # Cria o cabeçalho da query de inserção
        columns_lower   = [col.lower() for col in columns.to_list()]
        placeholders    = ", ".join(["%s"] * len(columns_lower))
        tabela          = "ncm_vigentes" # pylint: disable=C0301
        template        = Template(
            "INSERT INTO {{ tabela }} ({{ cols|join(', ') }}) VALUES ({{ placeholders }})"
        )
        query_header    = template.render(
            tabela=tabela,
            cols=columns_lower,
            placeholders=placeholders
        )

        columns_data    = [colum for colum in columns.to_list() if "data" in colum.lower()]
        idx_columns     = [columns.get_loc(colum) for colum in columns_data]

        # Inserção em batch de x em x linhas
        rows            = df.to_numpy()
        for i in range(0, len(rows), batch_size):
            batch       = rows[i:i+batch_size]

            # ATUALIZA A DATA CASO O CAMPO SEJA DO TIPO DATA
            for row in batch:
                for idx in idx_columns:
                    row[idx] = datetime.strptime(row[idx], "%d/%m/%Y").strftime("%Y-%m-%d")

            params      = [tuple([str(item) for item in row]) for row in batch]
            db_manager.create_batch(query_header, params)
            print(f"Lote de linhas {i} a {i+len(batch)-1} inserido com sucesso.")

    db_manager.connection.commit()
    db_manager.connection.close()
