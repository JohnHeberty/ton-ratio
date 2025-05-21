from .idatabase_connection import IDatabaseConnection
from typing import Any, List, Tuple, Optional  # pylint: disable=C0411
from psycopg2 import sql
import psycopg2

class PostgresConnection(IDatabaseConnection):
    """
    Classe para gerenciar a conexão com um banco de dados PostgreSQL.
    Métodos:
        __init__(host, database, user, password, port=5432):
            Inicializa a instância da conexão com os parâmetros fornecidos.
        connect():
            Estabelece a conexão com o banco de dados PostgreSQL usando os parâmetros fornecidos.
        close():
            Fecha o cursor e a conexão com o banco de dados, se estiverem abertos.
        execute(query, params=None):
            Executa uma consulta SQL no banco de dados.
            Parâmetros:
                query (str): Consulta SQL a ser executada.
                params (Optional[Tuple[Any, ...]]): Parâmetros opcionais para a consulta.
            Exceções:
                Exception: Se a conexão com o banco de dados não estiver estabelecida.
        fetchone():
            Recupera a próxima linha do resultado da última consulta executada.
            Retorna:
                Optional[Tuple[Any, ...]]: Próxima linha do resultado ou 
                None se não houver mais linhas.
        fetchall():
            Recupera todas as linhas do resultado da última consulta executada.
            Retorna:
                List[Tuple[Any, ...]]: Lista de todas as linhas do resultado ou 
                lista vazia se não houver resultados.
        commit():
            Realiza o commit da transação atual no banco de dados.
        rollback():
            Realiza o rollback da transação atual no banco de dados.
    """
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 5432):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.conn = None
        self.cur = None

    def connect(self) -> None:  # pylint: disable=C0116
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )
        self.cur = self.conn.cursor()

    def close(self) -> None:  # pylint: disable=C0116
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def execute(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:  # pylint: disable=C0116
        if not self.cur:
            raise RuntimeError("Database connection is not established.")
        self.cur.execute(query, params)

    def fetchone(self) -> Optional[Tuple[Any, ...]]:  # pylint: disable=C0116
        return self.cur.fetchone() if self.cur else None

    def fetchall(self) -> List[Tuple[Any, ...]]:  # pylint: disable=C0116
        return self.cur.fetchall() if self.cur else []

    def commit(self) -> None:  # pylint: disable=C0116
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:  # pylint: disable=C0116
        if self.conn:
            self.conn.rollback()
