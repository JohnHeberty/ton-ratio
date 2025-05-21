from typing import Any, List, Tuple, Optional  # pylint: disable=C0411
from .idatabase_connection import IDatabaseConnection
from .postgres_connection import PostgresConnection

class DatabaseManager:
    """
    DatabaseManager provides a high-level interface for performing CRUD 
    (Create, Read, Update, Delete) operations
    on a database using a provided database connection.
    Args:
        connection (IDatabaseConnection): An object implementing the database connection interface.
    Methods:
        create(query: str, params: Tuple[Any, ...]) -> None:
            Executes an INSERT or similar query with the given parameters and commits 
            the transaction. Rolls back the transaction if an exception occurs.
        read(query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]:
            Executes a SELECT query with the given parameters and returns the fetched results 
            as a list of tuples.
        update(query: str, params: Tuple[Any, ...]) -> None:
            Executes an UPDATE query with the given parameters and commits the transaction.
            Rolls back the transaction if an exception occurs.
        delete(query: str, params: Tuple[Any, ...]) -> None:
            Executes a DELETE query with the given parameters and commits the transaction.
            Rolls back the transaction if an exception occurs.
        execute_raw(query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
            Executes a raw SQL query with the given parameters and commits the transaction.
    """
    def __init__(self, connection: IDatabaseConnection):  # pylint: disable=C0116
        self.connection = connection

    def create(self, query: str, params: Tuple[Any, ...]) -> None:  # pylint: disable=C0116
        try:
            self.connection.execute(query, params)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e

    def read(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]:  # pylint: disable=C0116
        self.connection.execute(query, params)
        return self.connection.fetchall()

    def update(self, query: str, params: Tuple[Any, ...]) -> None:  # pylint: disable=C0116
        try:
            self.connection.execute(query, params)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e

    def delete(self, query: str, params: Tuple[Any, ...]) -> None:  # pylint: disable=C0116
        try:
            self.connection.execute(query, params)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e

    def execute_raw(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:  # pylint: disable=C0116
        self.connection.execute(query, params)
        self.connection.commit()
