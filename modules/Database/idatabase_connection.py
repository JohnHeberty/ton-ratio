
from typing import Any, List, Tuple, Optional  # pylint: disable=C0411
from abc import abstractmethod

class IDatabaseConnection:
    """
    Interface for a database connection, defining the essential methods required for 
    interacting with a database.
    Methods
    -------
    connect() -> None
        Establishes a connection to the database.
    close() -> None
        Closes the database connection.
    execute(query: str, params: Optional[Tuple[Any, ...]] = None) -> None
        Executes a database query with optional parameters.
    fetchone() -> Optional[Tuple[Any, ...]]
        Retrieves the next row of a query result set, returning a single sequence, 
        or None when no more data is available.
    fetchall() -> List[Tuple[Any, ...]]
        Retrieves all (remaining) rows of a query result set as a list of tuples.
    commit() -> None
        Commits the current transaction.
    rollback() -> None
        Rolls back the current transaction.
    """
    @abstractmethod
    def connect(self) -> None:  # pylint: disable=C0116
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:  # pylint: disable=C0116
        raise NotImplementedError

    @abstractmethod
    def execute(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:  # pylint: disable=C0116
        raise NotImplementedError

    @abstractmethod
    def executemany(self):  # pylint: disable=C0116
        raise NotImplementedError

    @abstractmethod
    def fetchone(self) -> Optional[Tuple[Any, ...]]:  # pylint: disable=C0116
        raise NotImplementedError

    @abstractmethod
    def fetchall(self) -> List[Tuple[Any, ...]]:  # pylint: disable=C0116
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:  # pylint: disable=C0116
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:  # pylint: disable=C0116
        raise NotImplementedError
