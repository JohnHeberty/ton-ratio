from typing import Optional
import requests

class PageFetcher:
    """
    Module: page_fetcher
    This module provides the `PageFetcher` class, which is responsible for fetching
    content from a given URL using the `requests` library. It abstracts the process
    of making HTTP GET requests and ensures proper error handling for failed requests.
    Classes:
        - PageFetcher: A utility class for fetching web page content.
    Dependencies:
        - requests: A library for making HTTP requests.
    Usage Example:
        fetcher = PageFetcher()
        content = fetcher.fetch("https://example.com")
        print(content)
    """
    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session: requests.Session = session or requests.Session()

    def fetch(self, url: str) -> str:
        """
        Fetches the content of a given URL using an HTTP GET request.

        Args:
            url (str): The URL to fetch.

        Returns:
            str: The response content as a string.

        Raises:
            requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful status code.
        """
        response: requests.Response = self.session.get(url)
        response.raise_for_status()
        return response.text