from bs4 import BeautifulSoup
import os

class LinkExtractor:
    """
    This module provides the `LinkExtractor` class, which is used to extract download links
    for OpenStreetMap (OSM) or Protocolbuffer Binary Format (PBF) files from HTML content.
    Classes:
        LinkExtractor: A class that extracts a specific download link for a given country
        from an HTML page.
    Dependencies:
        - BeautifulSoup from bs4: Used for parsing HTML content.
        - re: Used for regular expression matching.
        - urljoin from urllib.parse: Used to construct absolute URLs from relative links.
    Exceptions:
        - ValueError: Raised when no matching download link is found in the HTML content.
    """
    def __init__(self) -> None:
        self.dbs = {}

    def extract(self, html: str) -> dict:
        """
        Extracts download links from HTML content, organized by directory and year.

        Args:
            html (str): The HTML content to parse.

        Returns:
            dict: A nested dictionary with directory names as keys, years as 
            subkeys, and links as values.
        """
        soup = BeautifulSoup(html, "html.parser")
        self.dbs = {}

        tables = soup.find_all("table", class_=True)
        for table in tables:
            for link_tag in table.find_all("a", href=True):
                href = link_tag["href"]
                file_name = os.path.basename(href)
                dir_name = os.path.basename(os.path.dirname(href))
                year = ''.join(filter(str.isdigit, file_name))
                inport_export = file_name[:3]
                if not year.isdigit():
                    continue
                self.dbs.setdefault(dir_name, {}).setdefault(inport_export, {})[year] = href
        return self.dbs
