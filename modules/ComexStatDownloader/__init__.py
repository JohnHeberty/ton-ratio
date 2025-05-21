from .file_downloader import FileDownloader
from .link_extractor import LinkExtractor
from .page_fetcher import PageFetcher
import os

class ComexStatDownloader:
    """
    ComexStatDownloader is a utility class for downloading and organizing foreign trade statistics data from the Brazilian Ministry of Development, Industry, and Foreign Trade (MDIC) website.
    Attributes:
        base_path (str): Local directory where downloaded files will be stored.
        base_url (str): URL of the MDIC statistics database page.
        page_fetcher (PageFetcher): Object responsible for fetching HTML content from the web.
        link_extractor (LinkExtractor): Object responsible for extracting download links from HTML.
        file_downloader (FileDownloader): Object responsible for downloading files from URLs.
    Methods:
        fetch_database_links():
            Fetches the HTML content from the base_url and extracts available database download links.
            Returns:
                dict: Nested dictionary of available databases, directions, years, and their corresponding download links.
        get_file_path(base, direction, year):
            Constructs the local file path for a given database, direction, and year.
            Args:
                base (str): The database name.
                direction (str): The trade direction (e.g., 'import', 'export').
                year (str or int): The year of the data.
            Returns:
                str: The constructed file path.
        download_files(limit_bases=2):
            Downloads CSV files for each available database, direction, and year, up to a specified number of databases.
            Args:
                limit_bases (int): Maximum number of databases to process (default is 2).
            Side Effects:
                Downloads files to the local filesystem and prints status messages.
    """
    def __init__(self, base_path="data", base_url=None, 
                 page_fetcher=None, link_extractor=None, file_downloader=None):
        self.base_path = base_path
        self.base_url = base_url or "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
        self.page_fetcher = page_fetcher or PageFetcher()
        self.link_extractor = link_extractor or LinkExtractor()
        self.file_downloader = file_downloader or FileDownloader()

    def fetch_database_links(self):
        """
        Fetches and extracts database links from the base URL.

        This method retrieves the HTML content from the specified base URL using the page_fetcher,
        then extracts and returns the relevant links using the link_extractor.

        Returns:
            list: A list of extracted database links.
        """
        html_str = self.page_fetcher.fetch(url=self.base_url)
        return self.link_extractor.extract(html=html_str)

    def get_file_path(self, base, direction, year):
        """
        Constructs the file path for a CSV file based on the provided base, direction, and year.

        Args:
            base (str): The base directory name.
            direction (str): The direction subdirectory (e.g., 'import', 'export').
            year (int or str): The year to include in the file name.

        Returns:
            str: The full path to the corresponding CSV file.
        """
        return os.path.join(self.base_path, "external", base, direction, f"{year}.csv")

    def download_files(self, limit_bases=2):
        """
        Downloads data files from remote links for a limited number of database bases.

        For each base (up to `limit_bases`), iterates through available directions and years,
        checks if the corresponding file already exists locally, and downloads it if not.
        Creates necessary directories if they do not exist.

        Args:
            limit_bases (int, optional): The maximum number of database bases to process. Defaults to 2.

        Prints:
            Status messages indicating whether each file was downloaded or already exists.
        """
        dbs = self.fetch_database_links()
        for base in list(dbs.keys())[:limit_bases]:
            for direction in dbs[base]:
                for year, link in dbs[base][direction].items():
                    path_file = self.get_file_path(base, direction, year)
                    if not os.path.exists(path_file):
                        os.makedirs(os.path.dirname(path_file), exist_ok=True)
                        self.file_downloader.download(link, path_file)
                        print(f"Downloaded: {path_file}")
                    else:
                        print(f"Already exists: {path_file}")

# if __name__ == "__main__":
#     downloader = ComexStatDownloader()
#     downloader.download_files()