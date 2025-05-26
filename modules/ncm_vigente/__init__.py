# pylint: disable=C0114

from ..downloader import FileDownloader
from datetime import datetime
import os

class NcmVigenteDownloader:
    """
    NcmVigenteDownloader is responsible for managing the download and retrieval of the most recent 
    NCM (Nomenclatura Comum do Mercosul) files from a specified remote source.
    Attributes:
        base_path (str): The base directory where files will be stored.
        base_url (str): The URL from which the NCM JSON file is downloaded.
        file_downloader (FileDownloader): An object responsible for handling file downloads.
    Methods:
        get_file_path():
        download_file():
            Downloads the NCM file from the specified base URL to a local path 
            if it does not already exist.
            If the file is already present, it skips the download.
    """
    def __init__(self, base_path="data", file_downloader=None):
        self.base_path          = base_path
        self.base_url           = "https://portalunico.siscomex.gov.br/classif/api/publico/nomenclatura/download/json" # pylint: disable=C0301
        self.file_downloader    = file_downloader or FileDownloader()

    def get_file_path(self):
        """
        Retrieves the most recent file from the 'ncm_vigentes' directory based on the highest 
        numeric value in the filename, deletes all other files in the directory, and returns 
        the name of the selected file.

        Returns:
            str: The filename of the most recent file if found, otherwise the path to the directory.

        Raises:
            FileNotFoundError: If the 'ncm_vigentes' directory does not exist.
            PermissionError: If the process does not have permission to delete files.
            OSError: For other issues encountered during file operations.
        """
        path_file   = os.path.join(self.base_path, "external", "ncm_vigentes")
        os.makedirs(path_file, exist_ok=True)
        file_choice = ""
        if len(os.listdir(path_file)) > 0:
            file_choice = max(
                os.listdir(path_file),
                key=lambda x: "".join([row for row in x if row.isdigit()])
            )
            for file_now in os.listdir(path_file):
                if os.path.basename(file_choice) != file_now:
                    os.remove(os.path.join(path_file, file_now))
            return True, file_choice if file_choice != "" else path_file
        return False, file_choice if file_choice != "" else path_file

    def download_file(self):
        """
        Downloads a file from the specified base URL to a local path if it does not already exist.

        The method constructs the file path using `get_file_path` with the provided `base`, 
        `direction`, and `year` parameters. If the file does not exist at the constructed 
        path, it creates the necessary directories, downloads the file using 
        `file_downloader.download`, and prints a confirmation message. If the file already 
        exists, it prints a message indicating so.

        Returns:
            None
        """
        status, path_file = self.get_file_path()
        if not status:
            os.makedirs(os.path.dirname(path_file), exist_ok=True)
            download_path = os.path.join(
                path_file,
                f"ncm_vigentes_{datetime.now().strftime('%Y%m%d')}.json"
            )
            self.file_downloader.download(self.base_url, download_path)
            print(f"Downloaded: {path_file}")
        else:
            print(f"Already exists: {path_file}")

# if __name__ == "__main__":
#     downloader = ComexStatDownloader()
#     downloader.download_files()
