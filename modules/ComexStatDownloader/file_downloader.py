from pySmartDL import SmartDL

class FileDownloader:
    """
    This module provides the `FileDownloader` class, which facilitates downloading 
    files from a given URL to a specified local path using the SmartDL library.
    Classes:
        FileDownloader: A class for downloading files with a configurable timeout.
    Dependencies:
        - SmartDL: Ensure the `pySmartDL` library is installed to use this module.
    Example:
        downloader = FileDownloader(timeout=15)
        downloader.download("https://example.com/file.zip", "/path/to/save/file.zip")
    """
    def __init__(self, timeout: int = 10):
        self.timeout = timeout

    def download(self, url: str, download_path: str) -> str:
        """
        Downloads a file from the given URL to the specified download path.

        Args:
            url (str): The URL of the file to be downloaded.
            download_path (str): The local path where the file will be saved.

        Returns:
            tuple: A tuple containing:
                - bool: True if the download was successful, False otherwise.
                - str or list: The destination path of the downloaded file if successful,
                  or a list of error messages if the download fails.

        Raises:
            Exception: If an unexpected error occurs during the download process.
        """
        obj = SmartDL(url, download_path, timeout=self.timeout, verify=False, progress_bar=False)
        obj.start(blocking=True)
        if obj.isSuccessful():
            return True, obj.get_dest()  # Return the path of the downloaded file
        else:
            return False, obj.get_errors()  # Return the error message if download fails
