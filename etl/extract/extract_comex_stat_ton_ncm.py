# pylint: disable=C0114

from modules.ComexStatDownloader import ComexStatDownloader

def run(years: list):
    """
    This function is the entry point for the script. It creates an instance of ComexStatDownloader
    and calls the download_files method to start the download process.
    """

    # Create an instance of ComexStatDownloader and call the download_files method
    csd = ComexStatDownloader()
    csd.download_files(years)
