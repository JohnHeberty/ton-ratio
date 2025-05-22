# pylint: disable=C0114

from modules.ncm_vigente import NcmVigenteDownloader

def run():
    """
    Executes the process of downloading the current NCM 
    (Nomenclatura Comum do Mercosul) file.
    Args:
        config: Configuration object or dictionary containing necessary 
        parameters for the extraction process.
    Returns:
        None
    """

    NVD = NcmVigenteDownloader()
    NVD.download_file()
