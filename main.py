"""
This is the main entry point of the script. It calls the run function 
from the pipeline extract load and transform.
"""

# IMPORTS DE EXTRAÇÃO
from etl.extract.extract_comex_stat_ton_ncm import run as run_comex_stat_ton_ncm
from etl.extract.extract_ncm_vigentes import run as run_ncm_vigentes

# IMPORTS LOAD
from etl.load.load_create_tables_comex_stat import run as run_create_comex_stat_ton_ncm
from etl.load.load_rows_comex_stat_ton_by_years import run as run_load_rows_comex_stat_ton_by_years
from etl.load.load_rows_ncm_vigentes import run as run_load_rows_ncm_vigentes

# CONFIGURAÇÕES
from modules.Config import config, years

if __name__ == "__main__":

    # NUMERO DE LINHAS POR LOTE DE INSERÇÃO
    BATCH_SIZE = 30000

    # Run the extraction process - Comex Stat Ton by Years
    run_comex_stat_ton_ncm(years["years"])

    # Run the extraction process - NCM Vigentes
    run_ncm_vigentes()

    # Run the loading process - Create Tables
    run_create_comex_stat_ton_ncm(config)

    # Run the loading process - Load Rows - Comex Stat Ton by Years
    run_load_rows_comex_stat_ton_by_years(config, BATCH_SIZE, years["years"])

    # Run the loading process - Load Rows - NCM Vigentes
    run_load_rows_ncm_vigentes(config)
