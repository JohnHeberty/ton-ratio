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

# TRANSFORM
from etl.transform.transform_date import run as run_transform_date
from etl.transform.transform_create_view import run as run_create_view

# CONFIGURAÇÕES
from modules.Config import config, years

if __name__ == "__main__":
    print("Starting ETL process...")

    # Run the extraction process - Comex Stat Ton by Years
    print("Running extraction process for Comex Stat Ton by Years...")
    run_comex_stat_ton_ncm(years["years"])

    # Run the extraction process - NCM Vigentes
    print("Running extraction process for NCM Vigentes...")
    run_ncm_vigentes()

    # Run the loading process - Create Tables
    print("Running loading process to create tables...")
    run_create_comex_stat_ton_ncm(config)

    # Run the loading process - Load Rows - Comex Stat Ton by Years
    print("Running loading process to load rows for Comex Stat Ton by Years...")
    run_load_rows_comex_stat_ton_by_years(config, config["BATH_SIZE"], years["years"])

    # Run the loading process - Load Rows - NCM Vigentes
    print("Running loading process to load rows for NCM Vigentes...")
    run_load_rows_ncm_vigentes(config, config["BATH_SIZE"])

    # Run the transformation process - Transform Date, 01/01/2023 to 2023-01-01
    print("Running transformation process to transform date...")
    run_transform_date(config)

    # Run the transformation process - Create View
    print("Running transformation process to create view...")
    run_create_view(config)
