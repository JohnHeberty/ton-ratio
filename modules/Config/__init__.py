# pylint: disable=C0114

from dotenv import load_dotenv # pylint: disable=E0401
import os # pylint: disable=C0411

# Carrega as variáveis do arquivo .env
load_dotenv()

config = {
    "DB_HOST":          os.getenv("DB_HOST",        "localhost"),
    "DB_PORT":          int(os.getenv("DB_PORT",    "5432")),
    "DB_USER":          os.getenv("DB_USER",        "postgres"),
    "DB_PASSWORD":      os.getenv("DB_PASSWORD",    "postgres"),
    "DB_NAME":          os.getenv("DB_NAME",        "comexstat"),
    "BATH_SIZE":        int(os.getenv("BATCH_SIZE",     "1000"))
}

years = {
    "years": [
        row.strip()
        for row in      os.getenv("YEARS",          "2023").split(",")
    ]
}
