# pylint: disable=C0114

from dotenv import load_dotenv # pylint: disable=E0401
import os # pylint: disable=C0411

# Carrega as variáveis do arquivo .env
load_dotenv()

config = {
    "db": {
        "host":         os.getenv("DB_HOST",        "localhost"),
        "port":         int(os.getenv("DB_PORT",    "5432")),
        "user":         os.getenv("DB_USER",        "postgres"),
        "password":     os.getenv("DB_PASSWORD",    "postgres"),
        "database":     os.getenv("DB_NAME",        "comexstat")
    },
}

years = {
    "years":           eval(os.getenv("YEARS", "[]") ) # pylint: disable=W0123
}
