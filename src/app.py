import time
import requests
import pandas as pd
import logging
import logfire
import os
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, CryptoPrice
from dotenv import load_dotenv
from logging import basicConfig, getLogger

logfire.configure()
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(logging.INFO)
logfire.instrument_requests()
logfire.instrument_sqlalchemy()

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_table():
    Base.metadata.create_all(engine)
    logger.info("Tabela criada/verificada com sucesso!")

def extract_data():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'brl',
        'order': 'market_cap_desc',
        'per_page': 250,
        'page': 1
    }

    # Sending requests
    response = requests.get(url, params=params)

    if response.status_code == 200:
        logger.info('Conexão bem-sucedida com a API. Extraindo dados...')
        data = response.json()
        df = pd.DataFrame(data)
        # print(df.columns)
        df = df[[
            'id', 'symbol', 'name', 'image', 'current_price', 'market_cap', 
            'market_cap_rank', 'total_volume', 'high_24h', 'low_24h', 
            'price_change_24h', 'price_change_percentage_24h', 
            'ath', 'ath_date', 'atl', 'atl_date', 'last_updated'
        ]]
        df['time_stamp'] = datetime.now(timezone.utc)
        
        date_columns = ['ath_date', 'atl_date', 'last_updated', 'time_stamp']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None)
        
        format_data = df.to_dict(orient="records")
        return format_data

    else:
        logger.error(f"Erro na API: {response.status_code}")
        return None

def save_data_postgres(data):
    session = Session()
    novo_registro = CryptoPrice(**data)
    session.add(novo_registro)
    session.commit()
    session.close()
    logger.info(f"[{data['timestamp']}] Save data on PostgreSQL!")

    
def pipeline_crypto():
    with logfire.span("Executando pipeline ETL CryptoCoin"):
        
        with logfire.span("Extrair Dados da API CoinGecko"):
            json_data = extract_data()
        
        if not json_data:
            logger.error("Falha na extração dos dados. Abortando pipeline.")
            return
        
        with logfire.span("Tratar Dados da Crypto"):
            transform_data = json_data
        
        with logfire.span("Salvar Dados no Postgres"):
            save_data_postgres(transform_data)

        logger.info(
            f"Pipeline finalizada com sucesso!"
        )

if __name__ == "__main__":
    create_table()
    data_json = extract_data()
    print(data_json)
    
    while True:
        try:
            json_data = extract_data()
            if json_data:
                for item in json_data:
                    save_data_postgres(item)

            time.sleep(120)
        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuário. Finalizando...")
            break
        except Exception as e:
            print(f"Erro durante a execução: {e}")
            time.sleep(120)