from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Float, String, Integer, DateTime
from datetime import datetime, timezone

Base = declarative_base()

class CryptoPrice(Base):
    """Define a tabela de preços de criptomoedas no banco de dados."""
    __tablename__ = "crypto_prices"

    internal_id = Column(Integer, primary_key=True, autoincrement=True)  # ID interno do banco

    id = Column(String(100), nullable=False)        # ID da moeda (ex: 'bitcoin')
    symbol = Column(String(100), nullable=False)    # Sigla (ex: BTC)
    name = Column(String(100), nullable=False)     # Nome da moeda
    image = Column(String(200), nullable=False)        

    current_price = Column(Float, nullable=False)
    market_cap = Column(Float, nullable=True)
    market_cap_rank = Column(Integer, nullable=True)
    total_volume = Column(Float, nullable=True)

    high_24h = Column(Float, nullable=True)
    low_24h = Column(Float, nullable=True)
    price_change_24h = Column(Float, nullable=True)
    price_change_percentage_24h = Column(Float, nullable=True)

    ath = Column(Float, nullable=True)
    ath_date = Column(DateTime, nullable=True)
    atl = Column(Float, nullable=True)
    atl_date = Column(DateTime, nullable=True)

    last_updated = Column(DateTime, nullable=True)
    time_stamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))  # quando foi inserido no seu sistema
