from sqlalchemy import create_engine, Column, String, DateTime, BIGINT, Double, Integer, Float, Date, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
import json

load_dotenv()

pg_username=os.getenv("POSTGRES_USERNAME")
pg_password=os.getenv("POSTGRES_PASSWORD")
pg_host=os.getenv("POSTGRES_HOST")
pg_port=os.getenv("POSTGRES_PORT")
pg_db=os.getenv("POSTGRES_DB")

Base = declarative_base()

class FFRawTrade(Base):
    __tablename__ = "ff_raw_trade"
    
    id = Column(BIGINT, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    symbol_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    ltp = Column(Float, nullable=False)
    ltq = Column(BIGINT, nullable=False)
    atp = Column(Float, nullable=False)
    ttq = Column(BIGINT, nullable=False)
    day_open = Column(Float, nullable=False)
    day_high = Column(Float, nullable=False)
    day_low = Column(Float, nullable=False)
    prev_day_close = Column(Float, nullable=False)
    oi = Column(Float, nullable=False)
    prev_day_oi = Column(Float, nullable=False)
    turnover = Column(Float, nullable=False)
    tick_seq = Column(Integer, nullable=False)
    best_bid_price = Column(Float, nullable=False)
    best_bid_qty = Column(Integer, nullable=False)
    best_ask_price = Column(Float, nullable=False)
    best_ask_qty = Column(Integer, nullable=False)
    
    def to_dict(self):
        return {
            'id': self.id,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'symbol_id': self.symbol_id,
            'symbol': self.symbol,
            'ltp': self.ltp,
            'ltq': self.ltq,
            'atp': self.atp,
            'ttq': self.ttq,
            'day_open': self.day_open,
            'day_high': self.day_high,
            'day_low': self.day_low,
            'prev_day_close': self.prev_day_close,
            'oi': self.oi,
            'prev_day_oi': self.prev_day_oi,
            'turnover': self.turnover,
            'tick_seq': self.tick_seq,
            'best_bid_price': self.best_bid_price,
            'best_bid_qty': self.best_bid_qty,
            'best_ask_price': self.best_ask_price,
            'best_ask_qty': self.best_ask_qty,
        }

    def to_json_string(self):
        return json.dumps(self.to_dict())
    
    def add_all(self, session, datas):
        pass
    
Index('idx_symbol_timestamp_tickseq', FFRawTrade.symbol, FFRawTrade.timestamp, FFRawTrade.tick_seq, postgresql_using='btree')
    
class FFFilterTick(Base):
    __tablename__ = "ff_filter_trade"
    
    id=Column(Integer, primary_key=True)
    symbol_id=Column(String, nullable=True)
    timestamp=Column(DateTime, nullable=True)
    ltp=Column(Float, nullable=True)
    bid=Column(Float, nullable=True)
    bid_qty=Column(Integer, nullable=True)
    ask=Column(Float, nullable=True)
    ask_qty=Column(Integer, nullable=True)
    volume=Column(BIGINT, nullable=True)
    oi=Column(Integer, nullable=True)
    last_size=Column(BIGINT, nullable=True)
    lot_size=Column(Integer, nullable=True)
    symbol=Column(String, nullable=True)
    symbol_name=Column(String, nullable=True)
    exchange_token=Column(BIGINT, nullable=True)
    underlier_symbol=Column(String, nullable=True)
    underlier_price=Column(Float, nullable=True)
    trade_value=Column(Double, nullable=True)
    delta_volume=Column(BIGINT, nullable=True)
    delta_volume_value=Column(Double, nullable=True)
    # updated
    buy_volume=Column(BIGINT, nullable=True)
    buy_value = Column(Integer, nullable=True)
    sell_volume=Column(BIGINT, nullable=True)
    sell_value=Column(BIGINT, nullable=True)
    #
    symbol_type=Column(String, nullable=True)
    exchange=Column(String, nullable=True)
    tick_size=Column(String, nullable=True)
    expiry=Column(Date, nullable=True)
    option_type=Column(String, nullable=True)
    strike=Column(Integer, nullable=True)
    precision=Column(Float, nullable=True)
    oi_change=Column(Integer, nullable=True)
    moneyness=Column(String, nullable=True)
    # updated
    tag=Column(String, nullable=True)
    aggressor=Column(String, nullable=True)
    
    sweep1=Column(String, nullable=True)
    sweep1_volume=Column(BIGINT, nullable=True)
    sweep1_value=Column(Double, nullable=True)
    
    sweep2=Column(String, nullable=True)
    sweep2_volume=Column(BIGINT, nullable=True)
    sweep2_value=Column(Double, nullable=True)
    
    sweep3=Column(String, nullable=True)
    sweep3_volume=Column(BIGINT, nullable=True)
    sweep3_value=Column(Double, nullable=True)
    
    sweep4=Column(String, nullable=True)
    sweep4_volume=Column(BIGINT, nullable=True)
    sweep4_value=Column(Double, nullable=True)
    
    power_sweep=Column(String, nullable=True)
    power_sweep_volume=Column(BIGINT, nullable=True)
    power_sweep_value=Column(Double, nullable=True)
    
    block1=Column(String, nullable=True)
    block1_volume=Column(BIGINT, nullable=True)
    block1_value=Column(Double, nullable=True)
    
    block2=Column(String, nullable=True)
    block2_volume=Column(BIGINT, nullable=True)
    block2_value=Column(Double, nullable=True)
    
    """If Last Volume > Total of Last 20 Last volumes"""
    power_block=Column(String, nullable=True)
    power_block_volume=Column(BIGINT, nullable=True)
    power_block_value=Column(Double, nullable=True)
    
    """Current Price is 500 % > Previous Tick if price is below 0.25 OR Current Price is 200 % > previous Tick if the price above 0.25 and below 0.50 OR Current Price is 100% > previous Tick if the price is above 0.50 and below 1, OR Current Price is 50% > previous Tick if the price is above 1 and below 2 OR  Current Price is 25% > previous Tick if the price is above 2 and below 5 OR  Current Price is 10% > previous Tick if the price is above 5 and below 10 OR  Current Price is 5% > previous Tick if the price is above 10 and below 50 OR  Current Price is 4% > previous Tick if the price is above 50 and below 100 OR  Current Price is 3% > previous Tick if the price is above 100 and below 200 OR  Current Price is 2% > previous Tick if the price is above 200 and below 500 OR  Current Price is 1% > previous Tick if the price is above 500 AND Current Tick Time and Previous Tick Time > 60 seconds
    """
    unusal_prive_activity=Column(String, nullable=True)
    #
    strike_difference=Column(Float, nullable=True)
    symbol_root=Column(String, nullable=True)
    local_id=Column(String, nullable=True)
    is_contract_active=Column(Integer, nullable=True)
    oi_build_up=Column(String, nullable=True)
    sentiment=Column(String, nullable=True)
    tick_seq=Column(Integer, nullable=False)
    """for one minute tick"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cumulative_delta = 0.0 
    
class OneMinFilteringTick(Base):
    __tablename__ = "ff_filter_trade_one_min"
    
    id = Column(Integer, primary_key=True)
    symbol_id = Column(String, nullable=True)
    timestamp = Column(DateTime, nullable=True)
    open = Column(Double, nullable=True) 
    high = Column(Double, nullable=True) 
    low = Column(Double, nullable=True)  
    close = Column(Double, nullable=True) 
    volume = Column(BIGINT, nullable=True)
    oi = Column(Double, nullable=True)
    symbol = Column(String, nullable=True)
    underlier_symbol = Column(String, nullable=True)
    underlier_price = Column(Double, nullable=True) 
    trade_value = Column(Double, nullable=True)
    delta_volume = Column(BIGINT, nullable=True)
    delta_volume_value = Column(Double, nullable=True)

    # updated
    buy_volume = Column(BIGINT, nullable=True)
    buy_value = Column(Double, nullable=True)
    sell_volume = Column(BIGINT, nullable=True)
    sell_value = Column(Double, nullable=True)
    #
    oi_change = Column(BIGINT, nullable=True)
    
    min_delta = Column(BIGINT, nullable=True)
    max_delta = Column(BIGINT, nullable=True)
    oi_build_up = Column(String, nullable=True)


db_url=f"postgresql+psycopg2://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

engine=create_engine(db_url)

# Create a session
Session = sessionmaker(bind=engine)