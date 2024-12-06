# region Imports
from truedata_ws.websocket.TD import TD
from truedata_ws.websocket.TD_hist import cache_symbol_id
from truedata_ws.websocket.support import full_feed
from dotenv import load_dotenv
import os
import logging
from services.kafk_service import produce_message, close_producer
from services.db_connection import FFRawTrade, Session, Base, engine
import json
import time
from datetime import datetime as dt
from typing import Optional
import pytz

load_dotenv()
session = Session()
Base.metadata.create_all(engine)
# endregion

# region truedata
config = {
    "username": os.getenv("TRUEDATA_USERID"),
    "password": os.getenv("TRUEDATA_PASSWORD"),
    "liveport": os.getenv("TRUEDATA_LIVEPORT")
}

# Create td_obj for conection
td_obj = TD(login_id=config["username"], password=config["password"], live_port=config["liveport"], log_level=logging.DEBUG, log_format="%(message)s", full_feed=True, historical_api=False)

print('\nStarting Real Time Feed.... ')
time.sleep(1)  # Ensure the connection is established

# region truedata callback functions

@td_obj.bidask_callback
def mybidask_callback(bidask_data):
    bidask_dict = {
        "timestamp": bidask_data.timestamp.isoformat(),
        "symbol_id": bidask_data.symbol_id,
        "symbol": bidask_data.symbol,
        "bid": bidask_data.bid,
        "ask": bidask_data.ask
    }
    produce_message(topic_name="feed_tick_bidask", key=bidask_data.symbol, message=json.dumps(bidask_dict))

old_timestamp: Optional[dt] = None
count = 0
batch_db = []
batch_kafka = []

@td_obj.full_feed_trade_callback
def myfullfeed_callback(tick_data: full_feed):
    timestamp = dt.now()
    timestamp_truncated = timestamp.replace(microsecond=0)
    new_row = FFRawTrade()
    
    new_row.timestamp=tick_data.timestamp
    new_row.symbol_id=tick_data.symbol_id
    new_row.symbol=tick_data.symbol
    new_row.ltp=tick_data.ltp
    new_row.ltq=tick_data.ltq
    new_row.atp=tick_data.atp
    new_row.ttq=tick_data.ttq
    new_row.day_open=tick_data.day_open
    new_row.day_high=tick_data.day_high
    new_row.day_low=tick_data.day_low
    new_row.prev_day_close=tick_data.prev_day_close
    new_row.oi=tick_data.oi
    new_row.prev_day_oi=tick_data.prev_day_oi
    new_row.turnover=tick_data.turnover
    new_row.tick_seq=tick_data.tick_seq
    new_row.best_bid_price=tick_data.best_bid_price
    new_row.best_bid_qty=tick_data.best_bid_qty
    new_row.best_ask_price=tick_data.best_ask_price
    new_row.best_ask_qty=tick_data.best_ask_qty

    global old_timestamp, count, batch_kafka, batch_db
    if old_timestamp is not None:
        if timestamp_truncated.isoformat() == old_timestamp.isoformat():
            # Same timestamp, accumulate the data
            batch_db.append(new_row)
            batch_kafka.append(new_row.to_dict())
        else:
            # New timestamp, flush the previous batch
            try:
                # Insert the batch into the database
                session.bulk_save_objects(batch_db)  # SQLAlchemy bulk insert
                session.commit()

                # Produce the message to Kafka
                produce_message(topic_name="feed_tick_trade", key=tick_data.symbol, message=json.dumps(batch_kafka))

                print("Batch processed. Count:", len(batch_db))
            except Exception as e:
                session.rollback()  # Rollback in case of an error
                print(f"Error processing batch: {e}")

            # Reset after processing
            batch_db = [new_row]
            batch_kafka = [new_row.to_dict()]

    else:
        # First tick received, initialize the count and batch
        batch_db.append(new_row)
        batch_kafka.append(new_row.to_dict())

    # Update `old_timestamp` for the next comparison
    old_timestamp = timestamp_truncated
    
@td_obj.greek_callback
def mygreek_callback(greek_data):
    greek_dict = {
        "timestamp": greek_data.timestamp.isoformat(),
        "symbol_id": greek_data.symbol_id,
        "symbol": greek_data.symbol,
        "iv": greek_data.iv,
        "delta": greek_data.delta,
        "gamma": greek_data.gamma,
        "theta": greek_data.theta,
        "vega": greek_data.vega,
        "rho": greek_data.rho
    }
    produce_message(topic_name="feed_tick_option_greek", key=greek_data.symbol, message=json.dumps(greek_dict))
# endregion

# region update master file
timezone = pytz.timezone('Asia/Kolkata')
def check_current_time():
    current_time = dt.now(timezone).strftime("%H:%M")
    
    # Check if the time is 08:00 AM
    if current_time == "08:00" or current_time == "08:01":
        td_obj.symbol_id_map_dict = cache_symbol_id(td_obj.login_id , td_obj.password, td_obj ) if td_obj.full_feed else 0
        time.sleep(120)
# endregion

# Keep the script running
try:
    while True:
        check_current_time()
        time.sleep(120)
except KeyboardInterrupt:
    print("Shutting down gracefully...")
    close_producer()
# endregion
