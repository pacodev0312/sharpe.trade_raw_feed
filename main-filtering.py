# region import dependencies
from dotenv import load_dotenv
import os
from datetime import datetime as dt
import json

from services.masterfile import cache_symbol_id
from services.kafka_consumer import Consumerservice
from services.formatting import TickFomatting
from services.db_connection import Base, engine
import pytz
import time
import logging
import threading
# endregion

# region declaration
# Create the tables in the database
Base.metadata.create_all(engine)

load_dotenv()

bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVER")
sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM")
sasl_plain_username=os.getenv("KAFKA_CLIENT_USERNAME")
sasl_plain_password=os.getenv("KAFKA_CLIENT_PASSWORD")
security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL")


tick_convert = TickFomatting()

# timezone = pytz.timezone('Asia/Kolkata')
# def check_current_time():
#     while True:
#         current_time = dt.now(timezone).strftime("%H:%M")
        
#         # Check if the time is 08:00 AM
#         if current_time == "08:00" or current_time == "08:01":
#             tick_convert.cache_map = cache_symbol_id(tick_convert.user_name , tick_convert.password, tick_convert )
#             time.sleep(120)
#         print("check time for masterfile update")
#         time.sleep(60)

# check_time_thread = threading.Thread(target=check_current_time)
# check_time_thread.start()

logging.warning("consume message from producer")
test_consumer = Consumerservice(
    topic_name="feed_tick_trade",
    bootstrap_servers=bootstrap_servers,
    security_protocol=security_protocol,
    sasl_mechanism=sasl_mechanism,
    sasl_plain_username=sasl_plain_username,
    sasl_plain_password=sasl_plain_password,
    )
# endregion

# region main
@test_consumer.consume_message
def consume_message(message):
    tick_convert.neighboring_ticks(json.loads(message))
    
# endregion

