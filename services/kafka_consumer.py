# region imports dependencies
from kafka import KafkaConsumer
from typing import Callable
import json

# endregion

# region consumer_service
class Consumerservice:
    def __init__(
        self, 
        topic_name, 
        bootstrap_servers, 
        sasl_mechanism, 
        security_protocol, 
        sasl_plain_username, 
        sasl_plain_password, 
        group_id=None,
        auto_offset_reset='latest',
        ):
        self.consumer = KafkaConsumer( topic_name,
                                    bootstrap_servers=bootstrap_servers,
                                    sasl_mechanism=sasl_mechanism,
                                    security_protocol=security_protocol,
                                    sasl_plain_username=sasl_plain_username,
                                    sasl_plain_password=sasl_plain_password,
                                    group_id=group_id,
                                    auto_offset_reset=auto_offset_reset,
                                    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        
    def consume_message(self, func: Callable):
        for message in self.consumer:
            func(message.value)

# endregion