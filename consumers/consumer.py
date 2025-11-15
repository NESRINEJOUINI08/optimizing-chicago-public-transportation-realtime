"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        # Configure the broker properties.
        # Use host URLs (same as producers / workspace):
        #   Kafka broker: localhost:9092
        #
        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "cta.consumer.group",
            # let on_assign + offset_earliest control behaviour
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
        }

        # Create the Consumer, using the appropriate type.
        if is_avro:
            # add schema registry for Avro topics
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            pass
            


        #
        # Subscribe to topics (pattern / regex supported)
        #
        self.consumer.subscribe(
            [self.topic_name_pattern],
            on_assign=self.on_assign,
        )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest`
        # set the partition offset to the beginning / earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            msg = self.consumer.poll(timeout=self.consume_timeout)
        except SerializerError as error:
            logger.error(f"Error consuming data: {error}")
            return 0

        if msg is None:
            logger.debug("no message received")
            return 0

        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            return 0

        try:
            self.message_handler(msg)
            return 1
        except Exception as e:
            logger.error(f"Error in message_handler: {e}")
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        try:
            if self.consumer is not None:
                self.consumer.close()
        except Exception as e:
            logger.warning(f"Error while closing consumer: {e}")