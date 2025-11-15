"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            # Kafka broker on the host
            "bootstrap.servers": "localhost:9092",
            # Schema Registry on the host
            "schema.registry.url": "http://localhost:8081",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            config=self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        try:
            # Create an AdminClient using the same bootstrap servers
            client = AdminClient(
                {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
            )

            # Check if topic already exists
            existing_topics = client.list_topics(timeout=5).topics
            if self.topic_name in existing_topics:
                logger.info(f"Topic {self.topic_name} already exists")
                return

            logger.info(
                f"Creating topic {self.topic_name} with "
                f"{self.num_partitions} partition(s) and "
                f"{self.num_replicas} replication factor"
            )

            new_topic = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas,
            )

            futures = client.create_topics([new_topic])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic} successfully created")
                except Exception as e:
                    # If topic already exists or race condition, just log a warning
                    logger.warning(
                        f"Topic creation for {topic} have failed : {e}"
                    )

        except Exception as e:
            logger.error(f"Error while creating topic {self.topic_name}: {e}")


    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
    try:
        self.producer.flush()
    except Exception as e:
        logger.warning(f"Error while closing producer: {e}")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
