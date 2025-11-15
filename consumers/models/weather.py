"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        try:
            value = message.value()

            # Extract & update weather info
            self.temperature = value.get("temperature", self.temperature)
            self.status = value.get("status", self.status)

            logger.debug(
                f"Updated weather: temp={self.temperature}, status={self.status}"
            )

        except Exception as e:
            logger.error(f"Error processing weather message: {e}, raw={message.value()}")
