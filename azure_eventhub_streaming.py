"""
Azure Event Hub Real-Time Streaming Pipeline
Author: Sumanth Battu
Description: Real-time airline operations data streaming
             using Azure Event Hub and Stream Analytics
"""

from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import (
    BlobCheckpointStore
)
import asyncio
import json
import logging
from datetime import datetime
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirlineEventHubProducer:
    """
    Azure Event Hub producer for real-time
    airline operations event streaming.
    """

    def __init__(self, connection_str: str,
                 eventhub_name: str):
        self.connection_str = connection_str
        self.eventhub_name = eventhub_name
        logger.info(
            f"Event Hub Producer initialized: {eventhub_name}"
        )

    def generate_flight_event(self) -> dict:
        """Generate simulated flight operations event"""
        return {
            "event_id": f"EVT-{random.randint(10000, 99999)}",
            "timestamp": datetime.utcnow().isoformat(),
            "flight_number": f"AA{random.randint(100, 9999)}",
            "origin": random.choice([
                "DFW", "LAX", "JFK", "ORD", "MIA"
            ]),
            "destination": random.choice([
                "DFW", "LAX", "JFK", "ORD", "MIA"
            ]),
            "status": random.choice([
                "ON_TIME", "DELAYED", "DEPARTED", "ARRIVED"
            ]),
            "departure_delay": random.randint(-10, 120),
            "gate": f"{'ABCDE'[random.randint(0,4)]}"
                    f"{random.randint(1, 50)}",
            "aircraft_type": random.choice([
                "B737", "B777", "A320", "A321"
            ])
        }

    def send_batch_events(self,
                          num_events: int = 100) -> dict:
        """
        Send batch of flight events to Event Hub
        with partitioned consumer groups
        """
        logger.info(
            f"Sending {num_events} events to Event Hub..."
        )
        results = {
            "sent": 0,
            "failed": 0,
            "start_time": datetime.utcnow().isoformat()
        }

        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_str,
            eventhub_name=self.eventhub_name
        )

        try:
            with producer:
                event_batch = producer.create_batch()
                for i in range(num_events):
                    event = self.generate_flight_event()
                    try:
                        event_batch.add(
                            EventData(json.dumps(event))
                        )
                        results["sent"] += 1
                    except ValueError:
                        producer.send_batch(event_batch)
                        event_batch = producer.create_batch()
                        event_batch.add(
                            EventData(json.dumps(event))
                        )
                        results["sent"] += 1

                producer.send_batch(event_batch)

        except Exception as e:
            logger.error(f"Failed to send events: {str(e)}")
            results["failed"] += 1

        results["end_time"] = datetime.utcnow().isoformat()
        logger.info(
            f"Sent {results['sent']} events successfully"
        )
        return results


class AirlineEventHubConsumer:
    """
    Azure Event Hub consumer with checkpoint store
    for exactly-once processing semantics.
    """

    def __init__(self, connection_str: str,
                 eventhub_name: str,
                 storage_connection_str: str,
                 container_name: str):
        self.connection_str = connection_str
        self.eventhub_name = eventhub_name
        self.checkpoint_store = BlobCheckpointStore\
            .from_connection_string(
                storage_connection_str,
                container_name
            )
        self.processed_events = []
        logger.info(
            f"Event Hub Consumer initialized: {eventhub_name}"
        )

    async def process_event(self, partition_context,
                            event: EventData) -> None:
        """
