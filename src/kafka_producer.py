import json
import time
from typing import Dict, Any, List, Callable
import threading

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.data_generator import InteractionGenerator, DataGenerationController


class InteractionProducer:
    
    def __init__(
        self, 
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC
    ):
        """
        Initialize the Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic for publishing events
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.connected = False
        
    def connect(self) -> bool:
        """
        Connect to Kafka.
        
        Returns:
            bool: True if connection successful
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas
                retries=3,    # Retry on failure
                linger_ms=5   # Batch messages to improve throughput
            )
            self.connected = True
            print(f"Connected to Kafka at {self.bootstrap_servers}")
            return True
        except KafkaError as e:
            print(f"Failed to connect to Kafka: {e}")
            self.connected = False
            return False
            
    def send_event(self, event: Dict[str, Any], callback: Callable = None):
        """
        Send a single event to Kafka.
        
        Args:
            event: User interaction event
            callback: Optional callback function after sending
        """
        if not self.connected:
            if not self.connect():
                print("Not connected to Kafka. Cannot send event.")
                return
            
        def on_send_success(record_metadata):
            if callback:
                callback(record_metadata)
        
        def on_send_error(exc):
            print(f"Failed to send message: {exc}")
            
        self.producer.send(
            self.topic, 
            event
        ).add_callback(on_send_success).add_errback(on_send_error)
            
    def send_batch(self, events: List[Dict[str, Any]]):
        """
        Send a batch of events to Kafka.
        
        Args:
            events: List of user interaction events
        """
        if not self.connected:
            if not self.connect():
                print("Not connected to Kafka. Cannot send batch.")
                return
                
        for event in events:
            self.send_event(event)
            
        self.producer.flush()
            
    def close(self):
        """Close the Kafka producer."""
        if self.producer:
            self.producer.flush()  
            self.producer.close()
            self.connected = False
            print("Kafka producer closed.")


class StreamingController:
    """Controls the streaming of generated data to Kafka."""
    
    def __init__(
        self, 
        generator: InteractionGenerator = None,
        producer: InteractionProducer = None,
        generation_rate: float = 10.0,
        batch_size: int = 10
    ):
        """
        Initialize the streaming controller.
        
        Args:
            generator: InteractionGenerator instance
            producer: InteractionProducer instance
            generation_rate: Events per second to generate
            batch_size: Number of events in each batch
        """
        self.generator = generator or InteractionGenerator()
        self.producer = producer or InteractionProducer()
        self.generation_controller = DataGenerationController(self.generator)
        self.generation_controller.set_generation_rate(generation_rate)
        self.batch_size = batch_size
        self.thread = None
        self.running = False
        
    def start(self):
        """Start the streaming process in a separate thread."""
        if self.running:
            print("Streaming is already running.")
            return
            
        self.running = True
        self.thread = threading.Thread(
            target=self.generation_controller.generate_continuous,
            args=(self.producer.send_batch, self.batch_size)
        )
        self.thread.daemon = True
        self.thread.start()
        print(f"Started streaming at rate of {self.generation_controller.generation_rate} events/sec")
        
    def stop(self):
        """Stop the streaming process."""
        if not self.running:
            print("Streaming is not running.")
            return
            
        self.generation_controller.stop()
        self.producer.close()
        self.running = False
        print("Stopped streaming.")
        
    def set_rate(self, events_per_second: float):
        """
        Set the event generation rate.
        
        Args:
            events_per_second: Number of events to generate per second
        """
        self.generation_controller.set_generation_rate(events_per_second)
        print(f"Updated generation rate to {events_per_second} events/sec")


def demo_producer():
    controller = StreamingController(
        generation_rate=100.0,  
        batch_size=20          
    )
    
    try:
        controller.start()
        
        time.sleep(5)
        controller.set_rate(200.0)  
        time.sleep(5)
        controller.set_rate(50.0)   
        time.sleep(5)
        
    finally:

        controller.stop()


if __name__ == "__main__":
    demo_producer() 