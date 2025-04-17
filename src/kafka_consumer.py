import json
import threading
from typing import Dict, Any, List, Optional, Callable
from collections import defaultdict
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP
)
from src.db_manager import MongoDBManager


class InteractionAggregator:
    
    def __init__(self, window_size: int = 60):
        """
        Initialize the aggregator.
        
        Args:
            window_size: Window size in seconds for real-time aggregations
        """
        self.window_size = window_size
        
        self.user_interactions = defaultdict(list)  
        self.item_interactions = defaultdict(list)  
        self.interaction_counts = defaultdict(int)  
        
        self.last_cleanup = time.time()
        
    def add_interaction(self, interaction: Dict[str, Any]):
        """
        Add an interaction to the aggregator.
        
        Args:
            interaction: User interaction event
        """
        interaction['_added'] = time.time()
        
        self.user_interactions[interaction['user_id']].append(interaction)
        
        self.item_interactions[interaction['item_id']].append(interaction)
        
        self.interaction_counts[interaction['interaction_type']] += 1
        
        current_time = time.time()
        if current_time - self.last_cleanup > self.window_size / 2:
            self._cleanup_old_interactions()
            self.last_cleanup = current_time
            
    def _cleanup_old_interactions(self):
        """Remove interactions that are outside the current time window."""
        cutoff_time = time.time() - self.window_size
        
        for user_id in list(self.user_interactions.keys()):
            self.user_interactions[user_id] = [
                interaction for interaction in self.user_interactions[user_id]
                if interaction['_added'] >= cutoff_time
            ]
            if not self.user_interactions[user_id]:
                del self.user_interactions[user_id]
                
        for item_id in list(self.item_interactions.keys()):
            self.item_interactions[item_id] = [
                interaction for interaction in self.item_interactions[item_id]
                if interaction['_added'] >= cutoff_time
            ]
            if not self.item_interactions[item_id]:
                del self.item_interactions[item_id]
                
        self.interaction_counts.clear()
        for user_interactions in self.user_interactions.values():
            for interaction in user_interactions:
                self.interaction_counts[interaction['interaction_type']] += 1
                
    def get_avg_interactions_per_user(self) -> float:
        """
        Calculate the average number of interactions per user.
        
        Returns:
            float: Average interactions per user
        """
        if not self.user_interactions:
            return 0.0
            
        total_interactions = sum(len(interactions) for interactions in self.user_interactions.values())
        return total_interactions / len(self.user_interactions)
        
    def get_max_interactions_per_item(self) -> Dict[str, Any]:
        """
        Find the item with the maximum interactions.
        
        Returns:
            Dict with item_id and count
        """
        if not self.item_interactions:
            return {'item_id': None, 'count': 0}
            
        max_item = max(
            self.item_interactions.items(),
            key=lambda x: len(x[1]),
            default=(None, [])
        )
        
        return {
            'item_id': max_item[0],
            'count': len(max_item[1])
        }
        
    def get_min_interactions_per_item(self) -> Dict[str, Any]:
        """
        Find the item with the minimum interactions.
        
        Returns:
            Dict with item_id and count
        """
        if not self.item_interactions:
            return {'item_id': None, 'count': 0}
            
        min_item = min(
            self.item_interactions.items(),
            key=lambda x: len(x[1]),
            default=(None, [])
        )
        
        return {
            'item_id': min_item[0],
            'count': len(min_item[1])
        }
        
    def get_interaction_distribution(self) -> Dict[str, int]:
        """
        Get the distribution of interaction types.
        
        Returns:
            Dict mapping interaction types to counts
        """
        return dict(self.interaction_counts)
        
    def get_all_metrics(self) -> Dict[str, Any]:
        """
        Get all calculated metrics.
        
        Returns:
            Dict containing all metrics
        """
        return {
            'avg_interactions_per_user': self.get_avg_interactions_per_user(),
            'max_interactions_per_item': self.get_max_interactions_per_item(),
            'min_interactions_per_item': self.get_min_interactions_per_item(),
            'interaction_distribution': self.get_interaction_distribution(),
            'active_users_count': len(self.user_interactions),
            'active_items_count': len(self.item_interactions)
        }


class InteractionConsumer:
    """Consumes user interaction events from Kafka."""
    
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = KAFKA_CONSUMER_GROUP,
        db_manager: Optional[MongoDBManager] = None,
        aggregation_window: int = 60  
    ):
        """
        Initialize the Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            db_manager: MongoDB manager instance
            aggregation_window: Window size for aggregations (seconds)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.db_manager = db_manager or MongoDBManager()
        
        self.consumer = None
        self.running = False
        self.thread = None
        
        self.aggregator = InteractionAggregator(window_size=aggregation_window)
        
        self.metrics_update_freq = 5  
        self.last_metrics_update = 0
        
    def connect(self) -> bool:
        """
        Connect to Kafka and MongoDB.
        
        Returns:
            bool: True if connection successful
        """
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            if not self.db_manager.connect():
                print("Failed to connect to MongoDB.")
                return False
                
            print(f"Connected to Kafka at {self.bootstrap_servers}, topic: {self.topic}")
            return True
        except KafkaError as e:
            print(f"Failed to connect to Kafka: {e}")
            return False
            
    def process_message(self, message: Dict[str, Any]):
        """
        Process a single Kafka message.
        
        Args:
            message: User interaction event
        """
        self.db_manager.insert_raw_interaction(message)
        
        self.aggregator.add_interaction(message)
        
    def update_metrics(self):
        metrics = self.aggregator.get_all_metrics()
        
        self.db_manager.update_metric(
            metric_type='avg_interactions_per_user',
            metric_value=metrics['avg_interactions_per_user'],
            dimensions={'time_window': f'{self.aggregator.window_size}s'}
        )
        
        self.db_manager.update_metric(
            metric_type='max_interactions_per_item',
            metric_value=metrics['max_interactions_per_item']['count'],
            dimensions={
                'item_id': metrics['max_interactions_per_item']['item_id'],
                'time_window': f'{self.aggregator.window_size}s'
            }
        )
        
        self.db_manager.update_metric(
            metric_type='min_interactions_per_item',
            metric_value=metrics['min_interactions_per_item']['count'],
            dimensions={
                'item_id': metrics['min_interactions_per_item']['item_id'],
                'time_window': f'{self.aggregator.window_size}s'
            }
        )
        
        for interaction_type, count in metrics['interaction_distribution'].items():
            self.db_manager.update_metric(
                metric_type='interaction_count_by_type',
                metric_value=count,
                dimensions={
                    'interaction_type': interaction_type,
                    'time_window': f'{self.aggregator.window_size}s'
                }
            )
            
        self.db_manager.update_metric(
            metric_type='active_users_count',
            metric_value=metrics['active_users_count'],
            dimensions={'time_window': f'{self.aggregator.window_size}s'}
        )
        
        self.db_manager.update_metric(
            metric_type='active_items_count',
            metric_value=metrics['active_items_count'],
            dimensions={'time_window': f'{self.aggregator.window_size}s'}
        )
        
    def consume(self):
        if not self.consumer:
            if not self.connect():
                print("Failed to connect. Cannot consume messages.")
                return
                
        self.running = True
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                self.process_message(message.value)
                
                current_time = time.time()
                if current_time - self.last_metrics_update > self.metrics_update_freq:
                    self.update_metrics()
                    self.last_metrics_update = current_time
                    
        except Exception as e:
            print(f"Error consuming messages: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                print("Kafka consumer closed.")
                
    def start(self):
        """Start consuming in a separate thread."""
        if self.running:
            print("Consumer is already running.")
            return
            
        self.thread = threading.Thread(target=self.consume)
        self.thread.daemon = True
        self.thread.start()
        print("Started consuming messages.")
        
    def stop(self):
        """Stop consuming messages."""
        if not self.running:
            print("Consumer is not running.")
            return
            
        self.running = False
        if hasattr(self.consumer, 'close'):
            self.consumer.close()
            
        if self.db_manager:
            self.db_manager.close()
            
        print("Stopped consuming messages.")


def run_consumer():
    """Run the Kafka consumer."""
    consumer = InteractionConsumer()
    try:
        consumer.start()

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.stop()


if __name__ == "__main__":
    run_consumer() 