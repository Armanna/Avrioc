import random
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

from src.config import USER_COUNT, ITEM_COUNT, INTERACTION_TYPES


class InteractionGenerator:
    
    def __init__(
        self, 
        user_count: int = USER_COUNT,
        item_count: int = ITEM_COUNT,
        interaction_types: List[str] = None
    ):
        """
        Initialize the interaction generator.
        
        Args:
            user_count: Number of unique users to simulate
            item_count: Number of unique items for interactions
            interaction_types: Possible interaction types
        """
        self.user_count = user_count
        self.item_count = item_count
        self.interaction_types = interaction_types or INTERACTION_TYPES
        
        self.user_ids = [f"user_{i}" for i in range(1, self.user_count + 1)]
        self.item_ids = [f"item_{i}" for i in range(1, self.item_count + 1)]
        
        self.user_history = {}

    def generate_interaction(self) -> Dict[str, Any]:
        """
        Generate a single user interaction event.
        
        Returns:
            Dict containing interaction data
        """
        user_id = random.choices(
            self.user_ids, 
            weights=[1 + random.random() for _ in range(self.user_count)],
            k=1
        )[0]
        
        item_id = random.choices(
            self.item_ids,
            weights=[1 + random.random() * 5 for _ in range(self.item_count)],
            k=1
        )[0]
        
        
        interaction_weights = {
            'view': 0.65,
            'click': 0.2,
            'add_to_cart': 0.1, 
            'purchase': 0.03,
            'share': 0.02
        }
        interaction_type = random.choices(
            list(interaction_weights.keys()),
            weights=list(interaction_weights.values()),
            k=1
        )[0]
        
        interaction = {
            'user_id': user_id,
            'item_id': item_id,
            'interaction_type': interaction_type,
            'timestamp': datetime.now().isoformat(),
            'session_id': str(uuid.uuid4()),
            'value': round(random.random() * 10, 2),  
        }
        
        if interaction_type == 'purchase':
            interaction['price'] = round(random.uniform(5, 500), 2)
        
        if interaction_type == 'view':
            interaction['view_duration'] = round(random.uniform(1, 120), 1)  
            
        return interaction
    
    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """
        Generate a batch of interaction events.
        
        Args:
            batch_size: Number of events to generate
            
        Returns:
            List of interaction events
        """
        return [self.generate_interaction() for _ in range(batch_size)]


class DataGenerationController:
    
    def __init__(self, generator: InteractionGenerator):
        """
        Initialize the data generation controller.
        
        Args:
            generator: InteractionGenerator instance
        """
        self.generator = generator
        self.running = False
        self.generation_rate = 1.0  
        
    def set_generation_rate(self, events_per_second: float):
        """
        Set the generation rate.
        
        Args:
            events_per_second: Number of events to generate per second
        """
        self.generation_rate = max(0.1, events_per_second)  
        
    def generate_continuous(self, callback, batch_size: int = 1):
        """
        Continuously generate data at the specified rate.
        
        Args:
            callback: Function to call with generated data
            batch_size: Number of events to generate in each batch
        """
        self.running = True
        
        try:
            while self.running:
                sleep_time = batch_size / self.generation_rate
                
                events = self.generator.generate_batch(batch_size)
                
                callback(events)
                
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            self.running = False
            print("Data generation stopped.")
    
    def stop(self):
        self.running = False 