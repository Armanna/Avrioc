from typing import Dict, Any, List, Optional
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta

from src.config import (
    MONGODB_CONNECTION_STRING,
    MONGODB_DATABASE,
    MONGODB_COLLECTION_RAW,
    MONGODB_COLLECTION_AGGREGATED
)


class MongoDBManager:
    
    def __init__(
        self,
        connection_string: str = MONGODB_CONNECTION_STRING,
        db_name: str = MONGODB_DATABASE,
        raw_collection: str = MONGODB_COLLECTION_RAW,
        agg_collection: str = MONGODB_COLLECTION_AGGREGATED
    ):
        """
        Initialize the MongoDB manager.
        
        Args:
            connection_string: MongoDB connection string
            db_name: Database name
            raw_collection: Collection for raw interaction data
            agg_collection: Collection for aggregated metrics
        """
        self.connection_string = connection_string
        self.db_name = db_name
        self.raw_collection_name = raw_collection
        self.agg_collection_name = agg_collection
        
        self.client = None
        self.db = None
        self.raw_collection = None
        self.agg_collection = None
        
    def connect(self) -> bool:
        """
        Connect to MongoDB.
        
        Returns:
            bool: True if connection successful
        """
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.db_name]
            self.raw_collection = self.db[self.raw_collection_name]
            self.agg_collection = self.db[self.agg_collection_name]
            
            # Create indexes for better performance
            self.raw_collection.create_index([("user_id", pymongo.ASCENDING)])
            self.raw_collection.create_index([("item_id", pymongo.ASCENDING)])
            self.raw_collection.create_index([("timestamp", pymongo.DESCENDING)])
            
            self.agg_collection.create_index([("metric_type", pymongo.ASCENDING)])
            self.agg_collection.create_index([("timestamp", pymongo.DESCENDING)])
            
            print(f"Connected to MongoDB at {self.connection_string}")
            return True
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")
            return False
            
    def insert_raw_interaction(self, interaction: Dict[str, Any]) -> str:
        """
        Insert a raw interaction event into MongoDB.
        
        Args:
            interaction: User interaction event
            
        Returns:
            str: ID of the inserted document
        """
        if not self.client:
            self.connect()
            
        result = self.raw_collection.insert_one(interaction)
        return str(result.inserted_id)
        
    def insert_raw_interactions_batch(self, interactions: List[Dict[str, Any]]) -> List[str]:
        """
        Insert a batch of raw interaction events into MongoDB.
        
        Args:
            interactions: List of user interaction events
            
        Returns:
            List[str]: IDs of the inserted documents
        """
        if not self.client:
            self.connect()
            
        result = self.raw_collection.insert_many(interactions)
        return [str(id) for id in result.inserted_ids]
        
    def store_aggregation(
        self, 
        metric_type: str,
        metric_value: Any,
        dimensions: Dict[str, Any] = None,
        timestamp: str = None
    ) -> str:
        """
        Store an aggregation metric in MongoDB.
        
        Args:
            metric_type: Type of metric (e.g., 'avg_interactions_per_user')
            metric_value: Value of the metric
            dimensions: Additional dimensions (e.g., {'interaction_type': 'click'})
            timestamp: Timestamp for the metric, defaults to current time
            
        Returns:
            str: ID of the inserted document
        """
        if not self.client:
            self.connect()
            
        document = {
            'metric_type': metric_type,
            'metric_value': metric_value,
            'timestamp': timestamp or datetime.now().isoformat(),
        }
        
        if dimensions:
            document['dimensions'] = dimensions
            
        result = self.agg_collection.insert_one(document)
        return str(result.inserted_id)
    
    def update_metric(
        self,
        metric_type: str,
        metric_value: Any,
        dimensions: Dict[str, Any] = None
    ) -> bool:
        """
        Update or insert an aggregation metric.
        
        Args:
            metric_type: Type of metric
            metric_value: New value of the metric
            dimensions: Additional dimensions
            
        Returns:
            bool: True if update successful
        """
        if not self.client:
            self.connect()
            
        query = {'metric_type': metric_type}
        if dimensions:
            for key, value in dimensions.items():
                query[f'dimensions.{key}'] = value
                
        update = {
            '$set': {
                'metric_value': metric_value,
                'timestamp': datetime.now().isoformat()
            }
        }
        
        if dimensions:
            update['$set']['dimensions'] = dimensions
            
        result = self.agg_collection.update_one(
            query,
            update,
            upsert=True
        )
        
        return result.acknowledged
    
    def get_latest_metrics(self, metric_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get the latest metrics from MongoDB.
        
        Args:
            metric_type: Optional filter for specific metric type
            
        Returns:
            List of metric documents
        """
        if not self.client:
            self.connect()
            
        query = {}
        if metric_type:
            query['metric_type'] = metric_type
            
        pipeline = [
            {'$match': query},
            {'$sort': {'timestamp': -1}},
            {'$group': {
                '_id': {
                    'metric_type': '$metric_type',
                    'dimensions': '$dimensions'
                },
                'latest_doc': {'$first': '$$ROOT'}
            }},
            {'$replaceRoot': {'newRoot': '$latest_doc'}}
        ]
        
        return list(self.agg_collection.aggregate(pipeline))
    
    def get_metric_history(
        self, 
        metric_type: str,
        dimensions: Dict[str, Any] = None,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get historical data for a specific metric.
        
        Args:
            metric_type: Type of metric
            dimensions: Additional dimensions to filter by
            hours: Number of hours of history to retrieve
            
        Returns:
            List of metric documents ordered by timestamp
        """
        if not self.client:
            self.connect()
            
        query = {'metric_type': metric_type}
        
        start_time = (datetime.now() - timedelta(hours=hours)).isoformat()
        query['timestamp'] = {'$gte': start_time}
        
        if dimensions:
            for key, value in dimensions.items():
                query[f'dimensions.{key}'] = value
                
        return list(self.agg_collection.find(
            query,
            sort=[('timestamp', pymongo.ASCENDING)]
        ))
        
    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            print("MongoDB connection closed.")


def demo_db_usage():
    db_manager = MongoDBManager()
    
    if db_manager.connect():
        db_manager.store_aggregation(
            metric_type='avg_interactions_per_user',
            metric_value=12.5,
            dimensions={'time_window': '1h'}
        )
        
        db_manager.update_metric(
            metric_type='max_interactions_per_item',
            metric_value=358,
            dimensions={'item_id': 'item_42', 'time_window': '1h'}
        )
        
        metrics = db_manager.get_latest_metrics()
        print(f"Retrieved {len(metrics)} metrics")
        
        db_manager.close()


if __name__ == "__main__":
    demo_db_usage() 