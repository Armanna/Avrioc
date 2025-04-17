# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'user-interactions'
KAFKA_CONSUMER_GROUP = 'interaction-processor'

# MongoDB configuration
MONGODB_CONNECTION_STRING = 'mongodb://localhost:27017/'
MONGODB_DATABASE = 'user_interactions_db'
MONGODB_COLLECTION_RAW = 'raw_interactions'
MONGODB_COLLECTION_AGGREGATED = 'aggregated_metrics'

# Data generation settings
USER_COUNT = 1000
ITEM_COUNT = 500
INTERACTION_TYPES = ['click', 'view', 'purchase', 'add_to_cart', 'share']
DEFAULT_GENERATION_RATE = 100  

# Dashboard settings
DASHBOARD_HOST = '0.0.0.0'
DASHBOARD_PORT = 8050
DASHBOARD_REFRESH_INTERVAL = 5  

# Alerting thresholds (bonus)
ALERT_THRESHOLDS = {
    'high_interaction_count': 1000,  # Alert if item gets more than 1000 interactions
    'low_user_engagement': 5,        # Alert if average user engagement falls below 5
} 