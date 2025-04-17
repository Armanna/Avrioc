# User Interaction Analytics Pipeline

A scalable data pipeline for real-time user interaction analytics. This project implements a complete data flow from data generation to visualization, with real-time aggregations.

## Project Overview

This pipeline simulates, processes, and visualizes user interaction data from multiple platforms. It consists of the following components:

1. **Data Generator**: Simulates user interaction logs at scale
2. **Kafka Producer**: Publishes generated data to Kafka
3. **Kafka Consumer**: Processes data in real-time and performs aggregations
4. **MongoDB Storage**: Stores raw and aggregated data
5. **Dashboard**: Visualizes real-time metrics
6. **Alerting System**: Monitors metrics and triggers alerts when thresholds are exceeded

## Architecture 

```text
┌─────────────┐
│   Data      │
│ Generator   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Kafka     │
│  Producer   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Kafka     │
│   Broker    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Kafka     │
│  Consumer   │
└──────┬──────┘
       │
┌──────┴────────────┐
│                   │
▼                   ▼
┌─────────────┐   ┌─────────────┐
│   MongoDB   │   │ Real-time   │
│  Storage    │◄──┤ Aggregator  │
└──────┬──────┘   └──────┬──────┘
       │                 │
       │                 ▼
       │          ┌─────────────┐
       └─────────►│ Dashboard   │
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │  Alerting   │
                  │   System    │
                  └─────────────┘


```

## Components

### Data Generator

- Simulates user interactions (clicks, views, purchases, etc.)
- Configurable generation rate
- Realistic data patterns with biases toward certain users and items

### Kafka Producer

- Publishes generated data to Kafka
- Handles high throughput
- Batch processing for efficiency

### Kafka Consumer

- Processes messages in real-time
- Performs aggregations on the fly
- Updates MongoDB with raw and aggregated data

### MongoDB Storage

- Stores both raw interaction data and aggregated metrics
- Optimized for both write-heavy workloads and quick lookups
- Efficient indexing for query performance

### Dashboard

- Real-time metrics visualization
- Interactive charts and graphs
- Auto-refreshing data

### Alerting System (Bonus)

- Monitors metrics for threshold violations
- Configurable alert thresholds
- Multiple alert channels (logs, email)

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Kafka
- MongoDB

## Getting Started

### Using Docker Compose (Recommended)

1. Clone the repository:
   ```
   git clone https://github.com/Armanna/Avrioc.git
   cd user-interaction-pipeline
   ```

2. Start the services:
   ```
   docker-compose -f docker/docker-compose.yml up -d
   ```

3. Access the dashboard:
   ```
   http://localhost:8050
   ```

### Manual Installation

1. Clone the repository:
   ```
   git clone https://github.com/Armanna/Avrioc.git
   cd user-interaction-pipeline
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Start Kafka and MongoDB services separately

4. Run the application:
   ```
   python main.py
   ```

## Configuration

Configuration settings are centralized in `src/config.py`. Important settings include:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string
- `MONGODB_CONNECTION_STRING`: MongoDB connection string
- `DEFAULT_GENERATION_RATE`: Default events per second
- `DASHBOARD_REFRESH_INTERVAL`: Dashboard refresh interval
- `ALERT_THRESHOLDS`: Threshold values for alerts

## Usage

### Running All Components

```
python main.py
```

### Running Specific Components

```
python main.py --components producer consumer dashboard
```


### Changing Data Generation Rate

```
python main.py --generation-rate 200 --batch-size 50
```

## Design Choices

### MongoDB as the NoSQL Database

MongoDB was selected for this project for several reasons:

1. **Schema Flexibility**: MongoDB's document model allows for storing varied interaction data without a rigid schema.
2. **Scalability**: Built-in sharding and replication for horizontal scaling.
3. **Performance**: High write throughput which is essential for storing real-time interaction data.
4. **Aggregation Framework**: Powerful built-in aggregation capabilities.
5. **Ease of Integration**: Excellent Python driver and good compatibility with Kafka.

### Real-time Aggregation Approach

For real-time aggregation, we use a hybrid approach:

1. **In-memory Processing**: Maintains a sliding window of recent data for immediate calculations.
2. **Persistent Aggregations**: Regularly persists aggregated metrics to MongoDB.
3. **Incremental Updates**: Efficiently updates metrics as new data arrives.

## Project Structure

```text
user-interaction-pipeline/
│
├── src/
│ ├── init.py
│ ├── config.py # Configuration settings
│ ├── data_generator.py # Data generation module
│ ├── kafka_producer.py # Kafka producer implementation
│ ├── kafka_consumer.py # Kafka consumer with aggregations
│ ├── db_manager.py # NoSQL database manager
│ ├── dashboard.py # Dashboard implementation
│ └── alerting.py # Alerting system (bonus)
│
├── docker/
│ ├── docker-compose.yml # Docker services setup
│ └── Dockerfile # Application container
│
├── tests/
│ ├── init.py
│ └── test_.py # Test files
│
├── main.py # Application entry point
├── requirements.txt # Project dependencies
└── README.md # Project documentation



## Future Improvements

- Adding advanced unit and integration tests
- Implementing more sophisticated alerting mechanisms
- Adding user authentication for the dashboard
- Supporting distributed processing with Kafka Streams or Spark Streaming
- Implementing data retention policies for efficient storage

## Best practices and easy ways to implement

- Using confluent package
- Connecting to Confluent Control Center
- Creating all the necessary connections (source and sink)
- Creating KSQLDB for stream processing
- Managing RBAC


