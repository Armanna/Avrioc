import argparse
import time
import threading
import logging
import sys

from src.data_generator import InteractionGenerator
from src.kafka_producer import StreamingController, InteractionProducer
from src.kafka_consumer import InteractionConsumer
from src.db_manager import MongoDBManager
from src.dashboard import InteractionDashboard
from src.alerting import AlertingSystem


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("main")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="User Interaction Analytics Pipeline"
    )
    
    parser.add_argument(
        "--components", 
        nargs="+", 
        choices=["producer", "consumer", "dashboard", "alerting", "all"],
        default=["all"],
        help="Components to run (default: all)"
    )
    
    parser.add_argument(
        "--generation-rate", 
        type=float, 
        default=100.0,
        help="Rate of data generation in events per second (default: 100.0)"
    )
    
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=20,
        help="Batch size for data generation (default: 20)"
    )
    
    return parser.parse_args()


def run_producer(generation_rate, batch_size):
    logger.info(f"Starting producer with rate={generation_rate} events/sec, batch_size={batch_size}")
    
    controller = StreamingController(
        generation_rate=generation_rate,
        batch_size=batch_size
    )
    
    try:
        controller.start()
        return controller  
    except Exception as e:
        logger.error(f"Error starting producer: {e}")
        return None


def run_consumer():
    logger.info("Starting Kafka consumer")
    
    consumer = InteractionConsumer()
    
    try:
        consumer.start()
        return consumer  
    except Exception as e:
        logger.error(f"Error starting consumer: {e}")
        return None


def run_dashboard():
    logger.info("Starting dashboard")
    
    dashboard = InteractionDashboard()
    
    dashboard_thread = threading.Thread(target=dashboard.run)
    dashboard_thread.daemon = True
    dashboard_thread.start()
    
    return dashboard_thread


def run_alerting():
    logger.info("Starting alerting system")
    
    alerting = AlertingSystem()
    
    try:
        alerting.start()
        return alerting  
    except Exception as e:
        logger.error(f"Error starting alerting system: {e}")
        return None


def main():
    args = parse_args()
    
    components_to_run = args.components
    run_all = "all" in components_to_run
    
    producer = None
    consumer = None
    dashboard_thread = None
    alerting = None
    
    try:
        if run_all or "producer" in components_to_run:
            producer = run_producer(args.generation_rate, args.batch_size)
        
        if run_all or "consumer" in components_to_run:
            consumer = run_consumer()
        
        if run_all or "dashboard" in components_to_run:
            dashboard_thread = run_dashboard()
        
        if run_all or "alerting" in components_to_run:
            alerting = run_alerting()
        
        logger.info("All requested components started. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if alerting:
            alerting.stop()
            
        if consumer:
            consumer.stop()
            
        if producer:
            producer.stop()
            
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main() 