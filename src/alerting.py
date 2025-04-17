import time
import threading
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from src.db_manager import MongoDBManager
from src.config import ALERT_THRESHOLDS


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AlertingSystem")


class AlertingSystem:
    """System for monitoring metrics and alerting on threshold violations."""
    
    def __init__(
        self,
        db_manager: MongoDBManager = None,
        thresholds: Dict[str, Any] = None,
        check_interval: int = 30  
    ):
        """
        Initialize the alerting system.
        
        Args:
            db_manager: MongoDB manager instance
            thresholds: Threshold values for alerts
            check_interval: Interval to check metrics in seconds
        """
        self.db_manager = db_manager or MongoDBManager()
        self.thresholds = thresholds or ALERT_THRESHOLDS
        self.check_interval = check_interval
        
        self.running = False
        self.thread = None
        
        self.alerted_items = set()
        self.alert_cooldown = 3600  # 1 hour cooldown between repeat alerts
        self.last_alert_time = {}
        
        self.alert_handlers = [
            self.log_alert
        ]
        
    def check_metrics(self):
        metrics = self.db_manager.get_latest_metrics()
        current_time = time.time()
        
        for metric in metrics:
            metric_type = metric['metric_type']
            metric_value = metric['metric_value']
            dimensions = metric.get('dimensions', {})
            
            if metric_type == 'max_interactions_per_item':
                threshold = self.thresholds.get('high_interaction_count')
                if threshold and metric_value > threshold:
                    item_id = dimensions.get('item_id')
                    if item_id:
                        alert_key = f"high_item_{item_id}"
                        last_alert = self.last_alert_time.get(alert_key, 0)
                        
                        if current_time - last_alert > self.alert_cooldown:
                            self.trigger_alert(
                                alert_type="HIGH_ITEM_INTERACTION",
                                message=f"Item {item_id} has {metric_value} interactions, exceeding threshold of {threshold}",
                                details={
                                    'item_id': item_id,
                                    'interaction_count': metric_value,
                                    'threshold': threshold
                                }
                            )
                            self.last_alert_time[alert_key] = current_time
            
            if metric_type == 'avg_interactions_per_user':
                threshold = self.thresholds.get('low_user_engagement')
                if threshold and metric_value < threshold:
                    alert_key = "low_user_engagement"
                    last_alert = self.last_alert_time.get(alert_key, 0)
                    
                    if current_time - last_alert > self.alert_cooldown:
                        self.trigger_alert(
                            alert_type="LOW_USER_ENGAGEMENT",
                            message=f"Average interactions per user is {metric_value}, below threshold of {threshold}",
                            details={
                                'avg_interactions': metric_value,
                                'threshold': threshold,
                                'time_window': dimensions.get('time_window', 'unknown')
                            }
                        )
                        self.last_alert_time[alert_key] = current_time
    
    def trigger_alert(self, alert_type: str, message: str, details: Dict[str, Any] = None):
        """
        Trigger an alert.
        
        Args:
            alert_type: Type of alert
            message: Alert message
            details: Additional alert details
        """
        alert = {
            'type': alert_type,
            'message': message,
            'details': details or {},
            'timestamp': datetime.now().isoformat()
        }
        
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Error in alert handler {handler.__name__}: {e}")
    
    def log_alert(self, alert: Dict[str, Any]):
        """
        Log an alert.
        
        Args:
            alert: Alert information
        """
        logger.warning(f"ALERT - {alert['type']}: {alert['message']}")
        
    def send_email_alert(self, alert: Dict[str, Any], recipients: List[str] = None):
        """
        Send an email alert.
        
        Args:
            alert: Alert information
            recipients: Email recipients
        """
        
        if not recipients:
            recipients = ['armanmalkhasyan1994@gmail.com']
            
        msg = MIMEMultipart()
        msg['Subject'] = f"ALERT: {alert['type']}"
        msg['From'] = 'alerts@yoursystem.com'
        msg['To'] = ', '.join(recipients)
        
        body = f"""
        Alert Type: {alert['type']}
        Message: {alert['message']}
        Time: {alert['timestamp']}
        
        Details:
        {alert['details']}
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Placeholder for actual email sending
        logger.info(f"Would send email alert to {recipients}: {alert['message']}")
        
        """
        try:
            server = smtplib.SMTP('smtp.yourserver.com', 587)
            server.starttls()
            server.login('your_email@example.com', 'your_password')
            server.send_message(msg)
            server.quit()
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
        """
        
    def monitor_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                self.check_metrics()
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                
            time.sleep(self.check_interval)
            
    def start(self):
        if self.running:
            logger.info("Alerting system is already running.")
            return
            
        # Connect to MongoDB
        if not self.db_manager.connect():
            logger.error("Failed to connect to MongoDB. Cannot start alerting system.")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self.monitor_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Started alerting system. Checking metrics every {self.check_interval} seconds.")
        
    def stop(self):
        if not self.running:
            logger.info("Alerting system is not running.")
            return
            
        self.running = False
        if self.db_manager:
            self.db_manager.close()
            
        logger.info("Stopped alerting system.")


def run_alerting_system():
    alerting = AlertingSystem()
    try:
        alerting.start()

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down alerting system...")
    finally:
        alerting.stop()


if __name__ == "__main__":
    run_alerting_system() 