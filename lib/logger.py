import os
import logging
import logging.handlers
import threading
import queue

# Set log level from environment variable
log_level = os.getenv('YB_DRIVER_LOG_LEVEL', 'WARNING').upper()

# Mapping of string level names to logging constants
level_mapping = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

# Default to 'WARNING' level if the environment variable is invalid
log_level = level_mapping.get(log_level, logging.WARNING)

# Create a queue for logging
log_queue = queue.Queue()

# Create a handler that writes log messages to the queue
queue_handler = logging.handlers.QueueHandler(log_queue)

# Create a stream handler for console output
console_handler = logging.StreamHandler()

# Set up logging with queue handler and console handler
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]',
    handlers=[queue_handler]  # Use queue handler for thread safety
)

# Create a logger instance
logger = logging.getLogger(__name__)

# Function that listens for logs from the queue and writes them
def log_listener():
    while True:
        try:
            # Get log record from the queue
            record = log_queue.get()
            if record is None:
                break  # Stop listener
            # Log the record to the console
            console_handler.emit(record)
        except Exception as e:
            print(f"Error while handling log: {e}")

# Start listener thread
listener_thread = threading.Thread(target=log_listener, daemon=True)
listener_thread.start()
