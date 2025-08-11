import os
from datetime import timedelta
from celery import Celery

# --- Configuration ---
# Load configuration from environment variables
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Construct the Redis URL for Celery
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

# --- Celery App Initialization ---
# The first argument is the name of the current module.
# The 'broker' argument specifies the URL of the message broker (Redis).
# The 'backend' argument specifies the URL of the result backend (also Redis).
celery_app = Celery(
    'video_processing_workers',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['tasks'] # List of modules to import when the worker starts.
)

# --- Celery Configuration ---
celery_app.conf.update(
    task_track_started=True,
    # Using json as the serializer is a good default for interoperability.
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    # Set a timeout for tasks to prevent them from running indefinitely.
    task_time_limit=3600, # 1 hour
    task_soft_time_limit=3500, # 58 minutes and 20 seconds
    # Ensure results are stored long enough to be retrieved.
    result_expires=timedelta(days=1),
)

if __name__ == '__main__':
    # This allows you to start the worker directly for testing.
    # Command: celery -A celery_app worker --loglevel=info
    celery_app.start()
