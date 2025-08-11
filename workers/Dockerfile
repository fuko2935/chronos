# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Install ffmpeg, which is a system-level dependency for ffmpeg-python
RUN apt-get update && apt-get install -y ffmpeg

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the requirements file and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# The command to run the Celery worker.
# It listens for tasks on the queue defined in the celery_app configuration.
# The worker is named 'worker1@%h' where %h is the hostname.
CMD ["celery", "-A", "celery_app", "worker", "--loglevel=info", "-n", "worker1@%h"]
