import os
import boto3
import redis
import json
from botocore.client import Config
from botocore.exceptions import ClientError
from flask import Flask, request, jsonify
from celery import Celery
from datetime import datetime

# --- App Initialization ---
app = Flask(__name__)

# --- Configuration ---
# Load configuration from environment variables
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "videos")
S3_REGION = os.environ.get("S3_REGION", "us-east-1") # MinIO doesn't use regions, but boto3 requires it.

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_CHANNEL = "video_events"

# Set a file size limit (2GB as per PRD)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024

# --- Service Clients ---
try:
    # S3 Client for MinIO or AWS S3
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=S3_REGION
    )
    # Ensure the bucket exists
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket '{S3_BUCKET_NAME}' not found. Creating it.")
            s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
        else:
            raise
except Exception as e:
    print(f"Error initializing S3 client: {e}")
    s3_client = None

try:
    # Redis Client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
    print("Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    print(f"Error connecting to Redis: {e}")
    redis_client = None

# Celery Client
# This instance is only used for sending tasks. The worker definition is in the 'workers' directory.
celery_app = Celery(
    'backend_tasks',
    broker=f"redis://{REDIS_HOST}:{REDIS_PORT}/0",
    backend=f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
)

# --- Multipart Upload API Endpoints ---

@app.route('/upload/initialize', methods=['POST'])
def initialize_upload():
    """
    Starts a multipart upload and returns an upload ID.
    The client must provide the object key (filename).
    """
    if not s3_client:
        return jsonify({"error": "S3 client not initialized"}), 503

    data = request.get_json()
    object_key = data.get('objectKey')
    if not object_key:
        return jsonify({"error": "objectKey is required"}), 400

    try:
        response = s3_client.create_multipart_upload(
            Bucket=S3_BUCKET_NAME,
            Key=object_key
        )
        return jsonify({"uploadId": response['UploadId']})
    except ClientError as e:
        print(f"Error initializing multipart upload: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/upload/part', methods=['POST'])
def get_presigned_url_for_part():
    """
    Generates a pre-signed URL for uploading a single part.
    Client must provide objectKey, uploadId, and partNumber.
    """
    if not s3_client:
        return jsonify({"error": "S3 client not initialized"}), 503

    data = request.get_json()
    object_key = data.get('objectKey')
    upload_id = data.get('uploadId')
    part_number = data.get('partNumber')

    if not all([object_key, upload_id, part_number]):
        return jsonify({"error": "objectKey, uploadId, and partNumber are required"}), 400

    try:
        url = s3_client.generate_presigned_url(
            'upload_part',
            Params={
                'Bucket': S3_BUCKET_NAME,
                'Key': object_key,
                'UploadId': upload_id,
                'PartNumber': part_number
            },
            ExpiresIn=3600  # 1 hour
        )
        return jsonify({"url": url})
    except ClientError as e:
        print(f"Error generating pre-signed URL: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/upload/complete', methods=['POST'])
def complete_upload():
    """
    Completes a multipart upload.
    Client must provide objectKey, uploadId, and a list of parts with their ETags.
    """
    if not s3_client or not redis_client:
        return jsonify({"error": "A required service (S3 or Redis) is not initialized"}), 503

    data = request.get_json()
    object_key = data.get('objectKey')
    upload_id = data.get('uploadId')
    parts = data.get('parts') # e.g., [{"PartNumber": 1, "ETag": "abc..."}, ...]

    if not all([object_key, upload_id, parts]):
        return jsonify({"error": "objectKey, uploadId, and parts are required"}), 400

    try:
        # TODO: Add virus scan logic here before completing the upload.
        # This could involve a separate service that scans the object in S3.
        # For now, we proceed directly to completion.

        s3_client.complete_multipart_upload(
            Bucket=S3_BUCKET_NAME,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        # Dispatch the 'video.uploaded' event to the task queue
        event_message = {
            "event_name": "video.uploaded",
            "object_key": object_key,
            "bucket": S3_BUCKET_NAME,
            "uploaded_at": datetime.utcnow().isoformat()
        }
        redis_client.publish(REDIS_CHANNEL, json.dumps(event_message))

        # Trigger the video processing task
        task = celery_app.send_task(
            'tasks.process_video',
            args=[object_key, S3_BUCKET_NAME]
        )

        return jsonify({
            "status": "upload complete, processing started",
            "objectKey": object_key,
            "taskId": task.id
        })

    except ClientError as e:
        print(f"Error completing multipart upload: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/upload/abort', methods=['POST'])
def abort_upload():
    """
    Aborts a multipart upload, deleting any uploaded parts.
    """
    if not s3_client:
        return jsonify({"error": "S3 client not initialized"}), 503

    data = request.get_json()
    object_key = data.get('objectKey')
    upload_id = data.get('uploadId')

    if not all([object_key, upload_id]):
        return jsonify({"error": "objectKey and uploadId are required"}), 400

    try:
        s3_client.abort_multipart_upload(
            Bucket=S3_BUCKET_NAME,
            Key=object_key,
            UploadId=upload_id
        )
        return jsonify({"status": "upload aborted"})
    except ClientError as e:
        print(f"Error aborting multipart upload: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/upload/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """
    Retrieves the status of a Celery task.
    """
    task_result = celery_app.AsyncResult(task_id)

    response = {
        'taskId': task_id,
        'state': task_result.state,
        'info': task_result.info,
    }

    return jsonify(response)


@app.route('/health', methods=['GET'])
def health_check():
    """A simple health check endpoint."""
    return jsonify({"status": "ok"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
