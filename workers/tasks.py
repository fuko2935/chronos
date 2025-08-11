import os
import time
import boto3
import ffmpeg
from botocore.client import Config
from botocore.exceptions import ClientError
from celery import chain
from celery_app import celery_app

# --- Configuration ---
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

# --- S3 Client ---
# It's good practice to initialize clients once per worker process if possible.
try:
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=S3_REGION
    )
    print("S3 client initialized successfully for worker.")
except Exception as e:
    print(f"Error initializing S3 client for worker: {e}")
    s3_client = None

# --- Constants ---
VIDEO_SEGMENT_THRESHOLD_SECONDS = 10 * 60  # 10 minutes
VIDEO_SEGMENT_DURATION_SECONDS = 10 * 60   # Each segment will be 10 minutes long
VIDEO_SEGMENT_OVERLAP_SECONDS = 15         # Overlap between segments

@celery_app.task(bind=True)
def health_check_task(self):
    """A simple task to check if the worker is alive and responding."""
    print(f"Received health check task. ID: {self.request.id}")
    # Simulate some work
    time.sleep(5)
    print("Health check task completed.")
    return {"status": "ok"}

@celery_app.task(bind=True)
def process_video(self, object_key, bucket_name):
    """
    Main orchestration task. It downloads the video, checks its duration,
    and decides whether to segment it or process it as a whole.
    """
    if not s3_client:
        raise ConnectionError("S3 client is not initialized. Cannot process video.")

    self.update_state(state='PROGRESS', meta={'status': 'Downloading video from S3...'})
    print(f"[{self.request.id}] Starting processing for '{object_key}' from bucket '{bucket_name}'.")

    # Ensure a temporary directory exists
    tmp_dir = '/tmp/chronos_videos'
    os.makedirs(tmp_dir, exist_ok=True)
    local_path = os.path.join(tmp_dir, os.path.basename(object_key))

    try:
        # Download the file from S3
        s3_client.download_file(bucket_name, object_key, local_path)
        self.update_state(state='PROGRESS', meta={'status': 'Probing video duration...'})

        # Probe video for duration
        probe = ffmpeg.probe(local_path)
        duration = float(probe['format']['duration'])
        print(f"[{self.request.id}] Video duration for '{object_key}' is {duration} seconds.")

        if duration > VIDEO_SEGMENT_THRESHOLD_SECONDS:
            # Video is long, needs segmentation
            self.update_state(state='PROGRESS', meta={'status': f'Video is long ({duration}s). Queueing for segmentation.'})
            segment_video.delay(object_key, bucket_name)
        else:
            # Video is short, process as a single chunk
            self.update_state(state='PROGRESS', meta={'status': f'Video is short ({duration}s). Processing directly.'})
            analyze_video_chunk.delay(object_key, bucket_name)

    except Exception as e:
        self.update_state(state='FAILURE', meta={'status': f'Error during processing: {str(e)}'})
        print(f"[{self.request.id}] Error processing '{object_key}': {e}")
        # Re-raise the exception to mark the task as failed in Celery
        raise
    finally:
        # Clean up the downloaded file
        if os.path.exists(local_path):
            os.remove(local_path)
            print(f"[{self.request.id}] Cleaned up temporary file: {local_path}")

    return {'status': 'Orchestration started successfully'}


@celery_app.task(bind=True)
def segment_video(self, object_key, bucket_name):
    """
    Downloads a video, splits it into overlapping segments, uploads them back to S3,
    and triggers the analysis task for each segment.
    """
    if not s3_client:
        raise ConnectionError("S3 client is not initialized. Cannot process video.")

    self.update_state(state='PROGRESS', meta={'status': 'Starting segmentation...'})

    base_name = os.path.basename(object_key)
    tmp_dir = f'/tmp/chronos_segments_{self.request.id}'
    os.makedirs(tmp_dir, exist_ok=True)
    local_path = os.path.join(tmp_dir, base_name)

    segment_output_pattern = os.path.join(tmp_dir, f"{os.path.splitext(base_name)[0]}_%03d.mp4")

    try:
        # 1. Download the original large file
        self.update_state(state='PROGRESS', meta={'status': f'Downloading {object_key} for segmentation.'})
        s3_client.download_file(bucket_name, object_key, local_path)

        # 2. Use FFmpeg to split the video into segments
        self.update_state(state='PROGRESS', meta={'status': 'Running ffmpeg for segmentation.'})
        (
            ffmpeg
            .input(local_path)
            .output(
                segment_output_pattern,
                c='copy',               # Copy codecs to avoid re-encoding
                map=0,                  # Select all streams
                f='segment',            # Use the segment muxer
                segment_time=VIDEO_SEGMENT_DURATION_SECONDS,
                segment_format_options=f'movflags=+faststart',
                reset_timestamps=1
            )
            .run(capture_stdout=True, capture_stderr=True)
        )

        # 3. Upload each segment and trigger analysis task
        self.update_state(state='PROGRESS', meta={'status': 'Uploading segments and queueing analysis.'})
        segment_files = sorted(f for f in os.listdir(tmp_dir) if f.startswith(os.path.splitext(base_name)[0]) and f.endswith('.mp4'))

        for segment_file in segment_files:
            segment_local_path = os.path.join(tmp_dir, segment_file)
            segment_object_key = f"segments/{base_name}/{segment_file}"

            s3_client.upload_file(segment_local_path, bucket_name, segment_object_key)

            # Queue this segment for analysis
            analyze_video_chunk.delay(segment_object_key, bucket_name)

        print(f"[{self.request.id}] Successfully segmented '{object_key}' into {len(segment_files)} chunks.")

    except ffmpeg.Error as e:
        print(f"FFmpeg error: {e.stderr.decode()}")
        self.update_state(state='FAILURE', meta={'status': f'FFmpeg error: {e.stderr.decode()}'})
        raise
    except Exception as e:
        self.update_state(state='FAILURE', meta={'status': f'Error during segmentation: {str(e)}'})
        raise
    finally:
        # 4. Clean up local files
        if os.path.exists(tmp_dir):
            import shutil
            shutil.rmtree(tmp_dir)
            print(f"[{self.request.id}] Cleaned up temporary segment directory: {tmp_dir}")

    return {'status': f'segmentation complete, {len(segment_files)} chunks created'}


@celery_app.task(bind=True)
def analyze_video_chunk(self, object_key, bucket_name):
    """
    Orchestrates the analysis of a single video chunk by chaining together
    the individual analysis tasks.
    """
    self.update_state(state='PROGRESS', meta={'status': 'Queueing analysis chain...'})
    print(f"[{self.request.id}] Orchestrating analysis for chunk {object_key}")

    # Define the chain of analysis tasks
    analysis_pipeline = chain(
        transcribe_audio.s(object_key, bucket_name),
        detect_scenes.s(bucket_name), # .s() creates a signature, allowing us to chain
        generate_visual_tags.s(bucket_name)
    )

    # Execute the chain
    analysis_pipeline.delay()

    return {'status': 'analysis pipeline started for chunk'}

@celery_app.task(bind=True)
def transcribe_audio(self, object_key, bucket_name):
    """Placeholder for Speech-to-Text analysis."""
    self.update_state(state='PROGRESS', meta={'status': f'Transcribing audio for {object_key}...'})
    print(f"[{self.request.id}] Placeholder: Transcribing audio for {object_key}")
    time.sleep(10) # Simulate Whisper transcription
    # In a real task, this would return the path to the transcript file or the transcript data itself.
    return {'object_key': object_key, 'transcript_result': 'path/to/transcript.json'}

@celery_app.task(bind=True)
def detect_scenes(self, previous_result, bucket_name):
    """Placeholder for scene detection."""
    object_key = previous_result['object_key']
    self.update_state(state='PROGRESS', meta={'status': f'Detecting scenes in {object_key}...'})
    print(f"[{self.request.id}] Placeholder: Detecting scenes in {object_key}")
    time.sleep(5) # Simulate scene detection
    previous_result['scene_detection_result'] = 'path/to/scenes.json'
    return previous_result

@celery_app.task(bind=True)
def generate_visual_tags(self, previous_result, bucket_name):
    """Placeholder for visual tagging."""
    object_key = previous_result['object_key']
    self.update_state(state='PROGRESS', meta={'status': f'Generating visual tags for {object_key}...'})
    print(f"[{self.request.id}] Placeholder: Generating visual tags for {object_key}")
    time.sleep(5) # Simulate visual tagging
    previous_result['visual_tags_result'] = 'path/to/tags.json'
    print(f"[{self.request.id}] Analysis pipeline complete for {object_key}")
    return previous_result
