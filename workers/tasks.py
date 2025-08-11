import os
import time
import json
import boto3
import ffmpeg
import whisper
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

# --- Model Loading ---
# Load the Whisper model once per worker process in the global scope.
# The 'base' model is a good starting point.
try:
    whisper_model = whisper.load_model("base")
    print("Whisper model 'base' loaded successfully.")
except Exception as e:
    print(f"Error loading Whisper model: {e}")
    whisper_model = None

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
    """
    Downloads a video chunk, transcribes the audio using Whisper, and uploads
    the transcript as a JSON file to S3.
    """
    if not s3_client or not whisper_model:
        raise ConnectionError("A required service (S3 or Whisper) is not initialized.")

    self.update_state(state='PROGRESS', meta={'status': f'Transcribing audio for {object_key}...'})
    print(f"[{self.request.id}] Starting transcription for {object_key}")

    base_name = os.path.basename(object_key)
    tmp_dir = f'/tmp/chronos_transcribe_{self.request.id}'
    os.makedirs(tmp_dir, exist_ok=True)
    local_video_path = os.path.join(tmp_dir, base_name)

    try:
        # 1. Download video chunk
        s3_client.download_file(bucket_name, object_key, local_video_path)

        # 2. Transcribe audio
        self.update_state(state='PROGRESS', meta={'status': 'Running Whisper model...'})
        result = whisper_model.transcribe(local_video_path, word_timestamps=True)

        # 3. Upload transcript to S3
        transcript_json = json.dumps(result, indent=2)
        transcript_key = f"transcripts/{os.path.splitext(base_name)[0]}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=transcript_key,
            Body=transcript_json,
            ContentType='application/json'
        )
        print(f"[{self.request.id}] Successfully transcribed and uploaded transcript to {transcript_key}")

        # 4. Return the path to the transcript for the next task in the chain
        return {'object_key': object_key, 'transcript_s3_path': transcript_key}

    except Exception as e:
        self.update_state(state='FAILURE', meta={'status': f'Error during transcription: {str(e)}'})
        print(f"[{self.request.id}] Error during transcription of '{object_key}': {e}")
        raise
    finally:
        # 5. Clean up local files
        if os.path.exists(tmp_dir):
            import shutil
            shutil.rmtree(tmp_dir)
            print(f"[{self.request.id}] Cleaned up temporary transcription directory: {tmp_dir}")

@celery_app.task(bind=True)
def detect_scenes(self, previous_result, bucket_name):
    """Placeholder for scene detection."""
    object_key = previous_result['object_key']
    transcript_s3_path = previous_result['transcript_s3_path']
    self.update_state(state='PROGRESS', meta={'status': f'Detecting scenes in {object_key}...'})
    print(f"[{self.request.id}] Placeholder: Detecting scenes in {object_key} using transcript {transcript_s3_path}")
    time.sleep(5) # Simulate scene detection
    previous_result['scene_detection_s3_path'] = f"scenes/{os.path.splitext(os.path.basename(object_key))[0]}.json"
    return previous_result

@celery_app.task(bind=True)
def generate_visual_tags(self, previous_result, bucket_name):
    """Placeholder for visual tagging."""
    object_key = previous_result['object_key']
    self.update_state(state='PROGRESS', meta={'status': f'Generating visual tags for {object_key}...'})
    print(f"[{self.request.id}] Placeholder: Generating visual tags for {object_key}")
    time.sleep(5) # Simulate visual tagging
    previous_result['visual_tags_s3_path'] = f"tags/{os.path.splitext(os.path.basename(object_key))[0]}.json"
    print(f"[{self.request.id}] Analysis pipeline complete for {object_key}")
    # This is the final result of the analysis chain
    return previous_result

@celery_app.task(bind=True)
def render_remaining_clips(self, selected_clips, source_object_key, bucket_name):
    """
    Calculates the inverse of the selected clips and renders the result.
    'selected_clips' is a list of dicts: [{'start': float, 'end': float}]
    These times are relative to the single 'source_object_key'.
    """
    if not s3_client:
        raise ConnectionError("S3 client is not initialized.")

    self.update_state(state='PROGRESS', meta={'status': 'Calculating remaining clips...'})

    tmp_dir = f'/tmp/chronos_render_remaining_{self.request.id}'
    os.makedirs(tmp_dir, exist_ok=True)
    local_source_path = os.path.join(tmp_dir, os.path.basename(source_object_key))

    try:
        # 1. Get total duration of the source video
        s3_client.download_file(bucket_name, source_object_key, local_source_path)
        probe = ffmpeg.probe(local_source_path)
        total_duration = float(probe['format']['duration'])

        # 2. Calculate the inverse clips (the "remaining" parts)
        sorted_clips = sorted(selected_clips, key=lambda x: x['start'])
        remaining_clips = []
        cursor = 0.0

        for clip in sorted_clips:
            if clip['start'] > cursor:
                remaining_clips.append({'start': cursor, 'end': clip['start']})
            cursor = max(cursor, clip['end'])

        if cursor < total_duration:
            remaining_clips.append({'start': cursor, 'end': total_duration})

        if not remaining_clips:
            return {'status': 'complete', 'message': 'No remaining clips to render.'}

        # 3. Reuse the rendering logic (simplified here, should be refactored)
        self.update_state(state='PROGRESS', meta={'status': 'Rendering remaining clips...'})
        trimmed_clip_paths = []
        for i, clip in enumerate(remaining_clips):
            trimmed_path = os.path.join(tmp_dir, f"remaining_clip_{i}.mp4")
            (
                ffmpeg
                .input(local_source_path, ss=clip['start'], to=clip['end'])
                .output(trimmed_path, c='copy').run(capture_stdout=True, capture_stderr=True)
            )
            trimmed_clip_paths.append(trimmed_path)

        concat_list_path = os.path.join(tmp_dir, 'concat_list.txt')
        with open(concat_list_path, 'w') as f:
            for path in trimmed_clip_paths:
                f.write(f"file '{os.path.basename(path)}'\n")

        final_video_path = os.path.join(tmp_dir, 'final_remaining_video.mp4')
        (
            ffmpeg
            .input(concat_list_path, format='concat', safe=0)
            .output(final_video_path, c='copy').run(capture_stdout=True, capture_stderr=True)
        )

        final_video_key = f"rendered/remaining_{self.request.id}.mp4"
        s3_client.upload_file(final_video_path, bucket_name, final_video_key)

        return {'status': 'complete', 'final_video_s3_path': final_video_key}

    except Exception as e:
        self.update_state(state='FAILURE', meta={'status': f'Error during remaining clips render: {str(e)}'})
        raise
    finally:
        if os.path.exists(tmp_dir):
            import shutil
            shutil.rmtree(tmp_dir)

@celery_app.task(bind=True)
def render_video_clips(self, clips, bucket_name):
    """
    Renders a final video by trimming and concatenating a list of clips.
    'clips' is a list of dicts: [{'object_key': str, 'start': float, 'end': float}]
    """
    if not s3_client:
        raise ConnectionError("S3 client is not initialized.")

    self.update_state(state='PROGRESS', meta={'status': 'Starting video render...'})

    tmp_dir = f'/tmp/chronos_render_{self.request.id}'
    os.makedirs(tmp_dir, exist_ok=True)

    trimmed_clip_paths = []

    try:
        # 1. Trim each clip
        for i, clip in enumerate(clips):
            self.update_state(state='PROGRESS', meta={'status': f'Processing clip {i+1}/{len(clips)}...'})
            source_key = clip['object_key']
            start_time = clip['start']
            end_time = clip['end']

            local_source_path = os.path.join(tmp_dir, os.path.basename(source_key))
            trimmed_clip_path = os.path.join(tmp_dir, f"clip_{i}.mp4")

            s3_client.download_file(bucket_name, source_key, local_source_path)

            (
                ffmpeg
                .input(local_source_path, ss=start_time, to=end_time)
                .output(trimmed_clip_path, c='copy') # Re-mux without re-encoding
                .run(capture_stdout=True, capture_stderr=True)
            )
            trimmed_clip_paths.append(trimmed_clip_path)
            os.remove(local_source_path) # Clean up source file immediately

        # 2. Concatenate trimmed clips
        self.update_state(state='PROGRESS', meta={'status': 'Concatenating clips...'})
        concat_list_path = os.path.join(tmp_dir, 'concat_list.txt')
        with open(concat_list_path, 'w') as f:
            for path in trimmed_clip_paths:
                f.write(f"file '{os.path.basename(path)}'\n")

        final_video_path = os.path.join(tmp_dir, 'final_video.mp4')
        (
            ffmpeg
            .input(concat_list_path, format='concat', safe=0)
            .output(final_video_path, c='copy')
            .run(capture_stdout=True, capture_stderr=True)
        )

        # 3. Upload final video to S3
        self.update_state(state='PROGRESS', meta={'status': 'Uploading final video...'})
        final_video_key = f"rendered/{self.request.id}.mp4"
        s3_client.upload_file(final_video_path, bucket_name, final_video_key)

        return {'status': 'complete', 'final_video_s3_path': final_video_key}

    except ffmpeg.Error as e:
        print(f"FFmpeg error during render: {e.stderr.decode()}")
        self.update_state(state='FAILURE', meta={'status': f'FFmpeg error: {e.stderr.decode()}'})
        raise
    except Exception as e:
        self.update_state(state='FAILURE', meta={'status': f'Error during render: {str(e)}'})
        raise
    finally:
        # 4. Clean up local files
        if os.path.exists(tmp_dir):
            import shutil
            shutil.rmtree(tmp_dir)
            print(f"[{self.request.id}] Cleaned up temporary render directory: {tmp_dir}")
