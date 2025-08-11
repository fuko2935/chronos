import dspy
import os
import json
import boto3
from botocore.client import Config
from utils.key_manager import key_manager

# --- S3 Client Setup ---
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

try:
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=S3_REGION
    )
except Exception as e:
    print(f"Error initializing S3 client in ai_core: {e}")
    s3_client = None

def configure_gemini_lm():
    """
    Configures and returns a DSPy Google LLM client.

    This function retrieves a key from our ApiKeyManager and uses it
    to configure the language model. This approach allows us to rotate
    keys for each call if needed.
    """
    gemini_api_key = key_manager.get_key()

    if not gemini_api_key:
        print("ERROR: No available Gemini API keys to configure the language model.")
        return None

    # As per the PRD, the best model is gemini-2.5-pro.
    # We will use this model for our DSPy configuration.
    # dspy.Google requires the model name to be passed.
    llm = dspy.Google(
        model='gemini-2.5-pro',
        api_key=gemini_api_key,
        max_output_tokens=1024 # A reasonable default
    )

    dspy.settings.configure(lm=llm)

    print(f"DSPy configured with Gemini model and key ending in ...{gemini_api_key[-4:]}")
    return llm

# We can call this on module load to have a default configuration.
# In a more complex app, this might be done on app startup.
# Note: This is a basic setup. A robust implementation would wrap DSPy calls
# in a try/except block to catch rate limit errors (429) and report them
# back to the key_manager, then retry with a new key.
# For now, we will configure it once.
configure_gemini_lm()


# --- DSPy Signatures and Programs ---

class VideoRAGSignature(dspy.Signature):
    """
    Given a video transcript as context and a user's question,
    generate a conversational answer and a JSON array of relevant video clips.
    """
    context = dspy.InputField(desc="The full text transcript of a video, with word-level timestamps.")
    question = dspy.InputField(desc="The user's question about the video content.")

    clips = dspy.OutputField(
        desc="A JSON formatted string representing an array of objects. Each object must have a 'start' and 'end' time in seconds. Example: [{\"start\": 120.5, \"end\": 125.0}]"
    )
    answer = dspy.OutputField(
        desc="A conversational, helpful answer that summarizes the findings based on the context."
    )

# Create the DSPy program using the signature
rag_predictor = dspy.Predict(VideoRAGSignature)

def query_video_transcript(transcript_s3_path: str, bucket_name: str, user_query: str):
    """
    Queries a video transcript using the RAG pipeline.
    """
    if not s3_client or not dspy.settings.lm:
        raise ConnectionError("A required service (S3 or LLM) is not initialized.")

    try:
        # 1. Load the transcript from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=transcript_s3_path)
        transcript_data = json.loads(response['Body'].read().decode('utf-8'))

        # Format the context for the LLM. We'll just use the full text for now.
        # A more advanced implementation would format this better or chunk it.
        context = transcript_data.get('text', '')

        # 2. Run the DSPy RAG program
        prediction = rag_predictor(context=context, question=user_query)

        # 3. Parse the results
        try:
            clips = json.loads(prediction.clips)
        except json.JSONDecodeError:
            print(f"Warning: LLM output for clips was not valid JSON: {prediction.clips}")
            clips = [] # Default to an empty list if parsing fails

        return {
            "clips": clips,
            "answer": prediction.answer
        }

    except Exception as e:
        print(f"Error during RAG query for transcript {transcript_s3_path}: {e}")
        # In a real app, you might want to report this failure to the key manager
        # if it's a 429 error.
        raise
