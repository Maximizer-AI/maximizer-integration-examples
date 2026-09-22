
### Client Side Code (Consumer)

## Install Python Client Dependencies:
# pip install google-cloud-pubsub google-cloud-storage


from google.cloud import storage, pubsub_v1
import json
import os

# Set up GCP configurations for user authentication
# User must authenticate using `gcloud auth application-default login`
PROJECT_ID = os.environ["MAXIMIZER_GCP_PROJECT_ID"]
SUBSCRIPTION_ID = os.environ["MAXIMIZER_GCP_SUBSCRIPTION"]
BUCKET_NAME = os.environ["MAXIMIZER_RESULTS_BUCKET"]
MOCK_DATABASE = {}  # Simulating a database as a dictionary

# Documentation: User Account Configuration
# Ensure the authenticated user has the following roles:
# 1. Storage Object Viewer: To read files from the GCS bucket.
# 2. Pub/Sub Subscriber: To subscribe to Pub/Sub topics and process messages.

# Define GCS utility functions
def file_exists(bucket_name, file_name):
    """Check if a file exists in a GCP bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.exists()

def download_file(bucket_name, file_name, destination):
    """Download a file from GCS to a local destination."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(destination)
    print(f"Downloaded {file_name} to {destination}")

def check_and_download_results(file_name):
    """Check if results exist for a file and download them to a mock database."""
    results_file = f"{file_name}.results.json"
    if file_exists(BUCKET_NAME, results_file):
        print(f"Results file found for {file_name}. Proceeding to download...")
        local_path = f"/tmp/{results_file}"
        download_file(BUCKET_NAME, results_file, local_path)

        # Simulate saving to a database
        with open(local_path, "r") as file:
            MOCK_DATABASE[file_name] = json.load(file)
        print(f"Results for {file_name} saved to mock database: {MOCK_DATABASE[file_name]}")
    else:
        print(f"No results found for {file_name}.")

# Example client logic to listen for messages
def listen_for_messages():
    """Subscribe to a Pub/Sub topic and process messages."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    def callback(message):
        print(f"Received message: {message.data.decode('utf-8')}")
        data = json.loads(message.data.decode('utf-8'))

        # If results are ready, attempt to download them
        if data.get("status") == "Results ready" and "results_file" in data:
            check_and_download_results(data["file_name"])

        message.ack()

    # Listen to the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...\n")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    # Start listening for Pub/Sub messages
    listen_for_messages()
