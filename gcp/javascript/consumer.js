
// Client Side Code (Consumer)

// Install Node.JS Client Dependencies:
// npm install @google-cloud/pubsub @google-cloud/storage



const { PubSub } = require('@google-cloud/pubsub');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');

// GCP configurations
const requiredEnv = (name) => {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
};

const projectId = requiredEnv('MAXIMIZER_GCP_PROJECT_ID');
const subscriptionName = requiredEnv('MAXIMIZER_GCP_SUBSCRIPTION');
const bucketName = requiredEnv('MAXIMIZER_RESULTS_BUCKET');
const mockDatabase = {};

// Initialize Pub/Sub and Storage clients
const pubSubClient = new PubSub({ projectId });
const storage = new Storage({ projectId });

// Utility function to download a file from GCS
async function downloadFile(fileName, destination) {
  const bucket = storage.bucket(bucketName);
  const file = bucket.file(fileName);
  await file.download({ destination });
  console.log(`Downloaded ${fileName} to ${destination}`);
}

// Process messages from Pub/Sub
function listenForMessages() {
  const subscription = pubSubClient.subscription(subscriptionName);

  subscription.on('message', async (message) => {
    console.log(`Received message: ${message.data.toString()}`);
    const data = JSON.parse(message.data.toString());

    if (data.status === 'Results ready' && data.results_file) {
      const resultsFile = data.results_file;
      const localPath = `/tmp/${resultsFile}`;

      try {
        await downloadFile(resultsFile, localPath);
        const results = fs.readFileSync(localPath, 'utf8');
        mockDatabase[data.file_name] = JSON.parse(results);
        console.log(`Results for ${data.file_name} saved to mock database:`, mockDatabase[data.file_name]);
      } catch (error) {
        console.error(`Error downloading or saving results for ${data.file_name}:`, error);
      }
    }

    message.ack();
  });

  subscription.on('error', (error) => {
    console.error('Error listening for messages:', error);
  });

  console.log(`Listening for messages on subscription: ${subscriptionName}`);
}

// Start listening
listenForMessages();
