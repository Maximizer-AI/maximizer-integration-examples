// Maximizer AI — GCP integration example (Java).
//
// Consumer side: subscribes to the Pub/Sub "results ready" topic and downloads
// the matching results object from Google Cloud Storage.
//
// Configuration comes from environment variables:
//   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
//   export MAXIMIZER_GCP_PROJECT_ID=your-gcp-project-id
//   export MAXIMIZER_GCP_SUBSCRIPTION=file-status-subscription
//   export MAXIMIZER_RESULTS_BUCKET=your-results-bucket
//   java Consumer
//
// Maven dependencies:
//   com.google.cloud:google-cloud-pubsub
//   com.google.cloud:google-cloud-storage
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.pubsub.v1.ProjectSubscriptionName;
import org.json.JSONObject;

import java.nio.file.Paths;

public class Consumer {
    static String env(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) throw new IllegalStateException("missing env var: " + name);
        return v;
    }

    public static void main(String[] args) throws Exception {
        String projectId = env("MAXIMIZER_GCP_PROJECT_ID");
        String subscriptionId = env("MAXIMIZER_GCP_SUBSCRIPTION");
        String resultsBucket = env("MAXIMIZER_RESULTS_BUCKET");
        Storage storage = StorageOptions.getDefaultInstance().getService();

        MessageReceiver receiver = (message, consumer) -> {
            try {
                JSONObject data = new JSONObject(message.getData().toStringUtf8());
                if ("Results ready".equals(data.optString("status")) && data.has("results_file")) {
                    String resultsFile = data.getString("results_file");
                    Blob blob = storage.get(BlobId.of(resultsBucket, resultsFile));
                    if (blob != null) {
                        blob.downloadTo(Paths.get(resultsFile.replace('/', '_')));
                        System.out.println("downloaded result: " + resultsFile);
                    }
                }
            } catch (Exception e) {
                System.err.println("error handling message: " + e.getMessage());
            } finally {
                consumer.ack();
            }
        };

        ProjectSubscriptionName sub = ProjectSubscriptionName.of(projectId, subscriptionId);
        Subscriber subscriber = Subscriber.newBuilder(sub, receiver).build();
        subscriber.startAsync().awaitRunning();
        System.out.println("listening on " + sub);
        subscriber.awaitTerminated();
    }
}
