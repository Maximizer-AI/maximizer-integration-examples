// Maximizer AI — AWS S3 integration example (Java / AWS SDK for Java v2).
//
// Uploads a call recording to the Maximizer INPUT bucket, then retrieves the
// **client results file** from the RESULTS location.
//
// What you receive: a curated display document built only from the fields
// configured for your tenant (an allow-list); format .json or .pdf per
// your tenant config. It is NOT the internal *.llm-results.json dataset. In
// production the pipeline notifies you with the exact key; this example polls
// the results location under your category prefix as a simple fallback.
//
// Environment:
//   MAXIMIZER_AWS_ACCESS_KEY_ID, MAXIMIZER_AWS_SECRET_ACCESS_KEY,
//   MAXIMIZER_AWS_REGION (default us-west-2),
//   MAXIMIZER_INPUT_BUCKET, MAXIMIZER_RESULTS_BUCKET,
//   MAXIMIZER_RESULTS_EXT (json | pdf, default json)
//   MAXIMIZER_RESULTS_QUEUE_URL (notification queue, recommended; omit to poll the bucket)
//   java UploadAndFetch claims ./call.wav
//
// Maven dependencies: software.amazon.awssdk:s3 and software.amazon.awssdk:sqs
// (BOM 2.25.x or later).
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class UploadAndFetch {
    static final Set<String> ALLOWED =
        Set.of("claims", "service", "new_policy_sales", "renew_policy_sales");

    static String env(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) throw new IllegalStateException("missing env var: " + name);
        return v;
    }

    static Set<String> keys(S3Client s3, String bucket, String prefix, String ext) {
        List<S3Object> objs = s3.listObjectsV2(
            ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()).contents();
        return objs.stream().map(S3Object::key).filter(k -> k.endsWith("." + ext)).collect(Collectors.toSet());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) { System.err.println("usage: UploadAndFetch <category> <audio_file>"); System.exit(2); }
        String category = args[0];
        Path audioPath = Paths.get(args[1]);
        if (!ALLOWED.contains(category)) { System.err.println("category must be one of " + ALLOWED); System.exit(2); }
        // The bucket accepts these and refuses everything else with AccessDenied,
        // so checking here only turns an opaque permission error into a clear one.
        String audioExt = audioPath.toString().replaceFirst("^.*(?=\\.[^.]*$)", "");
        boolean extOk = java.util.List.of(".wav", ".mp3", ".json").contains(audioExt.toLowerCase())
                && (audioExt.equals(audioExt.toLowerCase()) || audioExt.equals(audioExt.toUpperCase()));
        if (!extOk) {
            System.err.println(audioPath + ": only .wav, .mp3, .json may be uploaded, spelled all-lower or all-upper; convert or rename first");
            System.exit(2);
        }

        String region = System.getenv().getOrDefault("MAXIMIZER_AWS_REGION", "us-west-2");
        String inputBucket = env("MAXIMIZER_INPUT_BUCKET");
        String resultsBucket = env("MAXIMIZER_RESULTS_BUCKET");
        String ext = System.getenv().getOrDefault("MAXIMIZER_RESULTS_EXT", "json").replaceFirst("^\\.", "");

        S3Client s3 = S3Client.builder()
            .region(Region.of(region))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                env("MAXIMIZER_AWS_ACCESS_KEY_ID"), env("MAXIMIZER_AWS_SECRET_ACCESS_KEY"))))
            .build();

        // The category comes from the FILE NAME, not the folder: a recording with
        // no `<category>__` token is analysed as a claim wherever it is put.
        String filename = audioPath.getFileName().toString();
        if (!filename.startsWith(category + "__")) filename = category + "__" + filename;
        String key = category + "/" + filename;

        // 1) Upload. SSE-KMS is applied by the bucket automatically.
        System.out.printf("uploading %s -> s3://%s/%s%n", audioPath, inputBucket, key);
        s3.putObject(PutObjectRequest.builder().bucket(inputBucket).key(key).build(),
                     RequestBody.fromFile(audioPath));
        System.out.println("upload complete");

        // 2) Retrieve the curated results file.
        //    Preferred: wait for the notification. For every processed file the
        //    pipeline publishes one file_status message to your environment's queue:
        //    {"upload_file_name","results_file_name","status":"processed","message_timestamp"}.
        //    Same access key, registered IP ranges only. Delete the message once handled.
        long deadline = System.currentTimeMillis() + 15 * 60 * 1000L;
        String queueUrl = System.getenv("MAXIMIZER_RESULTS_QUEUE_URL");
        if (queueUrl != null && !queueUrl.isBlank()) {
            try (SqsClient sqs = SqsClient.builder().region(Region.of(region))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        System.getenv("MAXIMIZER_AWS_ACCESS_KEY_ID"), System.getenv("MAXIMIZER_AWS_SECRET_ACCESS_KEY")))).build()) {
                System.out.printf("waiting for the file_status notification on %s%n", queueUrl);
                while (System.currentTimeMillis() < deadline) {
                    List<Message> msgs = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl).maxNumberOfMessages(10).waitTimeSeconds(20).build()).messages();
                    for (Message m : msgs) {
                        String body = m.body();
                        String uploadName = jsonField(body, "upload_file_name");
                        if (!key.equals(uploadName)) continue; // another file's notification
                        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
                        String resultKey = jsonField(body, "results_file_name");
                        Path out = Paths.get(Paths.get(resultKey).getFileName().toString());
                        s3.getObject(GetObjectRequest.builder().bucket(resultsBucket).key(resultKey).build(),
                                     ResponseTransformer.toFile(out));
                        System.out.printf("result ready: s3://%s/%s -> %s%n", resultsBucket, resultKey, out);
                        return;
                    }
                }
                System.err.println("timed out waiting for the notification — contact Maximizer with the upload key");
                System.exit(1);
            }
        }

        //    Fallback without a queue: poll the results location under your category prefix.
        String prefix = category + "/";
        Set<String> before = keys(s3, resultsBucket, prefix, ext);
        System.out.printf("waiting for a new *.%s result under s3://%s/%s%n", ext, resultsBucket, prefix);

        while (System.currentTimeMillis() < deadline) {
            Set<String> current = keys(s3, resultsBucket, prefix, ext);
            TreeSet<String> fresh = current.stream().filter(k -> !before.contains(k))
                .collect(Collectors.toCollection(TreeSet::new));
            Optional<String> hit = Optional.ofNullable(fresh.isEmpty() ? null : fresh.last());
            if (hit.isPresent()) {
                String resultKey = hit.get();
                Path out = Paths.get(Paths.get(resultKey).getFileName().toString());
                s3.getObject(GetObjectRequest.builder().bucket(resultsBucket).key(resultKey).build(),
                             ResponseTransformer.toFile(out));
                System.out.printf("result ready: s3://%s/%s -> %s%n", resultsBucket, resultKey, out);
                return;
            }
            Thread.sleep(15000);
        }
        System.err.println("timed out waiting for result — contact Maximizer with the upload key");
        System.exit(1);
    }

    /** Minimal extractor for the flat file_status message — avoids a JSON dependency. */
    static String jsonField(String json, String field) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\"" + field + "\\"\\s*:\\s*\\"((?:[^\\"\\\\]|\\\\.)*)\\"").matcher(json);
        return m.find() ? m.group(1).replace("\\/", "/") : null;
    }
}
