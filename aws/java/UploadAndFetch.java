// Upload recordings and metadata, then download the completion result.
// See README.md. Dependencies: software.amazon.awssdk:s3, software.amazon.awssdk:sqs,
// and com.fasterxml.jackson.core:jackson-databind (with its core and annotations dependencies).
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UploadAndFetch {
    static final ObjectMapper JSON = new ObjectMapper()
        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    static void checkCategory(String category) {
        if (!category.matches("[a-z0-9_]+") || category.contains("__"))
            fail("category must use lowercase letters, digits or underscores, with no '__'");
    }

    static String metadataName(String category, String ucid, Path path) {
        String prefix = category + "__" + ucid;
        String name = path.getFileName().toString();
        String pattern = "\\.[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)*\\.json";
        if (name.startsWith(prefix) && name.substring(prefix.length()).matches(pattern)) return name;
        String suffix = setting("MAXIMIZER_METADATA_SUFFIX", "");
        if (!suffix.matches(pattern)) fail("metadata filename must be <category>__<UCID>.<suffix>.json, or set MAXIMIZER_METADATA_SUFFIX to the full ending agreed at onboarding (including the leading dot and .json)");
        return prefix + suffix;
    }

    // A single construction seam lets offline CLI tests supply local fakes.
    // The default runtime reads the environment and uses the ordinary AWS SDK.
    static class Clients {
        String environment(String name) { return System.getenv(name); }
        S3Client s3(String region) {
            return S3Client.builder().region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    env("MAXIMIZER_AWS_ACCESS_KEY_ID"), env("MAXIMIZER_AWS_SECRET_ACCESS_KEY"))))
                .build();
        }
        S3Client resultS3(String region) {
            // Result-only reads never hide access/network failures behind SDK retries.
            return S3Client.builder().region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    env("MAXIMIZER_AWS_ACCESS_KEY_ID"), env("MAXIMIZER_AWS_SECRET_ACCESS_KEY"))))
                .overrideConfiguration(configuration -> configuration
                    .retryPolicy(software.amazon.awssdk.core.retry.RetryPolicy.builder(
                        software.amazon.awssdk.core.retry.RetryMode.STANDARD).numRetries(0).build())
                    .apiCallAttemptTimeout(Duration.ofSeconds(30)).apiCallTimeout(Duration.ofSeconds(30)))
                .build();
        }
        long resultTimeMillis() { return System.nanoTime() / 1_000_000; }
        void resultPollSleep(long milliseconds) throws InterruptedException { Thread.sleep(milliseconds); }
        SqsClient sqs(String region) {
            return SqsClient.builder().region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    env("MAXIMIZER_AWS_ACCESS_KEY_ID"), env("MAXIMIZER_AWS_SECRET_ACCESS_KEY"))))
                .build();
        }
        void admittedUpload(Path file, String bucket, String key, String function) throws Exception {
            AdmittedUploads.uploadConfigured(file, bucket, key, env("MAXIMIZER_AWS_REGION"), function,
                StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    env("MAXIMIZER_AWS_ACCESS_KEY_ID"), env("MAXIMIZER_AWS_SECRET_ACCESS_KEY"))));
        }
    }
    static Clients clients = new Clients();
    static String setting(String name, String fallback) {
        String value = clients.environment(name); return value == null ? fallback : value;
    }
    static String env(String name) {
        String v = clients.environment(name);
        if (v == null || v.isBlank()) throw new IllegalStateException("missing env var: " + name);
        return v;
    }

    static final String USAGE = String.join("\n",
        "usage:",
        "  UploadAndFetch <category> <audio_file>",
        "  UploadAndFetch --ucid [--batch] <category> <UCID> <metadata.json> <audio> [<audio> ...]",
        "  UploadAndFetch --org <org_YYYY-MM-DD.json> <users_YYYY-MM-DD.json>",
        "  UploadAndFetch --result-only <category> <UCID>");

    static final java.util.regex.Pattern ORG_NAME =
        java.util.regex.Pattern.compile("(org|users)_(\\d{4}-\\d{2}-\\d{2})\\.json");

    static S3Client s3Client(String region) {
        return clients.s3(region);
    }

    static void fail(String message) { System.err.println(message); System.exit(2); }

    static boolean admitted(String function) { return function != null && !function.isEmpty(); }

    interface FileUpload { void upload(Path path, String bucket, String key) throws Exception; }
    static void put(S3Client s3, String bucket, String key, Path path) throws Exception {
        String function = clients.environment("MAXIMIZER_UPLOAD_FUNCTION");
        put(s3, bucket, key, path, function,
            (file, targetBucket, targetKey) -> clients.admittedUpload(file, targetBucket, targetKey, function));
    }
    static void put(S3Client s3, String bucket, String key, Path path, String function,
                    FileUpload transfer) throws Exception {
        System.out.printf("uploading %s -> %s/%s%n", path, bucket, key);
        if (admitted(function)) transfer.upload(path, bucket, key);
        else s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(path));
    }

    /** One call: its recording part(s) plus one metadata file, named by the UCID. */
    static void uploadUcidCall(String category, String ucid, Path metadataPath, List<Path> audioPaths,
                               boolean batch) throws Exception {
        checkCategory(category);
        // "__" separates the category token; a trailing -<digits> is read as a part number.
        if (ucid.isEmpty() || ucid.contains("/") || ucid.contains("__") || ucid.matches(".+-\\d+"))
            fail(ucid + ": a UCID may not be empty, contain '/' or '__', or end in -<digits>");
        String folder = category + (batch ? "_batch" : "");
        // One-part recording: <category>__<UCID>.wav; parts: <category>__<UCID>-<n>.wav.
        List<String> audioNames = new ArrayList<>();
        for (int i = 0; i < audioPaths.size(); i++) {
            String name = audioPaths.get(i).getFileName().toString();
            String ext = name.contains(".") ? name.substring(name.lastIndexOf('.')) : "";
            if (!List.of(".wav", ".mp3").contains(ext.toLowerCase())
                    || !(ext.equals(ext.toLowerCase()) || ext.equals(ext.toUpperCase())))
                fail(audioPaths.get(i) + ": a recording must be .wav or .mp3, spelled all-lower or all-upper");
            audioNames.add(category + "__" + ucid + (audioPaths.size() == 1 ? "" : "-" + (i + 1)) + ext);
        }
        String uploadMetadataName = metadataName(category, ucid, metadataPath);
        checkMetadataFile(metadataPath, category, ucid, audioNames);

        String region = env("MAXIMIZER_AWS_REGION"); // required: the region in your config file
        String inputBucket = env("MAXIMIZER_INPUT_BUCKET");
        S3Client s3 = s3Client(region);
        // 1) Every part plus the one metadata file, in any order.
        for (int i = 0; i < audioPaths.size(); i++) put(s3, inputBucket, folder + "/" + audioNames.get(i), audioPaths.get(i));
        put(s3, inputBucket, folder + "/" + uploadMetadataName, metadataPath);
        System.out.println("upload complete");
        // 2) The result is always <category>/<UCID>.json, also for batch and multi-part calls.
        waitForResult(s3, region, env("MAXIMIZER_RESULTS_BUCKET"), category + "/" + ucid + ".json");
    }

    static void waitForResult(S3Client s3, String region, String resultsBucket, String resultKey) throws Exception {
        long deadline = System.currentTimeMillis() + 15 * 60 * 1000L;
        Path out = Paths.get(Paths.get(resultKey).getFileName().toString());
        String queueUrl = clients.environment("MAXIMIZER_RESULTS_QUEUE_URL");
        if (queueUrl != null && !queueUrl.isBlank()) {
            try (SqsClient sqs = clients.sqs(region)) {
                if (waitForQueueResult(s3, sqs, resultsBucket, resultKey, queueUrl,
                        admitted(clients.environment("MAXIMIZER_UPLOAD_FUNCTION")), deadline)) return;
            }
            System.err.println("timed out waiting for the notification — contact Maximizer with the UCID");
            System.exit(1);
        }
        System.out.printf("waiting for %s/%s%n", resultsBucket, resultKey);
        while (System.currentTimeMillis() < deadline) {
            try {
                s3.headObject(HeadObjectRequest.builder().bucket(resultsBucket).key(resultKey).build());
            } catch (S3Exception e) {
                Thread.sleep(15000);
                continue;
            }
            s3.getObject(GetObjectRequest.builder().bucket(resultsBucket).key(resultKey).build(), ResponseTransformer.toFile(fresh(out)));
            System.out.printf("result ready: %s/%s -> %s%n", resultsBucket, resultKey, out);
            return;
        }
        System.err.println("timed out waiting for result — contact Maximizer with the UCID");
        System.exit(1);
    }

    static boolean existingResult(S3Client s3, String bucket, String key, Path out) throws Exception {
        try {
            s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (S3Exception error) {
            String code = error.awsErrorDetails() == null ? "" : error.awsErrorDetails().errorCode();
            if (error.statusCode() == 404 || "404".equals(code) || "NoSuchKey".equals(code) || "NotFound".equals(code)) return false;
            throw error;
        }
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(), ResponseTransformer.toFile(fresh(out)));
        System.out.printf("result ready: %s/%s -> %s%n", bucket, key, out);
        return true;
    }

    static boolean waitForQueueResult(S3Client s3, SqsClient sqs, String bucket, String key,
                                     String queue, boolean admitted, long deadline) throws Exception {
        Path out = Paths.get(Paths.get(key).getFileName().toString());
        if (admitted && existingResult(s3, bucket, key, out)) return true;
        System.out.printf("waiting for the file_status notification for %s on %s%n", key, queue);
        while (System.currentTimeMillis() < deadline) {
            for (Message message : sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queue).maxNumberOfMessages(10).waitTimeSeconds(20).build()).messages()) {
                if (!key.equals(jsonField(message.body(), "results_file_name"))) continue;
                s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(), ResponseTransformer.toFile(fresh(out)));
                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queue).receiptHandle(message.receiptHandle()).build());
                System.out.printf("result ready: %s/%s -> %s%n", bucket, key, out);
                return true;
            }
            if (admitted && existingResult(s3, bucket, key, out)) return true;
        }
        return false;
    }

    /** Read an existing call result without any upload or queue interaction. */
    static void resultOnly(String category, String ucid) throws Exception {
        checkCategory(category);
        if (ucid.isEmpty() || ucid.contains("/") || ucid.contains("__") || ucid.matches(".+-\\d+"))
            fail(ucid + ": a UCID may not be empty, contain '/' or '__', or end in -<digits>");
        // These names must remain one local filename on Windows as well as POSIX.
        if (ucid.contains("\\") || ucid.contains(":") || ucid.chars().anyMatch(c -> c < 32 || c == 127))
            fail("result UCID may not contain backslashes, colons or control characters");
        Path out = Paths.get(ucid + ".json").toAbsolutePath();
        String region = env("MAXIMIZER_AWS_REGION");
        String bucket = env("MAXIMIZER_RESULTS_BUCKET");
        String key = category + "/" + ucid + ".json";
        long deadline = clients.resultTimeMillis() + 15 * 60 * 1000L;
        // Deliberately do not inspect the input bucket, queue or upload function.
        try (S3Client s3 = clients.resultS3(region)) {
            while (clients.resultTimeMillis() < deadline) {
                try {
                    Duration timeout = resultReadTimeout(deadline);
                    s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key)
                        .overrideConfiguration(request -> request.apiCallAttemptTimeout(timeout).apiCallTimeout(timeout)).build());
                    downloadResultOnly(s3, bucket, key, out, deadline);
                    System.out.printf("result ready: %s/%s -> %s%n", bucket, key, out.getFileName());
                    return;
                } catch (S3Exception error) {
                    if (!resultNotFound(error)) throw error;
                }
                long remaining = deadline - clients.resultTimeMillis();
                if (remaining > 0) clients.resultPollSleep(Math.min(15000, remaining));
            }
        } catch (ResultOnlyTimeout error) {
            throw new ResultOnlyTimeout();
        } catch (S3Exception error) {
            throw new IllegalStateException("Result retrieval failed"
                + (error.statusCode() > 0 ? ": HTTP " + error.statusCode() : "")
                + "; check result access, network and UCID; no upload was attempted");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Result retrieval interrupted; no upload was attempted");
        } catch (Exception error) {
            throw new IllegalStateException("Result retrieval failed; check result access, network and UCID; no upload was attempted");
        }
        throw new ResultOnlyTimeout();
    }

    static final class ResultOnlyTimeout extends IllegalStateException {
        ResultOnlyTimeout() { super("timed out waiting for result — contact Maximizer with the UCID"); }
    }

    static boolean resultNotFound(S3Exception error) {
        if (error.statusCode() == 404) return true;
        // A known access/server error must never be disguised as pending work.
        if (error.statusCode() > 0) return false;
        String code = error.awsErrorDetails() == null ? null : error.awsErrorDetails().errorCode();
        return "404".equals(code) || "NoSuchKey".equals(code) || "NotFound".equals(code);
    }

    static Duration resultReadTimeout(long deadline) {
        long remaining = deadline - clients.resultTimeMillis();
        if (remaining <= 0) throw new ResultOnlyTimeout();
        return Duration.ofMillis(Math.min(30000, remaining));
    }

    static void downloadResultOnly(S3Client s3, String bucket, String key, Path out, long deadline) throws Exception {
        Path parent = out.getParent();
        Path temporary = Files.getFileStore(parent).supportsFileAttributeView("posix")
            ? Files.createTempFile(parent, ".maximizer-result-", ".partial",
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
            : Files.createTempFile(parent, ".maximizer-result-", ".partial");
        try {
            Duration timeout = resultReadTimeout(deadline);
            try (java.io.OutputStream stream = Files.newOutputStream(temporary)) {
                s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key)
                    .overrideConfiguration(request -> request.apiCallAttemptTimeout(timeout).apiCallTimeout(timeout)).build(),
                    ResponseTransformer.toOutputStream(stream));
            }
            Files.move(temporary, out, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    /** The SDK refuses to overwrite an existing file; a rerun for the same call must. */
    static Path fresh(Path path) throws Exception {
        Files.deleteIfExists(path);
        return path;
    }

    /** A UTF-8 BOM is tolerated. */
    static String readText(Path path) throws Exception {
        return Files.readString(path, StandardCharsets.UTF_8).replaceFirst("^\uFEFF", "");
    }

    static void checkFinite(JsonNode node) {
        if (node.isFloatingPointNumber() && !Double.isFinite(node.doubleValue()))
            fail("non-finite JSON number");
        if (node.isContainerNode()) for (JsonNode child : node) checkFinite(child);
    }

    static JsonNode readJson(Path path) throws Exception {
        JsonNode value = JSON.readTree(readText(path));
        if (value == null) throw new IllegalArgumentException(path + ": empty JSON");
        checkFinite(value);
        return value;
    }

    static void checkMetadataFile(Path metadataPath, String category, String ucid, List<String> audioNames) throws Exception {
        if (Files.size(metadataPath) > 1024 * 1024) fail(metadataPath + ": metadata must be at most 1 MiB");
        JsonNode metadata = readJson(metadataPath);
        if (!metadata.isObject()) fail(metadataPath + ": must be one JSON object");
        if (!metadata.path("ucid").isTextual() || !ucid.equals(metadata.path("ucid").textValue()))
            fail(metadataPath + ": 'ucid' must equal the call id " + ucid);
        if (!JSON.valueToTree(audioNames).equals(metadata.get("audio_files")))
            fail(metadataPath + ": 'audio_files' must list the uploaded names exactly, in part order: " + audioNames);
    }

    /** The daily org file and users list: same date, both to org/. No result file. */
    static void uploadOrgFiles(Path orgPath, Path usersPath) throws Exception {
        java.util.regex.Matcher o = ORG_NAME.matcher(orgPath.getFileName().toString());
        java.util.regex.Matcher u = ORG_NAME.matcher(usersPath.getFileName().toString());
        if (!o.matches() || !o.group(1).equals("org") || !u.matches() || !u.group(1).equals("users"))
            fail("expected org_YYYY-MM-DD.json and users_YYYY-MM-DD.json, in that order");
        if (!o.group(2).equals(u.group(2))) fail("the org file and the users list must carry the same date");
        for (Path p : List.of(orgPath, usersPath)) // refuse a file that is not a JSON array locally
            if (!readJson(p).isArray()) fail(p + ": must be a JSON array");
        S3Client s3 = s3Client(env("MAXIMIZER_AWS_REGION")); // required: the region in your config file
        String inputBucket = env("MAXIMIZER_INPUT_BUCKET");
        for (Path p : List.of(orgPath, usersPath)) put(s3, inputBucket, "org/" + p.getFileName(), p);
        System.out.println("upload complete (org files produce no result file)");
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("--result-only")) {
            if (args.length != 3) fail(USAGE);
            resultOnly(args[1], args[2]);
            return;
        }
        if (args.length == 3 && args[0].equals("--org")) {
            uploadOrgFiles(Paths.get(args[1]), Paths.get(args[2]));
            return;
        }
        if (args.length > 0 && args[0].equals("--ucid")) {
            int i = 1;
            boolean batch = args.length > 1 && args[1].equals("--batch");
            if (batch) i = 2;
            if (args.length - i < 4) fail(USAGE);
            List<Path> audio = new ArrayList<>();
            for (int j = i + 3; j < args.length; j++) audio.add(Paths.get(args[j]));
            uploadUcidCall(args[i], args[i + 1], Paths.get(args[i + 2]), audio, batch);
            return;
        }
        if (args.length != 2 || args[0].startsWith("--")) fail(USAGE);
        String category = args[0];
        Path audioPath = Paths.get(args[1]);
        checkCategory(category);
        String filename = audioPath.getFileName().toString();
        String audioExt = filename.contains(".") ? filename.substring(filename.lastIndexOf('.')) : "";
        if (!List.of(".wav", ".mp3", ".json").contains(audioExt.toLowerCase())
                || !(audioExt.equals(audioExt.toLowerCase()) || audioExt.equals(audioExt.toUpperCase())))
            fail(audioPath + ": expected .wav, .mp3 or .json, spelled all-lower or all-upper");
        String prefix = category + "__";
        if (!filename.startsWith(prefix)) filename = prefix + filename;
        String stem = filename.substring(prefix.length(), filename.lastIndexOf('.'));
        String ext = setting("MAXIMIZER_RESULTS_EXT", "json").replaceFirst("^\\.", "");
        String region = env("MAXIMIZER_AWS_REGION");
        try (S3Client s3 = s3Client(region)) {
            put(s3, env("MAXIMIZER_INPUT_BUCKET"), category + "/" + filename, audioPath);
            waitForResult(s3, region, env("MAXIMIZER_RESULTS_BUCKET"), category + "/" + stem + "." + ext);
        }
    }

    static String jsonField(String json, String field) throws Exception {
        try {
            JsonNode notification = JSON.readTree(json);
            if (notification == null || !notification.isObject()) throw new IllegalArgumentException();
            return notification.path(field).asText(null);
        } catch (Exception error) {
            throw new IllegalStateException("Invalid result notification; no queue message was deleted");
        }
    }
}
