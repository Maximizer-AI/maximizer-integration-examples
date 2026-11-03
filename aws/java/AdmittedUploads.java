// Optional pre-storage recording transport. Result reads continue through S3/SQS.
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

final class AdmittedUploads {
    static final int PART_BYTES = 8 * 1024 * 1024;
    interface Invoker { JsonNode invoke(JsonNode event) throws Exception; }
    interface PartWriter { PartResponse put(URI uri, byte[] data) throws IOException, InterruptedException; }
    interface Sleeper { void sleep(long milliseconds) throws InterruptedException; }
    static final class PartResponse {
        final int status; final String etag;
        PartResponse(int status, String etag) { this.status = status; this.etag = etag; }
    }
    static final class Part {
        final URI uri; final int length;
        Part(URI uri, int length) { this.uri = uri; this.length = length; }
    }
    static final class SdkInvoker implements Invoker {
        final LambdaClient client; final String function;
        SdkInvoker(LambdaClient client, String function) { this.client = client; this.function = function; }
        public JsonNode invoke(JsonNode event) throws Exception {
            InvokeResponse response;
            try {
                response = client.invoke(InvokeRequest.builder().functionName(function)
                    .invocationType(InvocationType.REQUEST_RESPONSE)
                    .payload(SdkBytes.fromUtf8String(event.toString())).build());
            } catch (RuntimeException error) {
                throw new IllegalStateException("Upload admission request failed; no direct upload was attempted");
            }
            if (response.functionError() != null && !response.functionError().isEmpty())
                throw new IllegalStateException("Upload admission service failed; no direct upload was attempted");
            try {
                JsonNode value = UploadAndFetch.JSON.readTree(response.payload().asByteArray());
                if (value == null || !value.isObject()) throw new IOException();
                return value;
            } catch (Exception error) {
                throw new IllegalStateException("Invalid upload admission response; no direct upload was attempted");
            }
        }
    }
    static final class HttpPartWriter implements PartWriter {
        // Keep default certificate/hostname verification and forbid redirects.
        final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER).build();
        public PartResponse put(URI uri, byte[] data) throws IOException, InterruptedException {
            HttpRequest request = partRequest(uri, data);
            // The fixed byte-array publisher supplies the signed Content-Length.
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            return new PartResponse(response.statusCode(), response.headers().firstValue("ETag").orElse(null));
        }
    }

    static HttpRequest partRequest(URI uri, byte[] data) {
        return HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(45))
            .PUT(HttpRequest.BodyPublishers.ofByteArray(data)).build();
    }

    final String region; final Invoker invoker; final PartWriter writer; final Sleeper sleeper;
    AdmittedUploads(String region, Invoker invoker, PartWriter writer, Sleeper sleeper) {
        this.region = region; this.invoker = invoker; this.writer = writer; this.sleeper = sleeper;
    }
    static void uploadConfigured(Path path, String bucket, String key, String region,
                                 String function, AwsCredentialsProvider credentials) throws Exception {
        try (LambdaClient lambda = LambdaClient.builder().region(Region.of(region))
                .credentialsProvider(credentials).overrideConfiguration(configuration -> configuration
                    .retryPolicy(RetryPolicy.builder(RetryMode.STANDARD).numRetries(5).build())
                    .apiCallAttemptTimeout(Duration.ofSeconds(45)).apiCallTimeout(Duration.ofMinutes(3))).build()) {
            new AdmittedUploads(region, new SdkInvoker(lambda, function), new HttpPartWriter(), Thread::sleep)
                .upload(path, bucket, key);
        }
    }

    static final Set<String> ERROR_CODES = Set.of("admission_unavailable", "request_in_progress",
        "upload_quota_exceeded", "upload_expired", "object_name_already_reserved", "upload_conflict",
        "request_id_conflict", "unsupported_object_name", "invalid_file_size", "invalid_request",
        "invalid_request_id", "unsupported_operation", "organization_upload_not_permitted",
        "unknown_upload", "invalid_upload_parts", "upload_size_or_etag_mismatch");
    JsonNode invoke(JsonNode event) throws Exception {
        for (int attempt = 0; attempt < 3; attempt++) {
            JsonNode reply = invoker.invoke(event);
            if (reply == null || !reply.isObject() || !reply.path("status").isIntegralNumber()
                    || !reply.path("status").canConvertToInt())
                throw new IllegalStateException("Invalid upload admission response");
            int status = reply.path("status").intValue();
            if (status == 200) return reply;
            String supplied = reply.path("error").asText("");
            String code = ERROR_CODES.contains(supplied) ? supplied : "admission_failed";
            JsonNode retryNode = reply.path("retry_after");
            int retry = retryNode.isIntegralNumber() && retryNode.canConvertToInt() ? retryNode.intValue() : 0;
            if ((status == 503 || (status == 409 && code.equals("request_in_progress")))
                    && attempt < 2 && retry >= 1 && retry <= 10) {
                sleeper.sleep(retry * 1000L);
                continue;
            }
            String guidance = "";
            if (code.equals("upload_expired")) guidance = "; incomplete reservation expired; use a fresh UCID";
            if (code.equals("object_name_already_reserved")) guidance = "; filename is reserved; changed or replacement files need a fresh UCID";
            if (code.equals("request_in_progress")) guidance = "; retry unchanged files while valid; use a fresh UCID if the incomplete reservation cannot be recovered";
            if (code.equals("upload_conflict")) guidance = "; contact Maximizer with the UCID before replacing this upload";
            throw new IllegalStateException("Upload admission rejected: " + status + " " + code
                + (retry > 0 ? "; retry after " + retry + "s" : "") + guidance);
        }
        throw new IllegalStateException("Upload admission retry limit reached");
    }

    String put(URI uri, byte[] data) throws Exception {
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                PartResponse response = writer.put(uri, data);
                if (response.status >= 200 && response.status < 300) {
                    if (response.etag == null || response.etag.isBlank())
                        throw new IllegalStateException("Reserved upload part returned no ETag; completion was not attempted");
                    return response.etag;
                }
                if (response.status < 500 || attempt == 2)
                    throw new IllegalStateException("Reserved upload part failed: HTTP " + response.status
                        + "; start no replacement upload automatically");
            } catch (IOException error) {
                if (attempt == 2)
                    throw new IllegalStateException("Reserved upload part failed after three network attempts");
            }
            sleeper.sleep((attempt + 1) * 1000L);
        }
        throw new IllegalStateException("Reserved upload part retry limit reached");
    }

    static byte[] identityPrefix(String bucket, String key, long size) {
        return ("maximizer-upload-v1\0" + bucket + "\0" + key + "\0" + size + "\0")
            .getBytes(StandardCharsets.UTF_8);
    }
    static MessageDigest digest(byte[] prefix) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256"); digest.update(prefix); return digest;
    }
    static String hex(byte[] value) {
        StringBuilder text = new StringBuilder();
        for (byte part : value) text.append(String.format("%02x", part & 255));
        return text.toString();
    }
    static byte[] hashFile(Path path, byte[] prefix) throws Exception {
        MessageDigest hash = digest(prefix);
        try (InputStream stream = Files.newInputStream(path)) {
            byte[] buffer = new byte[PART_BYTES]; int count;
            while ((count = stream.read(buffer)) != -1) hash.update(buffer, 0, count);
        }
        return hash.digest();
    }
    List<Part> validatePlan(JsonNode reply, String token, String bucket, long size) {
        JsonNode parts = reply.path("parts");
        if (!token.equals(reply.path("request_id").asText(null)) || !parts.isArray() || parts.isEmpty())
            throw new IllegalStateException("Invalid reserved upload plan");
        List<Part> plan = new ArrayList<>(); long total = 0;
        for (JsonNode part : parts) {
            if (!part.path("part_number").isIntegralNumber() || !part.path("part_number").canConvertToInt()
                    || part.path("part_number").intValue() != plan.size() + 1
                    || !part.path("size").isIntegralNumber() || !part.path("size").canConvertToInt())
                throw new IllegalStateException("Invalid reserved upload destination");
            int length = part.path("size").intValue();
            if (length < 1 || length > PART_BYTES || !part.path("url").isTextual())
                throw new IllegalStateException("Invalid reserved upload destination");
            URI uri;
            try { uri = new URI(part.path("url").textValue()); }
            catch (URISyntaxException error) { throw new IllegalStateException("Invalid reserved upload destination"); }
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || !(bucket + ".s3." + region + ".amazonaws.com").equalsIgnoreCase(uri.getHost())
                    || uri.getRawUserInfo() != null || !(uri.getPort() == -1 || uri.getPort() == 443))
                throw new IllegalStateException("Invalid reserved upload destination");
            total += length; plan.add(new Part(uri, length));
        }
        if (total != size) throw new IllegalStateException("Invalid reserved upload plan");
        return plan;
    }
    void upload(Path path, String bucket, String key) throws Exception {
        long size = Files.size(path); byte[] prefix = identityPrefix(bucket, key, size);
        byte[] original = hashFile(path, prefix); String token = hex(original).substring(0, 32);
        ObjectNode start = UploadAndFetch.JSON.createObjectNode().put("operation", "start")
            .put("request_id", token).put("key", key).put("size", size);
        JsonNode reply = invoke(start);
        if (reply.path("completed").isBoolean() && reply.path("completed").booleanValue()) return;
        List<Part> plan = validatePlan(reply, token, bucket, size);
        MessageDigest transferred = digest(prefix); List<String> etags = new ArrayList<>();
        try (InputStream stream = Files.newInputStream(path)) {
            for (Part part : plan) {
                byte[] bytes = stream.readNBytes(part.length);
                if (bytes.length != part.length) throw new IllegalStateException("Local file changed during upload");
                transferred.update(bytes); etags.add(put(part.uri, bytes));
            }
            if (stream.read() != -1) throw new IllegalStateException("Local file changed during upload");
        }
        if (!Arrays.equals(original, transferred.digest()))
            throw new IllegalStateException("Local file changed during upload; completion was not attempted");
        ObjectNode complete = UploadAndFetch.JSON.createObjectNode().put("operation", "complete").put("request_id", token);
        complete.set("etags", UploadAndFetch.JSON.valueToTree(etags)); invoke(complete);
    }
}
