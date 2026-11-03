import org.junit.*;
import static org.junit.Assert.*;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import java.io.*;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

public class ResultOnlyTest {
    @BeforeClass public static void noNetwork() { OfflineNetwork.install(); }
    @AfterClass public static void restoreNetwork() { OfflineNetwork.restore(); }
    final byte[] result = "{\"synthetic\":\"RESULT_BODY_MUST_NOT_BE_LOGGED\"}".getBytes(StandardCharsets.UTF_8);
    final byte[] previous = "previous local result".getBytes(StandardCharsets.UTF_8);
    UploadAndFetch.Clients original;
    String ucid; Path out; S3Client reader;
    int heads, gets, closes, factories, missingHeads, missingGets, headStatus;
    String headCode; boolean networkFailure, partialDownloadFailure, checkPrivateMode, oldMustSurviveFirstPoll;
    long clock; final List<Long> sleeps = new ArrayList<>();
    final List<String> operations = new ArrayList<>();

    @Before public void setup() throws Exception {
        ucid = "resultonly" + UUID.randomUUID().toString().replace("-", "");
        out = Path.of(ucid + ".json").toAbsolutePath(); original = UploadAndFetch.clients;
        reader = (S3Client) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{S3Client.class}, (proxy, method, args) -> {
            switch (method.getName()) {
                case "headObject": {
                    heads++; operations.add("head");
                    HeadObjectRequest request = (HeadObjectRequest) args[0];
                    assertEquals("demo-results", request.bucket()); assertEquals("category_a/" + ucid + ".json", request.key());
                    assertTrue(request.overrideConfiguration().get().apiCallTimeout().get().toMillis() <= 30000);
                    if (networkFailure) throw SdkClientException.create("synthetic network failure");
                    if (headStatus != 0 || headCode != null) throw error(headStatus, headCode);
                    if (heads <= missingHeads) throw error(404, "NoSuchKey");
                    return HeadObjectResponse.builder().contentLength((long) result.length).build();
                }
                case "getObject": {
                    gets++; operations.add("get");
                    GetObjectRequest request = (GetObjectRequest) args[0];
                    assertEquals("demo-results", request.bucket()); assertEquals("category_a/" + ucid + ".json", request.key());
                    assertTrue(request.overrideConfiguration().get().apiCallTimeout().get().toMillis() <= 30000);
                    if (checkPrivateMode && Files.getFileStore(out.getParent()).supportsFileAttributeView("posix")) {
                        try (var entries = Files.list(out.getParent())) {
                            Path temporary = entries.filter(p -> p.getFileName().toString().startsWith(".maximizer-result-")).findFirst().orElseThrow();
                            assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(temporary));
                        }
                    }
                    if (gets <= missingGets) throw error(404, "NoSuchKey");
                    @SuppressWarnings("unchecked") ResponseTransformer<GetObjectResponse,Object> transformer = (ResponseTransformer<GetObjectResponse,Object>) args[1];
                    if (partialDownloadFailure) {
                        InputStream broken = new InputStream() {
                            boolean read;
                            public int read() throws IOException { if (!read) { read=true; return 123; } throw new IOException("synthetic interrupted download"); }
                        };
                        return transformer.transform(GetObjectResponse.builder().build(), AbortableInputStream.create(broken));
                    }
                    return transformer.transform(GetObjectResponse.builder().contentLength((long) result.length).build(),
                        AbortableInputStream.create(new ByteArrayInputStream(result)));
                }
                case "close": closes++; return null;
                default: throw new AssertionError("Result-only forbids S3 operation: " + method.getName());
            }
        });
        UploadAndFetch.clients = new UploadAndFetch.Clients() {
            String environment(String name) {
                switch (name) {
                    case "MAXIMIZER_AWS_REGION": return "eu-central-1";
                    case "MAXIMIZER_RESULTS_BUCKET": return "demo-results";
                    case "MAXIMIZER_AWS_ACCESS_KEY_ID": return "synthetic-read-key";
                    case "MAXIMIZER_AWS_SECRET_ACCESS_KEY": return "synthetic-read-secret";
                    default: throw new AssertionError("Result-only must ignore environment setting: " + name);
                }
            }
            S3Client resultS3(String region) { factories++; assertEquals("eu-central-1", region); return reader; }
            S3Client s3(String region) { throw new AssertionError("Upload client must not be constructed"); }
            SqsClient sqs(String region) { throw new AssertionError("SQS client must not be constructed"); }
            void admittedUpload(Path p, String b, String k, String f) { throw new AssertionError("No upload/Lambda invocation permitted"); }
            long resultTimeMillis() { return clock; }
            void resultPollSleep(long milliseconds) throws InterruptedException {
                assertTrue(milliseconds > 0 && milliseconds <= 15000);
                if (oldMustSurviveFirstPoll) {
                    try { assertArrayEquals(previous, Files.readAllBytes(out)); assertNoTemporary(); }
                    catch (IOException error) { throw new AssertionError(error); }
                }
                sleeps.add(milliseconds); clock += milliseconds;
            }
        };
    }
    @After public void cleanup() throws Exception { UploadAndFetch.clients=original; Files.deleteIfExists(out); }
    static S3Exception error(int status, String code) {
        S3Exception.Builder builder=S3Exception.builder();
        builder.statusCode(status);builder.message("PRIVATE_RESPONSE_OR_URL_SENTINEL");
        if (code != null) builder.awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build());
        return (S3Exception) builder.build();
    }
    void run() throws Exception { UploadAndFetch.main(new String[]{"--result-only", "category_a", ucid}); }
    void assertNoTemporary() throws IOException {
        try (var entries=Files.list(out.getParent())) { assertFalse(entries.anyMatch(p->p.getFileName().toString().startsWith(".maximizer-result-"))); }
    }
    @Test public void actualCliNeedsNoFilesInputQueueOrFunctionAndLogsNoBody() throws Exception {
        checkPrivateMode=true; ByteArrayOutputStream printed=new ByteArrayOutputStream(); PrintStream old=System.out;
        try { System.setOut(new PrintStream(printed,true,"UTF-8")); run(); } finally { System.setOut(old); }
        assertArrayEquals(result, Files.readAllBytes(out)); assertEquals(List.of("head","get"),operations);
        assertEquals(1,factories); assertEquals(1,closes); assertTrue(sleeps.isEmpty());
        assertFalse(printed.toString("UTF-8").contains("RESULT_BODY_MUST_NOT_BE_LOGGED")); assertNoTemporary();
        if (Files.getFileStore(out.getParent()).supportsFileAttributeView("posix")) assertEquals(PosixFilePermissions.fromString("rw-------"),Files.getPosixFilePermissions(out));
    }
    @Test public void pending404PollsThenDownloadsExactResult() throws Exception {
        missingHeads=1;run();assertEquals(List.of("head","head","get"),operations);assertEquals(List.of(15000L),sleeps);assertArrayEquals(result,Files.readAllBytes(out));
    }
    @Test public void get404RaceRetriesWithoutRemovingPreviousLocalResult() throws Exception {
        Files.write(out,previous);missingGets=1;oldMustSurviveFirstPoll=true;run();assertEquals(2,heads);assertEquals(2,gets);assertEquals(List.of(15000L),sleeps);assertArrayEquals(result,Files.readAllBytes(out));assertNoTemporary();
    }
    @Test public void http403FailsImmediatelyEvenWithMisleadingNotFoundCode() throws Exception {
        Files.write(out,previous);headStatus=403;headCode="NotFound";
        try { run();fail("403 must fail immediately"); } catch(IllegalStateException expected){assertTrue(expected.getMessage().contains("HTTP 403"));assertNull(expected.getCause());assertFalse(expected.toString().contains("PRIVATE_RESPONSE_OR_URL_SENTINEL"));assertEquals(0,expected.getSuppressed().length);}
        assertEquals(1,heads);assertEquals(0,gets);assertTrue(sleeps.isEmpty());assertArrayEquals(previous,Files.readAllBytes(out));assertNoTemporary();
    }
    @Test public void networkFailureDoesNotBecomeFifteenMinutePolling() throws Exception {
        networkFailure=true;try { run();fail("Network error must escape"); } catch(IllegalStateException expected){assertTrue(expected.getMessage().contains("Result retrieval failed"));assertNull(expected.getCause());assertFalse(expected.toString().contains("synthetic network"));}
        assertEquals(1,heads);assertEquals(0,gets);assertTrue(sleeps.isEmpty());assertFalse(Files.exists(out));
    }
    @Test public void unexpectedServerErrorDoesNotBecomePolling() throws Exception {
        headStatus=503;headCode="NoSuchKey";try {run();fail("Unexpected server error must escape");}catch(IllegalStateException expected){assertTrue(expected.getMessage().contains("HTTP 503"));assertNull(expected.getCause());assertFalse(expected.toString().contains("PRIVATE_RESPONSE_OR_URL_SENTINEL"));}
        assertEquals(1,heads);assertTrue(sleeps.isEmpty());
    }
    @Test public void failedPartialDownloadPreservesExistingFileAndRemovesTemporary() throws Exception {
        Files.write(out,previous);partialDownloadFailure=true;
        try{run();fail("Interrupted download must fail");}catch(IllegalStateException expected){assertNull(expected.getCause());assertFalse(expected.toString().contains("synthetic interrupted download"));assertArrayEquals(previous,Files.readAllBytes(out));}
        assertEquals(1,heads);assertEquals(1,gets);assertTrue(sleeps.isEmpty());assertNoTemporary();
    }
    @Test public void onlyKnownNotFoundCodesArePendingWithoutHttpStatus() {
        for(String code:List.of("404","NoSuchKey","NotFound")) assertTrue(UploadAndFetch.resultNotFound(error(0,code)));
        assertFalse(UploadAndFetch.resultNotFound(error(0,"AccessDenied")));assertFalse(UploadAndFetch.resultNotFound(error(0,null)));
    }
    @Test public void notFoundPollingStopsAtFifteenMinutes() throws Exception {
        missingHeads=1000;
        try{run();fail("Missing result must time out");}catch(IllegalStateException expected){assertTrue(expected.getMessage().contains("timed out"));}
        assertEquals(900000L,clock);assertEquals(60,heads);assertEquals(0,gets);assertEquals(60,sleeps.size());assertEquals(1,closes);assertFalse(Files.exists(out));
    }
    @SuppressWarnings("removal") void invalid(String... args) throws Exception {
        SecurityManager old=System.getSecurityManager();
        System.setSecurityManager(new SecurityManager(){
            public void checkPermission(java.security.Permission p){}
            public void checkConnect(String h,int p){throw new AssertionError("Offline forbids network");}
            public void checkExit(int code){throw new SecurityException("expected-exit-"+code);}
        });
        try{UploadAndFetch.main(args);fail("Invalid arguments must fail");}
        catch(SecurityException expected){assertEquals("expected-exit-2",expected.getMessage());}
        finally{System.setSecurityManager(old);}
        assertEquals(0,factories);assertEquals(0,heads);assertEquals(0,gets);
    }
    @Test public void invalidCategoryBeforeAnySdkOrEnvironmentAccess() throws Exception { invalid("--result-only","../category",ucid); }
    @Test public void invalidUcidBeforeAnySdkOrEnvironmentAccess() throws Exception {
        for(String id:List.of("", "a/b", "a__b", "call-1", "a\\b", "c:demo", "a\nb", "a\u007fb"))invalid("--result-only","category_a",id);
    }
    @Test public void wrongArgumentCountBeforeAnySdkOrEnvironmentAccess() throws Exception { invalid("--result-only","category_a");invalid("--result-only","category_a",ucid,"extra"); }
}
