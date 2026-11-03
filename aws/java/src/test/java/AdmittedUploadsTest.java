import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.core.SdkBytes;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;

public class AdmittedUploadsTest {
    @org.junit.BeforeClass public static void prohibitNetwork() { OfflineNetwork.install(); }
    @org.junit.AfterClass public static void restoreNetworkGuard() { OfflineNetwork.restore(); }
    static final String ETAG = "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"";
    interface Action { void run() throws Exception; }
    static IllegalStateException refused(Action action) throws Exception {
        try { action.run(); fail("Expected fail-closed refusal"); return null; }
        catch (IllegalStateException expected) { return expected; }
    }
    static ObjectNode reply(int status, String code, int delay) {
        return UploadAndFetch.JSON.createObjectNode().put("status", status).put("error", code).put("retry_after", delay);
    }
    static final class Harness {
        final List<JsonNode> events = new ArrayList<>();
        final List<byte[]> bytes = new ArrayList<>();
        final List<Long> waits = new ArrayList<>();
        final List<String> requests = new ArrayList<>();
        Path file; String destination = "https://demo-bucket.s3.eu-central-1.amazonaws.com/owned-part";
        boolean completed; Runnable beforeStart; int failPuts; int putStatus = 200;
        Harness(byte[] content) throws Exception { file = Files.createTempFile("synthetic-upload-", ".mp3"); Files.write(file, content); }
        JsonNode invoke(JsonNode event) throws Exception {
            events.add(event.deepCopy());
            if (event.path("operation").asText().equals("complete")) return reply(200,"",0).put("completed",true);
            if (beforeStart != null) beforeStart.run();
            ObjectNode response = reply(200,"",0).put("request_id",event.path("request_id").textValue());
            if (completed) return response.put("completed",true);
            long size = event.path("size").longValue();
            com.fasterxml.jackson.databind.node.ArrayNode parts = response.putArray("parts");
            for (int i = 0; size > 0; i++) {
                int length = (int)Math.min(size, AdmittedUploads.PART_BYTES); size -= length;
                parts.addObject().put("part_number",i + 1).put("size",length).put("url",destination);
            }
            return response;
        }
        AdmittedUploads adapter() {
            return new AdmittedUploads("eu-central-1", this::invoke, (uri,data) -> {
                requests.add(uri.toString());
                if (failPuts-- > 0) throw new IOException("private-signed-url-must-not-escape");
                bytes.add(data.clone()); return new AdmittedUploads.PartResponse(putStatus, ETAG);
            }, waits::add);
        }
        void upload() throws Exception { adapter().upload(file,"demo-bucket","category_a__demo.mp3"); }
        void close() throws Exception { Files.deleteIfExists(file); }
    }
    @Test public void transferAndCompletionUseOneStableIdentity() throws Exception {
        Harness h = new Harness(new byte[]{0, (byte)255, 10});
        try { h.upload(); assertEquals(2,h.events.size());
            assertEquals("start",h.events.get(0).path("operation").textValue());
            assertEquals("complete",h.events.get(1).path("operation").textValue());
            assertEquals(h.events.get(0).path("request_id"),h.events.get(1).path("request_id"));
            assertArrayEquals(Files.readAllBytes(h.file),h.bytes.get(0));
            assertEquals(ETAG,h.events.get(1).path("etags").get(0).textValue());
        } finally { h.close(); }
    }
    @Test public void crossLanguageUtf8AndBinaryIdentityVector() throws Exception {
        byte[] bytes = new byte[]{0,(byte)255,(byte)0xc3,(byte)0xa9,10,13,(byte)128};
        Path file = Files.createTempFile("synthetic-vector-", ".mp3");
        try { Files.write(file,bytes);
            assertEquals("f935b14289f025bdcd72143ee54ba9dd",AdmittedUploads.hex(AdmittedUploads.hashFile(file,
                AdmittedUploads.identityPrefix("demo-bucket","category_a/category_a__דמו-é.mp3",bytes.length))).substring(0,32));
        } finally { Files.delete(file); }
    }
    @Test public void multipartBoundaryAndIdentityMatchPython() throws Exception {
        byte[] data = new byte[AdmittedUploads.PART_BYTES + 2]; Arrays.fill(data,(byte)'A'); data[data.length-2]=0; data[data.length-1]=(byte)255;
        Harness h = new Harness(data);
        try { AdmittedUploads adapter=h.adapter(); adapter.upload(h.file,"demo-bucket","category_a/category_a__boundary.mp3");
            assertEquals("9e0f8223d2660aecbe1af7e2c1536979",h.events.get(0).path("request_id").textValue());
            assertEquals(2,h.bytes.size()); assertEquals(AdmittedUploads.PART_BYTES,h.bytes.get(0).length); assertEquals(2,h.bytes.get(1).length);
        } finally { h.close(); }
    }
    @Test public void completedReplayNeverTransfersOrCompletesAgain() throws Exception {
        Harness h = new Harness(new byte[]{1,2}); h.completed=true;
        try { h.upload(); assertEquals(1,h.events.size()); assertTrue(h.bytes.isEmpty()); } finally { h.close(); }
    }
    @Test public void sameBytesReuseIdentityChangedBytesChangeIt() throws Exception {
        Harness h=new Harness(new byte[]{1,2}); h.completed=true;
        try { h.upload(); h.upload(); Files.write(h.file,new byte[]{2,1}); h.upload();
            assertEquals(h.events.get(0).path("request_id"),h.events.get(1).path("request_id"));
            assertNotEquals(h.events.get(0).path("request_id"),h.events.get(2).path("request_id"));
        } finally { h.close(); }
    }
    @Test public void sameSizeMutationCannotCompleteOriginalIdentity() throws Exception {
        Harness h=new Harness(new byte[]{1,2});
        h.beforeStart=() -> { try { Files.write(h.file,new byte[]{3,4}); } catch(IOException e) { throw new RuntimeException(e); } };
        try { assertTrue(refused(h::upload).getMessage().contains("completion was not attempted")); assertEquals(1,h.events.size()); }
        finally { h.close(); }
    }
    @Test public void everyDestinationIsValidatedBeforeAnyPut() throws Exception {
        for(String bad:List.of("http://demo-bucket.s3.eu-central-1.amazonaws.com/p", "https://example.invalid/p",
                "https://other-bucket.s3.eu-central-1.amazonaws.com/p","https://demo-bucket.s3.us-east-1.amazonaws.com/p",
                "https://demo-bucket.s3.eu-central-1.amazonaws.com:444/p","https://user@demo-bucket.s3.eu-central-1.amazonaws.com/p")) {
            Harness h=new Harness(new byte[]{1}); h.destination=bad;
            try { assertEquals("Invalid reserved upload destination",refused(h::upload).getMessage()); assertTrue(h.requests.isEmpty()); }
            finally { h.close(); }
        }
    }
    @Test public void laterInvalidPartPreventsEvenFirstPartUpload() throws Exception {
        Harness h=new Harness(new byte[]{1,2});
        AdmittedUploads a=new AdmittedUploads("eu-central-1", event -> {
            ObjectNode result=reply(200,"",0).put("request_id",event.path("request_id").asText());
            result.putArray("parts").addObject().put("part_number",1).put("size",1).put("url",h.destination);
            ((com.fasterxml.jackson.databind.node.ArrayNode)result.get("parts")).addObject().put("part_number",2).put("size",1).put("url","https://example.invalid/p");
            return result;
        },(uri,data)->{fail("No PUT before entire plan validates");return null;},n->{});
        try { refused(()->a.upload(h.file,"demo-bucket","category_a__demo.mp3")); } finally { h.close(); }
    }
    @Test public void wrongTokenSizesAndNonIntegerPartsFailClosed() throws Exception {
        Harness h=new Harness(new byte[]{1});
        try { for(String variant:List.of("token","size","boolean","empty")) {
            AdmittedUploads a=new AdmittedUploads("eu-central-1",event->{
                ObjectNode result=(ObjectNode)h.invoke(event);
                if(variant.equals("token"))result.put("request_id","different");
                if(variant.equals("size"))((ObjectNode)result.path("parts").get(0)).put("size",2);
                if(variant.equals("boolean"))((ObjectNode)result.path("parts").get(0)).put("size",true);
                if(variant.equals("empty"))result.putArray("parts");
                return result;
            },(uri,data)->{fail("Invalid plan sent bytes");return null;},n->{});
            refused(()->a.upload(h.file,"demo-bucket","category_a__demo.mp3"));
        } } finally {h.close();}
    }
    @Test public void transientAdmissionRetriesIdenticalPayloadOnlyTwice() throws Exception {
        List<String> events=new ArrayList<>(); List<Long> waits=new ArrayList<>();
        AdmittedUploads a=new AdmittedUploads("eu-central-1",event->{events.add(event.toString());
            if(events.size()==1)return reply(503,"admission_unavailable",5);
            if(events.size()==2)return reply(409,"request_in_progress",2);
            return reply(200,"",0).put("completed",true);
        },(uri,data)->{fail("No PUT for completed reply");return null;},waits::add);
        Path file=Files.createTempFile("synthetic-retry-",".mp3");
        try { Files.write(file,new byte[]{1});a.upload(file,"demo-bucket","category_a__demo.mp3");
            assertEquals(3,events.size());assertEquals(events.get(0),events.get(1));assertEquals(events.get(1),events.get(2));
            assertEquals(List.of(5000L,2000L),waits);
        } finally {Files.delete(file);}
    }
    @Test public void quotaAndInvalidRetryDoNotRetryOrLeakResponse() throws Exception {
        for(int status:List.of(429,403,503)) {
            List<JsonNode> events=new ArrayList<>();
            AdmittedUploads a=new AdmittedUploads("eu-central-1",event->{events.add(event);
                return reply(status,"private-url-or-payload",60).put("url","private-url-or-payload");
            },(uri,data)->{fail("Rejected admission sent bytes");return null;},n->{fail("Unexpected retry");});
            Path file=Files.createTempFile("synthetic-refusal-",".mp3");
            try {Files.write(file,new byte[]{1});String message=refused(()->a.upload(file,"demo-bucket","category_a__demo.mp3")).getMessage();
                assertFalse(message.contains("private-url"));assertEquals(1,events.size());
            } finally {Files.delete(file);}
        }
    }
    @Test public void repeated503HasOnlyThreeAttempts() throws Exception {
        int[] attempts={0};List<Long> waits=new ArrayList<>();
        AdmittedUploads a=new AdmittedUploads("eu-central-1",event->{attempts[0]++;return reply(503,"admission_unavailable",5);},(uri,data)->null,waits::add);
        refused(()->a.invoke(reply(200,"",0)));assertEquals(3,attempts[0]);assertEquals(2,waits.size());
    }
    @Test public void partNetworkRetriesKeepSameUriAndBytes() throws Exception {
        Harness h=new Harness(new byte[]{1,2});h.failPuts=2;
        try {h.upload();assertEquals(3,h.requests.size());assertEquals(1,h.requests.stream().distinct().count());assertEquals(List.of(1000L,2000L),h.waits);}
        finally {h.close();}
    }
    @Test public void part403DoesNotRetryAndPart503IsBounded() throws Exception {
        for(int status:List.of(403,503)) {Harness h=new Harness(new byte[]{1});h.putStatus=status;
            try {refused(h::upload);assertEquals(status==403?1:3,h.requests.size());assertEquals(1,h.events.size());}
            finally {h.close();}
        }
    }
    @Test public void exhaustedNetworkErrorDoesNotExposeSignedUrl() throws Exception {
        Harness h=new Harness(new byte[]{1});h.failPuts=3;
        try {assertFalse(refused(h::upload).getMessage().contains("private"));assertEquals(3,h.requests.size());assertEquals(1,h.events.size());}
        finally {h.close();}
    }
    @Test public void actualJdkPutRequestHasSignedLengthAndBoundedTimeout() throws Exception {
        byte[] data = new byte[]{0,(byte)255,10};
        java.net.http.HttpRequest request = AdmittedUploads.partRequest(
            URI.create("https://demo-bucket.s3.eu-central-1.amazonaws.com/owned"),data);
        assertEquals("PUT",request.method());assertEquals(3,request.bodyPublisher().get().contentLength());
        assertEquals(java.time.Duration.ofSeconds(45),request.timeout().get());
        assertEquals(java.net.http.HttpClient.Redirect.NEVER,new AdmittedUploads.HttpPartWriter().client.followRedirects());
    }
    @Test public void sdkInvocationUsesConfiguredFunctionAndSafeRuntimeError() throws Exception {
        List<InvokeRequest> requests=new ArrayList<>();
        LambdaClient client=(LambdaClient)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{LambdaClient.class},(proxy,method,args)->{
            if(method.getName().equals("invoke")) {requests.add((InvokeRequest)args[0]);return InvokeResponse.builder()
                .functionError("Unhandled").payload(SdkBytes.fromUtf8String("private-error-body")).build();}
            throw new AssertionError("Unexpected SDK call");
        });
        AdmittedUploads.SdkInvoker invoker=new AdmittedUploads.SdkInvoker(client,"configured-upload");
        assertFalse(refused(()->invoker.invoke(reply(200,"",0))).getMessage().contains("private"));
        assertEquals("configured-upload",requests.get(0).functionName());assertEquals(InvocationType.REQUEST_RESPONSE,requests.get(0).invocationType());
        assertEquals(reply(200,"",0),UploadAndFetch.JSON.readTree(requests.get(0).payload().asByteArray()));
    }
}
