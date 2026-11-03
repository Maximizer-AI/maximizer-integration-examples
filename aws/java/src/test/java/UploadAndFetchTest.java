import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.Assert.*;

public class UploadAndFetchTest {
    @org.junit.BeforeClass public static void prohibitNetwork() { OfflineNetwork.install(); }
    @org.junit.AfterClass public static void restoreNetworkGuard() { OfflineNetwork.restore(); }
    final byte[] resultBytes="{\"status\":\"synthetic-complete\"}".getBytes(StandardCharsets.UTF_8);
    final Map<String,String> environment=new HashMap<>();
    final List<String> operations=new ArrayList<>();
    final List<JsonNode> invocations=new ArrayList<>();
    final List<byte[]> transferred=new ArrayList<>();
    final List<String> directKeys=new ArrayList<>();
    int heads,receives,deletes,gets; boolean firstHeadMissing,forbidQueue; int headError;
    List<Message> messages=new ArrayList<>();
    Path directory; String ucid; UploadAndFetch.Clients original;
    S3Client s3; SqsClient sqs; LambdaClient lambda;

    @Before public void setup() throws Exception {
        directory=Files.createTempDirectory("synthetic-java-cli-");ucid="synthetic"+UUID.randomUUID().toString().replace("-","");
        environment.put("MAXIMIZER_AWS_REGION","eu-central-1");environment.put("MAXIMIZER_INPUT_BUCKET","demo-bucket");
        environment.put("MAXIMIZER_RESULTS_BUCKET","demo-results");environment.put("MAXIMIZER_UPLOAD_FUNCTION","configured-upload");
        environment.put("MAXIMIZER_RESULTS_QUEUE_URL","https://example.invalid/owned-queue");
        s3=(S3Client)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{S3Client.class},(proxy,method,args)->{
            switch(method.getName()) {
                case "headObject": heads++;operations.add("head");
                    if(headError!=0 || (firstHeadMissing && heads==1))throw S3Exception.builder().statusCode(headError==0?404:headError).build();
                    return HeadObjectResponse.builder().contentLength((long)resultBytes.length).build();
                case "getObject": gets++;operations.add("download");
                    @SuppressWarnings("unchecked") ResponseTransformer<GetObjectResponse,Object> transformer=(ResponseTransformer<GetObjectResponse,Object>)args[1];
                    return transformer.transform(GetObjectResponse.builder().contentLength((long)resultBytes.length).build(),
                        AbortableInputStream.create(new ByteArrayInputStream(resultBytes)));
                case "putObject":directKeys.add(((PutObjectRequest)args[0]).key());operations.add("direct-put");return PutObjectResponse.builder().build();
                case "close":return null;
                default:throw new AssertionError("Unexpected S3 operation: "+method.getName());
            }
        });
        sqs=(SqsClient)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{SqsClient.class},(proxy,method,args)->{
            switch(method.getName()) {
                case "receiveMessage":
                    if(forbidQueue || receives>0)throw new AssertionError("No extra/shared queue receive permitted");
                    receives++;operations.add("receive");return ReceiveMessageResponse.builder().messages(messages).build();
                case "deleteMessage":deletes++;operations.add("delete");return DeleteMessageResponse.builder().build();
                case "close":return null;
                default:throw new AssertionError("Unexpected SQS operation: "+method.getName());
            }
        });
        lambda=(LambdaClient)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{LambdaClient.class},(proxy,method,args)->{
            if(!method.getName().equals("invoke"))throw new AssertionError("Unexpected Lambda operation");
            InvokeRequest request=(InvokeRequest)args[0];assertEquals("configured-upload",request.functionName());
            JsonNode event=UploadAndFetch.JSON.readTree(request.payload().asByteArray());invocations.add(event);operations.add(event.path("operation").asText());
            ObjectNode response=UploadAndFetch.JSON.createObjectNode().put("status",200);
            if(event.path("operation").asText().equals("complete"))response.put("completed",true);
            else if(event.path("key").asText().startsWith("org/"))response.put("status",403).put("error","organization_upload_not_permitted");
            else {response.put("request_id",event.path("request_id").asText());response.putArray("parts").addObject()
                .put("part_number",1).put("size",event.path("size").intValue())
                .put("url","https://demo-bucket.s3.eu-central-1.amazonaws.com/owned-part");}
            return InvokeResponse.builder().payload(SdkBytes.fromUtf8String(response.toString())).build();
        });
        original=UploadAndFetch.clients;
        UploadAndFetch.clients=new UploadAndFetch.Clients() {
            String environment(String name){return environment.get(name);}
            S3Client s3(String region){assertEquals("eu-central-1",region);return s3;}
            SqsClient sqs(String region){return sqs;}
            void admittedUpload(Path path,String bucket,String key,String function) throws Exception {
                new AdmittedUploads("eu-central-1",new AdmittedUploads.SdkInvoker(lambda,function),
                    (uri,bytes)->{transferred.add(bytes.clone());return new AdmittedUploads.PartResponse(200,AdmittedUploadsTest.ETAG);},
                    n->{throw new AssertionError("No retries expected in successful CLI test");}).upload(path,bucket,key);
            }
        };
    }
    @After public void cleanup() throws Exception {
        UploadAndFetch.clients=original;Files.deleteIfExists(Path.of(ucid+".json"));
        try(java.util.stream.Stream<Path> files=Files.list(directory)){for(Path file:(Iterable<Path>)files::iterator)Files.deleteIfExists(file);}
        Files.deleteIfExists(directory);
    }
    Path audio(String name) throws Exception {Path path=directory.resolve(name);Files.write(path,new byte[]{0,(byte)255,1});return path;}
    Path metadata(List<String> names) throws Exception {
        Path path=directory.resolve("category_a__"+ucid+".policy.json");
        ObjectNode value=UploadAndFetch.JSON.createObjectNode().put("ucid",ucid);value.set("audio_files",UploadAndFetch.JSON.valueToTree(names));
        Files.writeString(path,value.toString());return path;
    }
    boolean waitResult(boolean admitted) throws Exception {
        return UploadAndFetch.waitForQueueResult(s3,sqs,"demo-results","category_a/"+ucid+".json",
            "https://example.invalid/owned-queue",admitted,System.currentTimeMillis()+1000);
    }
    @Test public void actualCliUcidAdmitsBothFilesAndDownloadsRealBytes() throws Exception {
        forbidQueue=true;Path audio=audio("input.mp3");Path metadata=metadata(List.of("category_a__"+ucid+".mp3"));
        UploadAndFetch.main(new String[]{"--ucid","category_a",ucid,metadata.toString(),audio.toString()});
        assertEquals(4,invocations.size());assertEquals(List.of("start","complete","start","complete","head","download"),operations);
        assertEquals(2,transferred.size());assertArrayEquals(Files.readAllBytes(audio),transferred.get(0));
        assertArrayEquals(Files.readAllBytes(metadata),transferred.get(1));assertArrayEquals(resultBytes,Files.readAllBytes(Path.of(ucid+".json")));
        assertTrue(directKeys.isEmpty());assertEquals(0,receives);assertEquals(0,deletes);
    }
    @Test public void actualBatchCliKeepsPartOrderAndMetadataNames() throws Exception {
        Path one=audio("one.mp3"),two=audio("two.wav");Path metadata=metadata(List.of("category_a__"+ucid+"-1.mp3","category_a__"+ucid+"-2.wav"));
        UploadAndFetch.main(new String[]{"--ucid","--batch","category_a",ucid,metadata.toString(),one.toString(),two.toString()});
        assertEquals(6,invocations.size());assertEquals("category_a_batch/category_a__"+ucid+"-1.mp3",invocations.get(0).path("key").asText());
        assertEquals("category_a_batch/category_a__"+ucid+"-2.wav",invocations.get(2).path("key").asText());
        assertEquals("category_a_batch/category_a__"+ucid+".policy.json",invocations.get(4).path("key").asText());assertTrue(directKeys.isEmpty());
    }
    @Test public void actualLegacyCliPreservesDirectTransport() throws Exception {
        environment.remove("MAXIMIZER_UPLOAD_FUNCTION");environment.remove("MAXIMIZER_RESULTS_QUEUE_URL");
        Path one=audio("input.mp3"),metadata=metadata(List.of("category_a__"+ucid+".mp3"));
        UploadAndFetch.main(new String[]{"--ucid","category_a",ucid,metadata.toString(),one.toString()});
        assertEquals(List.of("category_a/category_a__"+ucid+".mp3","category_a/category_a__"+ucid+".policy.json"),directKeys);
        assertTrue(invocations.isEmpty());assertTrue(transferred.isEmpty());assertArrayEquals(resultBytes,Files.readAllBytes(Path.of(ucid+".json")));
    }
    @Test public void admittedOrgCliCannotFallBackToDirectS3() throws Exception {
        Path org=directory.resolve("org_2026-01-01.json"),users=directory.resolve("users_2026-01-01.json");Files.writeString(org,"[]");Files.writeString(users,"[]");
        assertTrue(AdmittedUploadsTest.refused(()->UploadAndFetch.main(new String[]{"--org",org.toString(),users.toString()})).getMessage().contains("403"));
        assertEquals(1,invocations.size());assertTrue(directKeys.isEmpty());assertTrue(transferred.isEmpty());
    }
    @Test public void whitespaceFunctionNeverFallsBackToDirectS3() throws Exception {
        Path path=audio("input.mp3");
        assertTrue(AdmittedUploadsTest.refused(()->UploadAndFetch.put(s3,"demo-bucket","category_a__demo.mp3",path," ",
            (file,bucket,key)->{throw new IllegalStateException("Synthetic admission rejection");})).getMessage().contains("admission"));
        assertTrue(directKeys.isEmpty());
    }
    @Test public void malformedNotificationDoesNotExposePayloadOrDeleteMessage() throws Exception {
        messages=List.of(Message.builder().body("PRIVATE_SIGNED_URL_SENTINEL malformed").receiptHandle("owned").build());
        IllegalStateException error=AdmittedUploadsTest.refused(()->waitResult(false));
        assertEquals("Invalid result notification; no queue message was deleted",error.getMessage());
        assertNull(error.getCause());assertEquals(0,deletes);assertEquals(0,gets);
    }
    @Test public void nullableErrorCodeRetainsOriginalS3Failure() throws Exception {
        S3Client denied=(S3Client)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{S3Client.class},(proxy,method,args)->{
            throw S3Exception.builder().statusCode(403).awsErrorDetails(software.amazon.awssdk.awscore.exception.AwsErrorDetails.builder().build()).build();
        });
        try{UploadAndFetch.existingResult(denied,"demo-results","category_a/owned.json",directory.resolve("owned.json"));fail("403 must fail");}
        catch(S3Exception expected){assertEquals(403,expected.statusCode());}
    }
    @Test public void alreadyAvailableResultNeverReceivesOrDeletesMessages() throws Exception {forbidQueue=true;assertTrue(waitResult(true));assertEquals(1,heads);assertEquals(1,gets);assertEquals(0,deletes);}
    @Test public void pendingResultAppearsAfterEmptyPollWithoutAnotherReceive() throws Exception {firstHeadMissing=true;assertTrue(waitResult(true));assertEquals(2,heads);assertEquals(1,receives);assertEquals(1,gets);assertEquals(0,deletes);}
    @Test public void unmatchedMessageIsNotDeletedWhenResultAppears() throws Exception {
        firstHeadMissing=true;messages=List.of(Message.builder().body("{\"results_file_name\":\"category_a/other.json\"}").receiptHandle("unrelated").build());
        assertTrue(waitResult(true));assertEquals(1,receives);assertEquals(2,heads);assertEquals(0,deletes);
    }
    @Test public void head403FailsBeforeConsumingMessages() throws Exception {
        headError=403;forbidQueue=true;try{waitResult(true);fail("403 must fail closed");}catch(S3Exception expected){assertEquals(403,expected.statusCode());}
        assertEquals(0,receives);assertEquals(0,gets);assertEquals(0,deletes);
    }
    @Test public void matchingNotificationIsAcknowledgedOnlyAfterDownload() throws Exception {
        messages=List.of(Message.builder().body("{\"results_file_name\":\"category_a/"+ucid+".json\"}").receiptHandle("owned").build());
        assertTrue(waitResult(false));assertEquals(List.of("receive","download","delete"),operations);assertEquals(0,heads);
    }
    @Test public void failedDownloadDoesNotAcknowledgeNotification() throws Exception {
        messages=List.of(Message.builder().body("{\"results_file_name\":\"category_a/"+ucid+".json\"}").receiptHandle("owned").build());
        S3Client failed=(S3Client)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{S3Client.class},(proxy,method,args)->{throw new IOException("synthetic download failure");});
        try{UploadAndFetch.waitForQueueResult(failed,sqs,"demo-results","category_a/"+ucid+".json","https://example.invalid/owned-queue",false,System.currentTimeMillis()+1000);fail("Download should fail");}
        catch(Exception expected){assertEquals(0,deletes);}
    }
}
