import test, { after, before } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EventEmitter } from "node:events";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { readFile } from "node:fs/promises";
import { Socket } from "node:net";
import { AdmittedUploads, putPart } from "./admitted_upload.mjs";
import { waitForCompletion } from "./result_waiter.mjs";

// Any accidental real network use fails this entire offline suite.
let originalConnect;
before(() => { originalConnect = Socket.prototype.connect; Socket.prototype.connect = function () { throw new Error("Offline test forbids network"); }; });
after(() => { Socket.prototype.connect = originalConnect; });
const bucket = "demo-bucket", region = "eu-central-1";
const url = `https://${bucket}.s3.${region}.amazonaws.com/reserved-part?private-signature=never-log`;
const etag = '"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"';

async function fixture(t, bytes = Buffer.from("synthetic spoken audio placeholder")) {
  const dir = await mkdtemp(join(tmpdir(), "maximizer-admission-js-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, "synthetic.mp3");
  await writeFile(file, bytes);
  const events = [], puts = [], sleeps = [];
  const response = event => event.operation === "start"
    ? { status: 200, request_id: event.request_id, parts: [{ part_number: 1, size: bytes.length, url }] }
    : { status: 200, completed: true };
  const client = new AdmittedUploads(async event => { events.push(structuredClone(event)); return response(event); }, region,
    { put: async (u, data) => { puts.push({ url: u, data: Buffer.from(data) }); return etag; }, sleep: async ms => { sleeps.push(ms); } });
  return { client, file, events, puts, sleeps, bytes };
}

const vectors = [
  ["ascii", "category_a/category_a__demo.mp3", Buffer.from("synthetic spoken audio placeholder"), "2479a7f92aab944d73bdea64c7eb20bb"],
  ["UTF-8 key and binary", "category_a/category_a__דמו-é.mp3", Buffer.from("00ffc3a90a0d80", "hex"), "f935b14289f025bdcd72143ee54ba9dd"],
  ["UTF-8 file", "category_a/category_a__unicode.json", Buffer.from("7b2273706f6b656e223a22d7a9d79cd795d79d20f09f8ea7227d0a", "hex"), "996874cda7a9b4cb48f01b5799d0cabb"],
  ["8 MiB boundary", "category_a/category_a__boundary.mp3", Buffer.concat([Buffer.alloc(8388608, 65), Buffer.from([0, 255])]), "9e0f8223d2660aecbe1af7e2c1536979"],
];
for (const [name, key, bytes, expected] of vectors) test(`Python-compatible stable identity: ${name}`, async t => {
  const f = await fixture(t, bytes);
  f.client.invokeFunction = async event => { f.events.push(event); return { status: 200, completed: true }; };
  await f.client.uploadFile(f.file, bucket, key);
  assert.equal(f.events[0].request_id, expected);
  assert.equal(f.events[0].size, bytes.length);
  assert.equal(f.puts.length, 0);
});

test("reserved transfer completes once with exact bytes and stable token", async t => {
  const f = await fixture(t);
  await f.client.uploadFile(f.file, bucket, "category_a/demo.mp3");
  assert.deepEqual(f.events.map(e => e.operation), ["start", "complete"]);
  assert.equal(f.events[0].request_id, f.events[1].request_id);
  assert.deepEqual(f.events[1].etags, [etag]);
  assert.deepEqual(f.puts[0].data, f.bytes);
});
test("same file/destination retries reuse identity; changed content changes identity", async t => {
  const f = await fixture(t);
  f.client.invokeFunction = async event => { f.events.push(event); return { status: 200, completed: true }; };
  await f.client.uploadFile(f.file, bucket, "category_a/demo.mp3");
  await f.client.uploadFile(f.file, bucket, "category_a/demo.mp3");
  await writeFile(f.file, Buffer.from("replacement"));
  await f.client.uploadFile(f.file, bucket, "category_a/demo.mp3");
  assert.equal(f.events[0].request_id, f.events[1].request_id);
  assert.notEqual(f.events[0].request_id, f.events[2].request_id);
});
test("changed same-size bytes after hashing never complete old identity", async t => {
  const f = await fixture(t, Buffer.from("old!"));
  const invoke = f.client.invokeFunction;
  f.client.invokeFunction = async event => { const value = await invoke(event); if (event.operation === "start") await writeFile(f.file, "new!"); return value; };
  await assert.rejects(f.client.uploadFile(f.file, bucket, "category_a/demo.mp3"), /Local file changed.*completion was not attempted/);
  assert.deepEqual(f.events.map(e => e.operation), ["start"]);
  assert.equal(f.puts[0].data.toString(), "new!");
});
for (const bytes of [Buffer.from("x"), Buffer.alloc(40)]) test(`changed file length ${bytes.length} never completes`, async t => {
  const f = await fixture(t);
  const invoke = f.client.invokeFunction;
  f.client.invokeFunction = async event => { const value = await invoke(event); if (event.operation === "start") await writeFile(f.file, bytes); return value; };
  await assert.rejects(f.client.uploadFile(f.file, bucket, "category_a/demo.mp3"), /Local file changed/);
  assert.deepEqual(f.events.map(e => e.operation), ["start"]);
});
test("failed part never completes and no direct fallback exists", async t => {
  const f = await fixture(t); f.client.put = async () => { throw new Error("network unavailable"); };
  await assert.rejects(f.client.uploadFile(f.file, bucket, "category_a/demo.mp3"), /network unavailable/);
  assert.deepEqual(f.events.map(e => e.operation), ["start"]);
});
for (const bad of ["http://demo-bucket.s3.eu-central-1.amazonaws.com/p", "https://example.invalid/p", "https://other-bucket.s3.eu-central-1.amazonaws.com/p", "https://demo-bucket.s3.us-east-1.amazonaws.com/p", "https://demo-bucket.s3.eu-central-1.amazonaws.com:444/p", "https://someone@demo-bucket.s3.eu-central-1.amazonaws.com/p"]) test("whole plan rejects unsafe destination " + new URL(bad).host, async t => {
  const f = await fixture(t, Buffer.from("xy"));
  f.client.invokeFunction = async event => ({ status: 200, request_id: event.request_id, parts: [
    { part_number: 1, size: 1, url }, { part_number: 2, size: 1, url: bad }] });
  await assert.rejects(f.client.uploadFile(f.file, bucket, "category_a/demo.mp3"), /Invalid reserved upload destination/);
  assert.equal(f.puts.length, 0); // valid first part must not be sent either
});
for (const mutation of [r => { r.request_id = "wrong"; }, r => { r.parts[0].size++; }, r => { r.parts[0].part_number = 2; }, r => { r.parts[0].size = "34"; }, r => { r.parts = []; }]) test("invalid plan releases no bytes", async t => {
  const f = await fixture(t); const invoke = f.client.invokeFunction;
  f.client.invokeFunction = async e => { const r = await invoke(e); mutation(r); return r; };
  await assert.rejects(f.client.uploadFile(f.file, bucket, "category_a/demo.mp3"), /Invalid reserved upload/);
  assert.equal(f.puts.length, 0);
});
test("503 and in-progress retry same event with bounded advertised delay", async t => {
  const f = await fixture(t); let i = 0;
  f.client.invokeFunction = async event => { f.events.push(event); return [{ status: 503, error: "admission_unavailable", retry_after: 5 }, { status: 409, error: "request_in_progress", retry_after: 2 }, { status: 200, completed: true }][i++]; };
  await f.client.uploadFile(f.file, bucket, "category_a/demo.mp3");
  assert.equal(f.events.length, 3); assert.deepEqual(f.events, [f.events[0], f.events[0], f.events[0]]); assert.deepEqual(f.sleeps, [5000, 2000]);
});
for (const [status, code, retry, attempts] of [[503,"admission_unavailable",5,3], [409,"request_in_progress",2,3], [429,"upload_quota_exceeded",60,1], [503,"admission_unavailable",11,1], [503,"admission_unavailable",true,1], [503,"admission_unavailable",1.5,1]]) test(`retry limit ${status}/${retry}`, async t => {
  const f = await fixture(t); f.client.invokeFunction = async event => { f.events.push(event); return { status, error: code, retry_after: retry }; };
  await assert.rejects(f.client.uploadFile(f.file, bucket, "category_a/demo.mp3"), /Upload admission rejected/);
  assert.equal(f.events.length, attempts); assert.equal(f.sleeps.length, attempts - 1); assert.equal(f.puts.length, 0);
});
for (const [code, guidance] of [["upload_expired", /fresh UCID/], ["object_name_already_reserved", /fresh UCID/], ["upload_conflict", /contact Maximizer with the UCID before replacing/]]) test("safe recovery guidance " + code, async t => {
  const f = await fixture(t); f.client.invokeFunction = async () => ({ status: 409, error: code, url: "secret signed URL" });
  await assert.rejects(f.client.uploadFile(f.file, bucket, "category_a/demo.mp3"), e => guidance.test(e.message) && !e.message.includes("secret"));
});
test("runtime and malformed payload errors are sanitized", async t => {
  const f = await fixture(t);
  for (const value of [null, [], "private body", new Error("https://secret.invalid/credential")]) {
    f.client.invokeFunction = async () => { if (value instanceof Error) throw value; return value; };
    await assert.rejects(f.client.uploadFile(f.file, bucket, "category_a/demo.mp3"), e => e.message === "Upload admission service failed; no direct upload was attempted");
  }
});

function fakeRequest(outcomes, calls) {
  return (u, options, callback) => {
    const req = new EventEmitter(); req.setTimeout = (ms, cb) => { calls.push({ timeout: ms }); req.timeout = cb; };
    req.destroy = error => req.emit("error", error);
    req.end = data => { calls.push({ url: u, options, data }); queueMicrotask(() => {
      const value = outcomes.shift();
      if (value instanceof Error) req.emit("error", value);
      else if (value === "timeout") req.timeout();
      else callback({ statusCode: value.status, headers: { etag: value.etag }, resume() {} });
    }); };
    return req;
  };
}
test("PUT retries exact URL/bytes on 5xx/network with 45s timeout", async () => {
  const calls = [], sleeps = [], data = Buffer.from([0, 255, 128]);
  const result = await putPart(url, data, { request: fakeRequest([{ status: 503 }, new Error("secret TLS details"), { status: 200, etag }], calls), sleep: async ms => sleeps.push(ms) });
  assert.equal(result, etag); assert.deepEqual(sleeps, [1000, 2000]);
  const sent = calls.filter(c => c.data); assert.equal(sent.length, 3);
  for (const c of sent) { assert.equal(c.url, url); assert.deepEqual(c.data, data); assert.deepEqual(c.options, { method: "PUT", headers: { "Content-Length": "3" } }); }
  assert.deepEqual(calls.filter(c => c.timeout).map(c => c.timeout), [45000,45000,45000]);
});
for (const status of [302, 403, 429]) test(`PUT HTTP ${status} does not retry or redirect`, async () => {
  const calls = [], sleeps = [];
  await assert.rejects(putPart(url, Buffer.from("a"), { request: fakeRequest([{ status }], calls), sleep: async ms => sleeps.push(ms) }), e => e.message.includes(`HTTP ${status}`) && !e.message.includes("private-signature"));
  assert.equal(calls.filter(c => c.data).length, 1); assert.deepEqual(sleeps, []);
});
test("PUT timeout retries at most three and suppresses transport secrets", async () => {
  const calls = [];
  await assert.rejects(putPart(url, Buffer.from("a"), { request: fakeRequest(["timeout", "timeout", "timeout"], calls), sleep: async () => {} }), /failed after three network attempts/);
  assert.equal(calls.filter(c => c.data).length, 3);
});
test("PUT missing ETag refuses completion without retry", async () => {
  const calls = [];
  await assert.rejects(putPart(url, Buffer.from("a"), { request: fakeRequest([{ status: 200 }], calls), sleep: async () => {} }), /no valid ETag/);
  assert.equal(calls.filter(c => c.data).length, 1);
});

function resultFixture() {
  const counts = { head:0, download:0, receive:0, remove:0 };
  const options = { resultKey:"category_a/demo.json", queueUrl:"https://example.invalid/owned-queue", admitted:true,
    head:async () => { counts.head++; }, download:async () => { counts.download++; },
    receive:async () => { counts.receive++; return { Messages:[{ Body:JSON.stringify({ results_file_name:"category_a/demo.json" }), ReceiptHandle:"owned" }] }; },
    remove:async () => { counts.remove++; } };
  return { counts, options };
}
const missing = () => Object.assign(new Error("missing"), { name:"NotFound", $metadata:{ httpStatusCode:404 } });
test("completed queue replay downloads without ANY SQS operation", async () => {
  const f = resultFixture(); f.options.receive = f.options.remove = async () => assert.fail("SQS must not be touched");
  await waitForCompletion(f.options); assert.equal(f.counts.download, 1); assert.equal(f.counts.head, 1);
});
test("pending result still downloads/deletes its matching notification", async () => {
  const f = resultFixture(); f.options.head = async () => { throw missing(); };
  await waitForCompletion(f.options); assert.equal(f.counts.receive,1); assert.equal(f.counts.download,1); assert.equal(f.counts.remove,1);
});
for (const messages of [[], [{ Body:JSON.stringify({ results_file_name:"category_a/other.json" }), ReceiptHandle:"foreign" }]]) test("pending result recovered after empty/unmatched poll without second receive", async () => {
  const f = resultFixture(); f.options.head = async () => { if (++f.counts.head === 1) throw missing(); };
  f.options.receive = async () => { assert.equal(++f.counts.receive,1); return { Messages:messages }; };
  await waitForCompletion(f.options); assert.equal(f.counts.head,2); assert.equal(f.counts.download,1); assert.equal(f.counts.remove,0);
});
test("HEAD 403 aborts before consuming any queue message", async () => {
  const f = resultFixture(); f.options.head = async () => { throw Object.assign(new Error("forbidden"), { name:"AccessDenied", $metadata:{ httpStatusCode:403 } }); };
  await assert.rejects(waitForCompletion(f.options), /forbidden/); assert.equal(f.counts.receive,0); assert.equal(f.counts.download,0); assert.equal(f.counts.remove,0);
});
test("legacy queue route never HEADs and deletes only after successful download", async () => {
  const f = resultFixture(); f.options.admitted = false;
  await waitForCompletion(f.options); assert.deepEqual(f.counts,{head:0,download:1,receive:1,remove:1});
});
test("download failure leaves matching message undeleted", async () => {
  const f = resultFixture(); f.options.admitted = false; f.options.download = async () => { throw new Error("download failed"); };
  await assert.rejects(waitForCompletion(f.options), /download failed/); assert.equal(f.counts.remove,0);
});
test("no-queue polling still uses S3 and never SQS", async () => {
  const f = resultFixture(); f.options.queueUrl = undefined;
  await waitForCompletion(f.options); assert.deepEqual(f.counts,{head:1,download:1,receive:0,remove:0});
});

test("legacy no-queue download failure escapes without polling again", async () => {
  const f = resultFixture(); f.options.queueUrl = undefined; f.options.admitted = false;
  f.options.download = async () => { throw new Error("download failed"); };
  f.options.sleep = async () => assert.fail("Download failure must not repoll");
  await assert.rejects(waitForCompletion(f.options), /download failed/);
  assert.equal(f.counts.head,1);
});

// Exercise the published CLI in a fresh process. All SDK calls are replaced at
// module resolution and that process also forbids real network connections.
async function runCli(t, admitted, mode = "success") {
  const dir = await mkdtemp(join(tmpdir(), "maximizer-js-cli-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  await writeFile(join(dir,"demo.mp3"), Buffer.from("synthetic words"));
  await writeFile(join(dir,"loader.mjs"), `export async function resolve(name, context, next) {
    if (name.startsWith("@aws-sdk/")) return { url: new URL("./sdk.mjs", import.meta.url).href, shortCircuit: true };
    return next(name, context);
  }`);
  await writeFile(join(dir,"sdk.mjs"), String.raw`
    import { appendFileSync } from "node:fs";
    import { Socket } from "node:net";
    Socket.prototype.connect = function () { throw new Error("Child offline guard forbids network"); };
    const trace = (op,input) => appendFileSync(process.env.JS_TEST_TRACE,JSON.stringify({op,input})+"\n");
    export class GetObjectCommand { constructor(input) { this.input=input; } }
    export class HeadObjectCommand { constructor(input) { this.input=input; } }
    export class ReceiveMessageCommand { constructor(input) { this.input=input; } }
    export class DeleteMessageCommand { constructor(input) { this.input=input; } }
    export class InvokeCommand { constructor(input) { this.input=input; } }
    export class S3Client { async send(c) { trace(c.constructor.name,c.input);
      if (c instanceof GetObjectCommand) return {Body:{transformToByteArray:async()=>Buffer.from('synthetic result')}};
      return {};
    } }
    export class SQSClient { async send(c) { trace(c.constructor.name,c.input); throw new Error('Unexpected SQS call'); } }
    export class LambdaClient { async send(c) {
      trace(c.constructor.name,{...c.input,Payload:JSON.parse(Buffer.from(c.input.Payload).toString())});
      if (process.env.JS_TEST_MODE==='function-error') return {FunctionError:'Unhandled',Payload:Buffer.from('private-debug-body')};
      if (process.env.JS_TEST_MODE==='malformed') return {Payload:Buffer.from('private malformed response')};
      if (process.env.JS_TEST_MODE==='rejected') return {Payload:Buffer.from(JSON.stringify({status:429,error:'upload_quota_exceeded',url:'private-signed-url'}))};
      return {Payload:Buffer.from(JSON.stringify({status:200,completed:true}))};
    } }
    export class Upload { constructor(options) { this.options=options; } async done() { trace('DirectUpload',{Bucket:this.options.params.Bucket,Key:this.options.params.Key}); this.options.params.Body.destroy(); } }
  `);
  const env = { PATH:process.env.PATH, MAXIMIZER_AWS_REGION:region, MAXIMIZER_AWS_ACCESS_KEY_ID:"synthetic-key", MAXIMIZER_AWS_SECRET_ACCESS_KEY:"synthetic-secret", MAXIMIZER_INPUT_BUCKET:bucket, MAXIMIZER_RESULTS_BUCKET:"demo-results", JS_TEST_TRACE:join(dir,"trace.jsonl"), JS_TEST_MODE:mode };
  if (admitted) env.MAXIMIZER_UPLOAD_FUNCTION = "configured-upload";
  const child = spawnSync(process.execPath, ["--no-warnings", "--experimental-loader", join(dir,"loader.mjs"), fileURLToPath(new URL("./upload_and_fetch.mjs", import.meta.url)), "category_a", join(dir,"demo.mp3")], {cwd:dir,env,encoding:"utf8",timeout:10000});
  assert.ok(!child.error, child.error?.message);
  assert.ok(!child.stderr.includes("SyntaxError"), child.stderr);
  const trace = (await readFile(join(dir,"trace.jsonl"),"utf8")).trim().split("\n").map(JSON.parse);
  return { child, trace, dir };
}
test("actual CLI opt-in invokes configured Lambda then reads existing result without direct Upload", async t => {
  const {child,trace,dir} = await runCli(t,true);
  assert.equal(child.status,0,child.stderr);
  assert.deepEqual(trace.map(r=>r.op),["InvokeCommand","HeadObjectCommand","GetObjectCommand"]);
  assert.equal(trace[0].input.FunctionName,"configured-upload");
  assert.equal(trace[0].input.InvocationType,"RequestResponse");
  assert.equal(trace[0].input.Payload.operation,"start");
  assert.equal(trace[0].input.Payload.key,"category_a/category_a__demo.mp3");
  assert.equal(await readFile(join(dir,"demo.json"),"utf8"),"synthetic result");
});
test("actual CLI without opt-in retains direct S3 Upload and result download", async t => {
  const {child,trace,dir} = await runCli(t,false);
  assert.equal(child.status,0,child.stderr);
  assert.deepEqual(trace.map(r=>r.op),["DirectUpload","HeadObjectCommand","GetObjectCommand"]);
  assert.equal(trace[0].input.Key,"category_a/category_a__demo.mp3");
  assert.equal(await readFile(join(dir,"demo.json"),"utf8"),"synthetic result");
});
for (const mode of ["rejected","function-error","malformed"]) test(`actual CLI ${mode} has no direct fallback and sanitizes private response`, async t => {
  const {child,trace} = await runCli(t,true,mode);
  assert.equal(child.status,1);
  assert.deepEqual(trace.map(r=>r.op),["InvokeCommand"]);
  assert.doesNotMatch(child.stderr,/private|synthetic-secret|signed-url/);
  assert.match(child.stderr,mode==="rejected" ? /429 upload_quota_exceeded/ : /admission service failed/);
});

test("malformed notification failure never prints its body or deletes a message", async () => {
  const f = resultFixture(); f.options.admitted = false;
  f.options.receive = async () => ({Messages:[{Body:"https://private.invalid/secret signed body",ReceiptHandle:"owned"}]});
  await assert.rejects(waitForCompletion(f.options), e => e.message === "Invalid result notification; no queue message was deleted");
  assert.equal(f.counts.download,0); assert.equal(f.counts.remove,0);
});
