import { mkdtemp, rename, rm } from "node:fs/promises";
import { join } from "node:path";
// Upload recordings and metadata, then download the completion result.
// See README.md and package.json for the required AWS SDK dependencies.
import { readFile, writeFile } from "node:fs/promises";
import { basename } from "node:path";
import { createReadStream } from "node:fs";
import { S3Client, GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { Upload } from "@aws-sdk/lib-storage";
import { AdmittedUploads } from "./admitted_upload.mjs";
import { waitForCompletion, waitForResultOnly } from "./result_waiter.mjs";

function checkCategory(category) {
  if ((!category || /[^a-z0-9_]/.test(category)) || category.includes("__"))
    throw new Error("category must use lowercase letters, digits or underscores, with no '__'");
}

function metadataName(category, ucid, path) {
  const prefix = `${category}__${ucid}`;
  const name = basename(path);
  const valid = (suffix) => /^\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\.json$/.test(suffix);
  if (name.startsWith(prefix) && valid(name.slice(prefix.length))) return name;
  const suffix = process.env.MAXIMIZER_METADATA_SUFFIX || "";
  if (!valid(suffix)) throw new Error("metadata filename must be <category>__<UCID>.<suffix>.json, or set MAXIMIZER_METADATA_SUFFIX to the full ending agreed at onboarding (including the leading dot and .json)");
  return prefix + suffix;
}

function env(name) {
  const v = process.env[name];
  if (!v) throw new Error(`missing env var: ${name}`);
  return v;
}

// The bucket matches the extension case-sensitively and lists only the
// all-lower and all-upper spellings, so ".wav" and ".WAV" pass while ".Wav" is
// refused. Mirror that exactly rather than lower-casing.
const ALLOWED_EXT = [".wav", ".mp3", ".json"];
const extensionIsAccepted = (p) => {
  const m = p.match(/\.[^.\/]*$/);
  if (!m) return false;
  const ext = m[0];
  return ALLOWED_EXT.includes(ext.toLowerCase())
    && (ext === ext.toLowerCase() || ext === ext.toUpperCase());
};

async function main(category, audioPath) {
  checkCategory(category);
  if (!extensionIsAccepted(audioPath)) throw new Error(`${audioPath}: expected .wav, .mp3 or .json, spelled all-lower or all-upper`);
  const prefix = `${category}__`;
  let filename = basename(audioPath);
  if (!filename.startsWith(prefix)) filename = prefix + filename;
  const ext = (process.env.MAXIMIZER_RESULTS_EXT || "json").replace(/^\./, "");
  const stem = filename.slice(prefix.length).replace(/\.[^.]*$/, "");
  const { s3, sqs } = await clients();
  await put(s3, env("MAXIMIZER_INPUT_BUCKET"), `${category}/${filename}`, audioPath);
  await waitForResult(s3, sqs, env("MAXIMIZER_RESULTS_BUCKET"), `${category}/${stem}.${ext}`);
}

const ORG_NAME_RE = /^(org|users)_(\d{4}-\d{2}-\d{2})\.json$/;
// JSON.parse checks grammar. Walk tokens to reject duplicate object keys.
function parseJson(text) {
  text = text.replace(/^\uFEFF/, "");
  const value = JSON.parse(text);
  const tokens = text.match(/"(?:[^"\\]|\\.)*"|[{}\[\]:,]|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null/g);
  let i = 0;
  function walk() {
    const token = tokens[i++];
    if (token === "{") {
      const seen = new Set();
      while (tokens[i] !== "}") {
        const key = JSON.parse(tokens[i++]);
        if (seen.has(key)) throw new Error(`duplicate JSON key: ${key}`);
        seen.add(key);
        i++; // colon
        walk();
        if (tokens[i] !== ",") break;
        i++;
      }
      i++;
    } else if (token === "[") {
      while (tokens[i] !== "]") {
        walk();
        if (tokens[i] !== ",") break;
        i++;
      }
      i++;
    } else if (/^-?\d/.test(token) && !Number.isFinite(Number(token))) {
      throw new Error("non-finite JSON number");
    }
  }
  walk();
  return value;
}

async function readJson(path, limit) {
  const bytes = await readFile(path);
  if (limit !== undefined && bytes.length > limit) throw new Error(`${path}: metadata must be at most 1 MiB`);
  return parseJson(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
}

async function checkMetadataFile(metadataPath, category, ucid, audioNames) {
  const metadata = await readJson(metadataPath, 1024 * 1024);
  if (metadata === null || typeof metadata !== "object" || Array.isArray(metadata))
    throw new Error(`${metadataPath}: must be one JSON object`);
  if (metadata.ucid !== ucid) throw new Error(`${metadataPath}: 'ucid' must equal the call id ${ucid}`);
  if (JSON.stringify(metadata.audio_files) !== JSON.stringify(audioNames))
    throw new Error(`${metadataPath}: 'audio_files' must list the uploaded names exactly, in part order: ${audioNames}`);
  return metadata;
}

async function clients() {
  const region = env("MAXIMIZER_AWS_REGION"); // required: the region in your config file
  const credentials = {
    accessKeyId: process.env.MAXIMIZER_AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.MAXIMIZER_AWS_SECRET_ACCESS_KEY,
  };
  const s3 = new S3Client({ region, credentials, maxAttempts: 5 });
  if (process.env.MAXIMIZER_UPLOAD_FUNCTION) {
    const { LambdaClient, InvokeCommand } = await import("@aws-sdk/client-lambda");
    const lambda = new LambdaClient({ region, credentials, maxAttempts: 5 });
    s3.admittedUploads = new AdmittedUploads(async event => {
      const response = await lambda.send(new InvokeCommand({
        FunctionName: process.env.MAXIMIZER_UPLOAD_FUNCTION,
        InvocationType: "RequestResponse", Payload: Buffer.from(JSON.stringify(event)),
      }));
      if (response.FunctionError) throw new Error("Admission function failed");
      return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(response.Payload));
    }, region);
  }
  return { s3, sqs: new SQSClient({ region, credentials }) };
}

async function put(s3, bucket, key, path) {
  console.log(`uploading ${path} -> ${bucket}/${key}`);
  if (s3.admittedUploads) return s3.admittedUploads.uploadFile(path, bucket, key);
  await new Upload({ client: s3, params: { Bucket: bucket, Key: key, Body: createReadStream(path) } }).done();
}

// One-part recording: `<category>__<UCID>.wav`; parts: `<category>__<UCID>-<n>.wav`.
function ucidNames(category, ucid, audioPaths) {
  return audioPaths.map((p, i) => {
    const ext = (p.match(/\.[^.\/]*$/) || [""])[0];
    if (![".wav", ".mp3"].includes(ext.toLowerCase()) || !extensionIsAccepted(p))
      throw new Error(`${p}: a recording must be .wav or .mp3, spelled all-lower or all-upper`);
    return `${category}__${ucid}${audioPaths.length === 1 ? "" : `-${i + 1}`}${ext}`;
  });
}

async function uploadUcidCall(category, ucid, metadataPath, audioPaths, batch) {
  checkCategory(category);
  // "__" separates the category token and a trailing -<digits> is read as a part number.
  if (!ucid || ucid.includes("/") || ucid.includes("__") || /.+-\d+$/.test(ucid))
    throw new Error(`${ucid}: a UCID may not be empty, contain '/' or '__', or end in -<digits>`);
  const folder = category + (batch ? "_batch" : "");
  const audioNames = ucidNames(category, ucid, audioPaths);

  const uploadMetadataName = metadataName(category, ucid, metadataPath);
  await checkMetadataFile(metadataPath, category, ucid, audioNames);

  const { s3, sqs } = await clients();
  const inputBucket = process.env.MAXIMIZER_INPUT_BUCKET;
  // 1) Every part plus the one metadata file, in any order.
  for (let i = 0; i < audioPaths.length; i++) await put(s3, inputBucket, `${folder}/${audioNames[i]}`, audioPaths[i]);
  await put(s3, inputBucket, `${folder}/${uploadMetadataName}`, metadataPath);
  console.log("upload complete");

  // 2) The result is always `<category>/<UCID>.json`, also for batch and multi-part calls.
  await waitForResult(s3, sqs, process.env.MAXIMIZER_RESULTS_BUCKET, `${category}/${ucid}.json`);
}

async function waitForResult(s3, sqs, resultsBucket, resultKey) {
  return waitForCompletion({
    resultKey, queueUrl: process.env.MAXIMIZER_RESULTS_QUEUE_URL,
    admitted: Boolean(process.env.MAXIMIZER_UPLOAD_FUNCTION),
    head: () => s3.send(new HeadObjectCommand({ Bucket: resultsBucket, Key: resultKey })),
    download: async () => {
      const obj = await s3.send(new GetObjectCommand({ Bucket: resultsBucket, Key: resultKey }));
      await writeFile(basename(resultKey), await obj.Body.transformToByteArray());
      console.log(`result ready: ${resultsBucket}/${resultKey} -> ${basename(resultKey)}`);
    },
    receive: () => sqs.send(new ReceiveMessageCommand({ QueueUrl: process.env.MAXIMIZER_RESULTS_QUEUE_URL, MaxNumberOfMessages: 10, WaitTimeSeconds: 20 })),
    remove: message => sqs.send(new DeleteMessageCommand({ QueueUrl: process.env.MAXIMIZER_RESULTS_QUEUE_URL, ReceiptHandle: message.ReceiptHandle })),
  });
}

// The daily org file and users list: same date, both to `org/`.
// They produce no result file and no file_status message.
async function uploadOrgFiles(orgPath, usersPath) {
  const [o, u] = [basename(orgPath).match(ORG_NAME_RE), basename(usersPath).match(ORG_NAME_RE)];
  if (!o || o[1] !== "org" || !u || u[1] !== "users")
    throw new Error("expected org_YYYY-MM-DD.json and users_YYYY-MM-DD.json, in that order");
  if (o[2] !== u[2]) throw new Error("the org file and the users list must carry the same date");
  for (const p of [orgPath, usersPath]) // refuse a file that is not a JSON array locally
    if (!Array.isArray(await readJson(p))) throw new Error(`${p}: must be a JSON array`);
  const { s3 } = await clients();
  for (const p of [orgPath, usersPath]) await put(s3, process.env.MAXIMIZER_INPUT_BUCKET, `org/${basename(p)}`, p);
  console.log("upload complete (org files produce no result file)");
}


async function fetchResultOnly(category, ucid) {
  checkCategory(category);
  if (!ucid || ucid.includes("/") || ucid.includes("__") || /.+-\d+$/.test(ucid))
    throw new Error("Invalid UCID for result retrieval");
  if (/[\\:\x00-\x1f\x7f]/.test(ucid)) throw new Error("UCID contains a character unsafe for a local result filename");
  const region = env("MAXIMIZER_AWS_REGION"), bucket = env("MAXIMIZER_RESULTS_BUCKET");
  const s3 = new S3Client({ region, maxAttempts: 1, credentials: {
    accessKeyId: env("MAXIMIZER_AWS_ACCESS_KEY_ID"), secretAccessKey: env("MAXIMIZER_AWS_SECRET_ACCESS_KEY")
  } });
  const key = `${category}/${ucid}.json`, output = `${ucid}.json`;
  try {
    await waitForResultOnly({
      head: () => s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }), { abortSignal: AbortSignal.timeout(30000) }),
      download: async () => {
        const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }), { abortSignal: AbortSignal.timeout(30000) });
        const bytes = await response.Body.transformToByteArray();
        const temporary = await mkdtemp(join(process.cwd(), ".maximizer-result-"));
        try {
          const file = join(temporary, "result.json");
          await writeFile(file, bytes, { mode: 0o600, flag: "wx" });
          await rename(file, output);
        } finally { await rm(temporary, { recursive: true, force: true }); }
      }
    });
    console.log(`result ready: ${bucket}/${key} -> ${output}`);
  } finally { s3.destroy(); }
}

const USAGE = `usage:
  node upload_and_fetch.mjs <category> <audio_file>
  node upload_and_fetch.mjs --ucid [--batch] <category> <UCID> <metadata.json> <audio> [<audio> ...]
  node upload_and_fetch.mjs --org <org_YYYY-MM-DD.json> <users_YYYY-MM-DD.json>
  node upload_and_fetch.mjs --result-only <category> <UCID>`;

let args = process.argv.slice(2);
let run;
if (args[0] === "--result-only" && args.length === 3) run = fetchResultOnly(args[1], args[2]);
else if (args[0] === "--org" && args.length === 3) run = uploadOrgFiles(args[1], args[2]);
else if (args[0] === "--ucid") {
  args = args.slice(1);
  const batch = args[0] === "--batch";
  if (batch) args = args.slice(1);
  if (args.length < 4) { console.error(USAGE); process.exit(2); }
  run = uploadUcidCall(args[0], args[1], args[2], args.slice(3), batch);
} else if (args.length === 2 && !args[0].startsWith("--")) run = main(args[0], args[1]);
else { console.error(USAGE); process.exit(2); }
run.catch((e) => { console.error(e.message); process.exit(1); });
