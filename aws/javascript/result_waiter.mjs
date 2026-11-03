// Result reads remain S3/SQS operations, including when admission is enabled.
export async function waitForCompletion({ head, download, receive, remove, resultKey,
  queueUrl, admitted = false, now = Date.now, sleep = ms => new Promise(r => setTimeout(r, ms)) }) {
  const deadline = now() + 15 * 60 * 1000;
  const existingResult = async () => {
    try { await head(); }
    catch (error) {
      const code = error.name || error.Code || error.code;
      if (["404", "NoSuchKey", "NotFound"].includes(code) || error.$metadata?.httpStatusCode === 404) return false;
      throw error;
    }
    await download();
    return true;
  };
  const admittedQueue = Boolean(queueUrl && admitted);
  if (admittedQueue && await existingResult()) return;
  if (queueUrl) {
    while (now() < deadline) {
      const response = await receive();
      for (const message of response.Messages ?? []) {
        let status;
        try {
          status = JSON.parse(message.Body);
          if (status === null || typeof status !== "object") throw new Error();
        } catch {
          throw new Error("Invalid result notification; no queue message was deleted");
        }
        if (status.results_file_name !== resultKey) continue;
        await download();
        await remove(message);
        return;
      }
      // Another consumer may have taken the notification since our first HEAD.
      if (admittedQueue && await existingResult()) return;
    }
    throw new Error("timed out waiting for the notification — contact Maximizer with the UCID");
  }
  while (now() < deadline) {
    try { await head(); return download(); }
    catch { await sleep(15000); }
  }
  throw new Error("timed out waiting for result — contact Maximizer with the UCID");
}


// Explicit result retrieval is independent of upload transport and SQS.
export async function waitForResultOnly({ head, download, now = Date.now,
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  const deadline = now() + 15 * 60 * 1000;
  while (now() < deadline) {
    try {
      await head();
      await download();
      return;
    } catch (error) {
      const code = error?.name || error?.Code || error?.code;
      const status = error?.$metadata?.httpStatusCode;
      const missing = status === 404 || ((status === undefined || status === 0) && ["404", "NoSuchKey", "NotFound"].includes(code));
      if (!missing) {
        const detail = Number.isInteger(status) && status >= 100 && status <= 599 ? ` (HTTP ${status})` : "";
        throw new Error(`Result read failed${detail}; check result-read access or retry the result-only command`);
      }
    }
    await sleep(15000);
  }
  throw new Error("timed out waiting for result — contact Maximizer with the UCID");
}
