// Optional pre-storage admission. Never falls back to direct S3 writes.
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { open, stat } from "node:fs/promises";
import { request as httpsRequest } from "node:https";

const pause = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const MAX_PART = 8 * 1024 * 1024;
const guidance = {
  upload_expired: "; incomplete reservation expired; use a fresh UCID",
  object_name_already_reserved: "; filename is already reserved; changed or replacement files need a fresh UCID",
  request_in_progress: "; reservation is still incomplete; retry the unchanged command while valid; use a fresh UCID if it cannot be recovered",
  upload_conflict: "; completion could not be confirmed; contact Maximizer with the UCID before replacing this upload",
};

// Injectable request/sleep only support offline verification. Production uses
// Node's HTTPS client with normal certificate verification and no redirects.
export async function putPart(url, data, { request = httpsRequest, sleep = pause } = {}) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      return await new Promise((resolve, reject) => {
        const req = request(url, { method: "PUT", headers: { "Content-Length": String(data.length) } }, response => {
          response.resume();
          const status = response.statusCode;
          if (status >= 200 && status < 300) {
            const etag = response.headers.etag;
            if (typeof etag !== "string" || !etag || etag.length > 1024) {
              reject(Object.assign(new Error(), { fatal: true }));
            } else resolve(etag);
          } else reject(Object.assign(new Error(), { status }));
        });
        req.on("error", reject);
        req.setTimeout(45000, () => req.destroy(new Error("timeout")));
        req.end(data);
      });
    } catch (error) {
      if (error.fatal) throw new Error("Reserved upload part returned no valid ETag; completion was not attempted");
      if (error.status && error.status < 500) throw new Error(`Reserved upload part failed: HTTP ${error.status}; start no replacement upload automatically`);
      if (attempt === 2) throw new Error(error.status
        ? `Reserved upload part failed: HTTP ${error.status}; start no replacement upload automatically`
        : "Reserved upload part failed after three network attempts");
      await sleep((attempt + 1) * 1000);
    }
  }
}

export class AdmittedUploads {
  constructor(invoke, region, { put = putPart, sleep = pause } = {}) {
    this.invokeFunction = invoke;
    this.region = region;
    this.put = put;
    this.sleep = sleep;
  }

  async invoke(event) {
    for (let attempt = 0; attempt < 3; attempt++) {
      let result;
      try {
        result = await this.invokeFunction(event);
        if (!result || typeof result !== "object" || Array.isArray(result)) throw new Error();
      } catch {
        throw new Error("Upload admission service failed; no direct upload was attempted");
      }
      if (result.status === 200) return result;
      const status = Number.isInteger(result.status) ? result.status : "invalid";
      const code = typeof result.error === "string" && /^[a-z_]{1,80}$/.test(result.error) ? result.error : "admission_failed";
      const retry = result.retry_after;
      const transient = status === 503 || (status === 409 && code === "request_in_progress");
      if (transient && attempt < 2 && Number.isInteger(retry) && retry >= 1 && retry <= 10) {
        await this.sleep(retry * 1000);
        continue;
      }
      throw new Error(`Upload admission rejected: ${status} ${code}`
        + (Number.isInteger(retry) && retry > 0 ? `; retry after ${retry}s` : "") + (guidance[code] || ""));
    }
  }

  async uploadFile(filename, bucket, key) {
    const size = (await stat(filename)).size;
    if (!Number.isSafeInteger(size) || size < 1) throw new Error("Upload file must have a positive safe byte size");
    const prefix = Buffer.from(`maximizer-upload-v1\0${bucket}\0${key}\0${size}\0`, "utf8");
    const original = createHash("sha256").update(prefix);
    for await (const data of createReadStream(filename, { highWaterMark: MAX_PART })) original.update(data);
    const originalDigest = original.digest();
    const requestId = originalDigest.toString("hex").slice(0, 32);
    const result = await this.invoke({ operation: "start", request_id: requestId, key, size });
    if (result.completed === true) return;
    const parts = result.parts;
    if (!Array.isArray(parts) || !parts.length || result.request_id !== requestId
        || parts.some(p => !p || !Number.isInteger(p.size))
        || parts.reduce((total, p) => total + p.size, 0) !== size) {
      throw new Error("Invalid reserved upload plan");
    }
    // Validate the ENTIRE plan before releasing any local bytes.
    for (const [index, part] of parts.entries()) {
      let url;
      try { url = new URL(part.url); } catch { throw new Error("Invalid reserved upload destination"); }
      if (part.part_number !== index + 1 || part.size < 1 || part.size > MAX_PART
          || url.protocol !== "https:" || url.hostname !== `${bucket}.s3.${this.region}.amazonaws.com`
          || url.username || url.password || (url.port && url.port !== "443")) {
        throw new Error("Invalid reserved upload destination");
      }
    }
    const etags = [];
    const transferred = createHash("sha256").update(prefix);
    const file = await open(filename, "r");
    try {
      for (const part of parts) {
        const data = Buffer.alloc(part.size);
        let offset = 0;
        while (offset < data.length) {
          const { bytesRead } = await file.read(data, offset, data.length - offset, null);
          if (!bytesRead) throw new Error("Local file changed during upload");
          offset += bytesRead;
        }
        transferred.update(data);
        etags.push(await this.put(part.url, data));
      }
      if ((await file.read(Buffer.alloc(1), 0, 1, null)).bytesRead) throw new Error("Local file changed during upload");
    } finally { await file.close(); }
    if (!transferred.digest().equals(originalDigest)) throw new Error("Local file changed during upload; completion was not attempted");
    await this.invoke({ operation: "complete", request_id: requestId, etags });
  }
}
