"""Optional pre-storage admission transport; result reads still use S3/SQS.

Set MAXIMIZER_UPLOAD_FUNCTION only when the supplied configuration enables this
transport. Admission credentials intentionally cannot write directly to S3.
"""
import json
import hashlib
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


class AdmittedUploads:
    def __init__(self, s3, invoke_client, function_name, region):
        self.s3, self.invoke_client = s3, invoke_client
        self.function_name, self.region = function_name, region

    def __getattr__(self, name):
        return getattr(self.s3, name)

    def invoke(self, event):
        payload = json.dumps(event).encode()
        for attempt in range(3):
            response = self.invoke_client.invoke(FunctionName=self.function_name,
                InvocationType='RequestResponse', Payload=payload)
            if response.get('FunctionError'):
                raise RuntimeError('Upload admission service failed; no direct upload was attempted')
            result = json.loads(response['Payload'].read())
            if result.get('status') == 200:
                return result
            # Print only stable error fields. Responses can otherwise contain
            # temporary signed transfer URLs, which must not enter logs.
            status, code = result.get('status'), result.get('error', 'admission_failed')
            retry = result.get('retry_after')
            transient = status == 503 or (status == 409 and code == 'request_in_progress')
            if transient and attempt < 2 and type(retry) is int and 1 <= retry <= 10:
                time.sleep(retry)
                continue
            guidance = {
                'upload_expired': '; incomplete reservation expired; use a fresh UCID',
                'object_name_already_reserved': '; filename is already reserved; changed or replacement files need a fresh UCID',
                'request_in_progress': '; reservation is still incomplete; retry the unchanged command while valid; use a fresh UCID if it cannot be recovered',
                'upload_conflict': '; completion could not be confirmed; contact Maximizer with the UCID before replacing this upload',
            }.get(code, '')
            raise RuntimeError(f'Upload admission rejected: {status} {code}' + (f'; retry after {retry}s' if retry else '') + guidance)

    @staticmethod
    def put(url, data):
        for attempt in range(3):
            try:
                with urlopen(Request(url, data=data, method='PUT', headers={'Content-Length': str(len(data))}), timeout=45) as response:
                    return response.headers['ETag']
            except HTTPError as error:
                if error.code < 500 or attempt == 2:
                    raise RuntimeError(f'Reserved upload part failed: HTTP {error.code}; start no replacement upload automatically') from None
            except (URLError, TimeoutError, OSError):
                if attempt == 2:
                    raise RuntimeError('Reserved upload part failed after three network attempts') from None
            time.sleep(attempt + 1)

    def upload_file(self, filename, bucket, key):
        size = os.path.getsize(filename)
        # Stable for this exact file and destination, so restarting the same
        # command reuses a valid transfer or confirmed completion after a network
        # outage. Expired or uncertain incomplete reservations need a fresh UCID.
        identity_prefix = f'maximizer-upload-v1\0{bucket}\0{key}\0{size}\0'.encode()
        digest = hashlib.sha256(identity_prefix)
        with open(filename, 'rb') as stream:
            for chunk in iter(lambda: stream.read(8 * 1024 ** 2), b''):
                digest.update(chunk)
        request_id = digest.hexdigest()[:32]
        result = self.invoke({'operation': 'start', 'request_id': request_id, 'key': key, 'size': size})
        if result.get('completed') is True:
            return
        parts = result.get('parts', [])
        if not parts or result.get('request_id') != request_id or sum(p['size'] for p in parts) != size:
            raise RuntimeError('Invalid reserved upload plan')
        for index, part in enumerate(parts):
            parsed = urlsplit(part['url'])
            if (part.get('part_number') != index + 1 or not 1 <= part['size'] <= 8 * 1024 ** 2
                    or parsed.scheme != 'https' or parsed.hostname != f'{bucket}.s3.{self.region}.amazonaws.com'
                    or parsed.username or parsed.password or parsed.port not in (None, 443)):
                raise RuntimeError('Invalid reserved upload destination')
        etags = []
        transferred = hashlib.sha256(identity_prefix)
        with open(filename, 'rb') as stream:
            for part in parts:
                data = stream.read(part['size'])
                if len(data) != part['size']:
                    raise RuntimeError('Local file changed during upload')
                transferred.update(data)
                etags.append(self.put(part['url'], data))
            if stream.read(1):
                raise RuntimeError('Local file changed during upload')
        if transferred.digest() != digest.digest():
            raise RuntimeError('Local file changed during upload; completion was not attempted')
        self.invoke({'operation': 'complete', 'request_id': request_id, 'etags': etags})
