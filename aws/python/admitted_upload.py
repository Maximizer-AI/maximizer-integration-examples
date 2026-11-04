"""Optional pre-storage admission transport; result reads still use S3/SQS.

Set MAXIMIZER_UPLOAD_FUNCTION only when the supplied configuration enables this
transport. Admission credentials intentionally cannot write directly to S3.
"""
import json
import hashlib
import os
import ssl
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

try:
    import certifi
except ImportError:  # optional: the interpreter's own trust store is used alone
    certifi = None

TLS_GUIDANCE = ('install certifi (python3 -m pip install certifi) or the interpreter\'s certificates, '
                'or set SSL_CERT_FILE to a CA bundle such as the path printed by python3 -m certifi')


def transfer_tls_context():
    """Verifying TLS context for reserved transfer URLs.

    The default context trusts the interpreter's store and honours SSL_CERT_FILE.
    When certifi is installed, its bundle is added so part uploads trust the
    same roots as the SDK's admission call. Verification is never disabled.
    """
    context = ssl.create_default_context()
    if certifi is not None:
        context.load_verify_locations(cafile=certifi.where())
    return context


class AdmittedUploads:
    def __init__(self, s3, invoke_client, function_name, region):
        self.s3, self.invoke_client = s3, invoke_client
        self.function_name, self.region = function_name, region
        self.tls_context = transfer_tls_context()

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

    def put(self, url, data):
        for attempt in range(3):
            try:
                with urlopen(Request(url, data=data, method='PUT', headers={'Content-Length': str(len(data))}),
                             timeout=45, context=self.tls_context) as response:
                    return response.headers['ETag']
            except HTTPError as error:
                if error.code < 500 or attempt == 2:
                    raise RuntimeError(f'Reserved upload part failed: HTTP {error.code}; start no replacement upload automatically') from None
            except (URLError, TimeoutError, OSError) as error:
                # Report only the error class: exception text can otherwise
                # carry the temporary signed transfer URL.
                cause = error.reason if isinstance(getattr(error, 'reason', None), BaseException) else error
                if isinstance(cause, ssl.SSLCertVerificationError):
                    raise RuntimeError('Reserved upload part failed: TLS certificate verification failed, so this '
                                       'Python build cannot verify the storage certificate; ' + TLS_GUIDANCE) from None
                if attempt == 2:
                    raise RuntimeError('Reserved upload part failed after three network attempts '
                                       f'({type(cause).__name__})') from None
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
