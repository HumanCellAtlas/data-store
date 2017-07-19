import hashlib

from botocore.auth import S3SigV4Auth, EMPTY_SHA256_HASH
from botocore.awsrequest import AWSRequest
import urllib3
import urllib3.connection

class S3SigningChunker(S3SigV4Auth):
    chunk_size = 1024 * 1024
    def __init__(self, fh, total_bytes, credentials, service_name, region_name):
        S3SigV4Auth.__init__(self, credentials, service_name, region_name)
        self.fh = fh
        self.total_bytes = total_bytes

    def chunk_string_to_sign(self, chunk, request, previous_signature):
        sts = ['AWS4-HMAC-SHA256-PAYLOAD']
        sts.append(request.context['timestamp'])
        sts.append(self.credential_scope(request))
        sts.append(previous_signature)
        sts.append(EMPTY_SHA256_HASH)
        sts.append(hashlib.sha256(chunk).hexdigest())
        return '\n'.join(sts)

    def __iter__(self):
        while True:
            chunk = self.fh.read(self.chunk_size)
            chunk_sts = self.chunk_string_to_sign(chunk, self.request, self.previous_signature)
            chunk_sig = self.signature(chunk_sts, self.request)
            yield b"".join([format(len(chunk), "x").encode(),
                            b";chunk-signature=",
                            chunk_sig.encode(),
                            b"\r\n",
                            chunk,
                            b"\r\n"])
            self.previous_signature = chunk_sig
            if len(chunk) == 0:
                break

    def payload(self, request):
        return "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

    def content_length(self):
        fixed_chunk_overhead = len(";chunk-signature=" + EMPTY_SHA256_HASH + "\r\n\r\n")
        std_chunk_gross_len = self.chunk_size + len(format(self.chunk_size, "x")) + fixed_chunk_overhead
        content_length = std_chunk_gross_len * (self.total_bytes // self.chunk_size)
        if self.total_bytes % self.chunk_size:
            short_chunk_net_len = self.total_bytes % self.chunk_size
            short_chunk_gross_len = short_chunk_net_len + len(format(short_chunk_net_len, "x")) + fixed_chunk_overhead
            content_length += short_chunk_gross_len
        zero_chunk_gross_len = fixed_chunk_overhead + 1
        content_length += zero_chunk_gross_len
        return content_length

    def get_headers(self, method, url):
        headers = {"content-encoding": "aws-chunked",
                   "content-length": str(self.content_length()),
                   "x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                   "x-amz-decoded-content-length": str(self.total_bytes)}
        self.request = AWSRequest(method=method, url=url, headers=headers)
        self.add_auth(self.request)
        seed_signature = self.request.headers["Authorization"].rsplit(" ", 1)[-1]
        assert seed_signature.startswith("Signature=")
        seed_signature = seed_signature[len("Signature="):]
        self.previous_signature = seed_signature
        return dict(self.request.headers.items())

class ChunkingHTTPSConnection(urllib3.connection.HTTPSConnection):
    def request_chunked(self, method, url, body=None, headers=None):
        self.putrequest(method, url, skip_accept_encoding=True)
        for header, value in headers.items():
            self.putheader(header, value)
        self.endheaders()
        for chunk in body:
            self.send(chunk)

class ChunkingHTTPSConnectionPool(urllib3.HTTPSConnectionPool):
    ConnectionCls = ChunkingHTTPSConnection

def get_pool_manager(**kwargs):
    kwargs["cert_reqs"] = "CERT_REQUIRED"
    pool_manager = urllib3.PoolManager(**kwargs)
    pool_manager.pool_classes_by_scheme["https"] = ChunkingHTTPSConnectionPool
    return pool_manager
