"""IshedDB's files from Python, as PyMongo's gridfs.GridFSBucket: upload_from_stream(_with_id), open_upload_stream,
open_download_stream(_by_name), download_to_stream(_by_name), find, delete, rename, drop — the same names and
arguments. The bytes go to IshedDB in pieces (a resumable upload, as Cloud Storage's) and come back as a stream; a
file's document is one of the collection <bucket>.files. IshedDB's own: signed_url() for a browser.
"""
from __future__ import annotations

import io
import re
from typing import Any, BinaryIO
from urllib.parse import quote

_BUCKET = re.compile(r"^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,93}$")
_PIECE = 8 * 1024 * 1024  # one request (a multiple of IshedDB's 256 KB segments)
_READ = 1024 * 1024


class GridOut(io.RawIOBase):
    """A file being read, as PyMongo's GridOut: read(), and its document's fields (_id, filename, length…)."""

    def __init__(self, response, document: dict):
        super().__init__()
        self._response = response
        self._id = document.get("_id")
        self.filename = document.get("filename")
        self.length = document.get("length")
        self.upload_date = document.get("uploadDate")
        self.metadata = document.get("metadata")
        self.content_type = document.get("contentType")
        self.sha256 = document.get("sha256")

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        chunk = self._response.read(len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)

    def close(self) -> None:
        if not self.closed:
            self._response.close()
        super().close()


class GridIn:
    """A file being written, as PyMongo's GridIn: write() then close(); its _id once closed."""

    def __init__(self, bucket: "GridFSBucket", filename: str, file_id=None, metadata=None, content_type=None):
        self._bucket, self.filename, self._file_id, self._metadata, self._content_type = bucket, filename, file_id, metadata, content_type
        self._buffer = io.BytesIO()
        self._id = None
        self.closed = False

    def write(self, data: bytes) -> int:
        if self.closed:
            raise ValueError("this GridIn is closed")
        return self._buffer.write(data)

    def close(self) -> None:
        if self.closed:
            return
        self._buffer.seek(0)
        self._id = self._bucket._upload(self.filename, self._buffer, self._file_id, self._metadata, self._content_type)
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if exc[0] is None:
            self.close()


class GridFSBucket:
    """As PyMongo's GridFSBucket(db, bucket_name="fs"): [db] is client.db(name) (or client[name])."""

    def __init__(self, db, bucket_name: str = "fs", chunk_size_bytes: int | None = None, **_ignored):
        from . import IshedError

        if not hasattr(db, "_raw"):
            raise TypeError("GridFSBucket(db): a base of IshedDB, client.db(name)")
        if not isinstance(bucket_name, str) or not _BUCKET.match(bucket_name):
            raise IshedError("invalid-argument", 'bucket_name: 1 to 94 letters, digits, "_", "-" or "."')
        self._db, self._call, self._raw = db, db._call, db._raw
        self.bucket_name = bucket_name
        self._path = f"{db._path}/buckets/{quote(bucket_name, safe='')}"

    # ---- writing --------------------------------------------------------------------------------------------------

    def _upload(self, filename: str, source, file_id=None, metadata=None, content_type=None):
        """Sends [source] (bytes or a binary stream) in pieces of 8 MB; the length told with the last one."""
        from . import IshedError, _to_wire

        stream = io.BytesIO(source) if isinstance(source, (bytes, bytearray, memoryview)) else source
        body: dict[str, Any] = {"filename": filename, "length": None}
        if file_id is not None:
            body["id"] = str(file_id)
        if metadata is not None:
            body["metadata"] = _to_wire(metadata)
        if content_type is not None:
            body["contentType"] = content_type
        first = stream.read(_PIECE)
        if not first:
            body["length"] = 0
        started = self._call("POST", f"{self._path}/uploads", body)
        if "file" in started:
            return started["id"]
        at, piece = 0, first
        while True:
            following = stream.read(_PIECE)
            last = not following
            total = str(at + len(piece)) if last else "*"
            status, _, answer = self._raw("PUT", started["uploadUrl"], piece, {"Content-Range": f"bytes {at}-{at + len(piece) - 1}/{total}", "Content-Type": "application/octet-stream"}, auth=False)
            if status != 200:
                raise IshedError(answer.get("error", {}).get("code", "internal"), answer.get("error", {}).get("message", f"HTTP {status}"), status)
            at += len(piece)
            if last:
                return started["id"]
            piece = following

    def upload_from_stream(self, filename: str, source, chunk_size_bytes=None, metadata=None, session=None, content_type=None):
        """As PyMongo: the file's _id (made by IshedDB)."""
        return self._upload(filename, source, None, metadata, content_type)

    def upload_from_stream_with_id(self, file_id, filename: str, source, chunk_size_bytes=None, metadata=None, session=None, content_type=None) -> None:
        """As PyMongo: the file kept under the _id given."""
        self._upload(filename, source, file_id, metadata, content_type)

    def open_upload_stream(self, filename: str, chunk_size_bytes=None, metadata=None, session=None, content_type=None) -> GridIn:
        return GridIn(self, filename, None, metadata, content_type)

    def open_upload_stream_with_id(self, file_id, filename: str, chunk_size_bytes=None, metadata=None, session=None, content_type=None) -> GridIn:
        return GridIn(self, filename, file_id, metadata, content_type)

    # ---- reading --------------------------------------------------------------------------------------------------

    def _document(self, file_id) -> dict:
        from . import IshedError

        doc = self.find_one({"_id": file_id})
        if doc is None:
            raise IshedError("not-found", f"no file {file_id} in the bucket {self.bucket_name}")
        return doc

    def open_download_stream(self, file_id, session=None) -> GridOut:
        """As PyMongo: a stream of the file's bytes (read() all, or in parts), with its document's fields."""
        from . import IshedError

        doc = self._document(file_id)
        status, response, answer = self._raw("GET", f"{self._path}/files/{quote(str(file_id), safe='')}:download", None, {}, stream=True)
        if status != 200:
            raise IshedError(answer.get("error", {}).get("code", "internal"), answer.get("error", {}).get("message", f"HTTP {status}"), status)
        return GridOut(response, doc)

    def download_to_stream(self, file_id, destination: BinaryIO, session=None) -> None:
        with self.open_download_stream(file_id) as source:
            while chunk := source.read(_READ):
                destination.write(chunk)

    def _by_name(self, filename: str, revision: int):
        from . import IshedError

        files = list(self.find({"filename": filename}).sort("uploadDate", 1 if revision >= 0 else -1).skip(revision if revision >= 0 else -revision - 1).limit(1))
        if not files:
            raise IshedError("not-found", f"no revision {revision} of the file {filename} in the bucket {self.bucket_name}")
        return files[0]["_id"]

    def open_download_stream_by_name(self, filename: str, revision: int = -1, session=None) -> GridOut:
        """As PyMongo: revision 0 the first one sent, -1 the last one (default)."""
        return self.open_download_stream(self._by_name(filename, revision))

    def download_to_stream_by_name(self, filename: str, destination: BinaryIO, revision: int = -1, session=None) -> None:
        self.download_to_stream(self._by_name(filename, revision), destination)

    # ---- the files' documents ----------------------------------------------------------------------------------------

    def find(self, filter=None, *args, **kwargs):
        """As PyMongo: a cursor of the files' documents (the collection <bucket>.files)."""
        return self._db[f"{self.bucket_name}.files"].find(filter or {}, *args, **kwargs)

    def find_one(self, filter=None):
        return self._db[f"{self.bucket_name}.files"].find_one(filter or {})

    def delete(self, file_id, session=None) -> None:
        """As PyMongo: the file's document and its bytes."""
        self._call("POST", f"{self._path}/files/{quote(str(file_id), safe='')}:delete", {})

    def rename(self, file_id, new_filename: str, session=None) -> None:
        self._call("POST", f"{self._path}/files/{quote(str(file_id), safe='')}:rename", {"filename": new_filename})

    def drop(self, session=None) -> None:
        """As PyMongo: every file of the bucket, and its <bucket>.files collection."""
        self._call("POST", f"{self._path}:drop", {})

    # ---- IshedDB's own -------------------------------------------------------------------------------------------------

    def signed_url(self, file_id, expires_in: int = 3600, disposition: str = "attachment") -> dict:
        """A signed address for a browser (no key in it), as Cloud Storage's signed URLs: { url, expiresAt }."""
        return self._call("POST", f"{self._path}/files/{quote(str(file_id), safe='')}:signUrl", {"expiresIn": expires_in, "disposition": disposition})
