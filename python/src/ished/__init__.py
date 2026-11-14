"""IshedDB for Python, written as PyMongo is: client.db("clinique").collection("patients").find({...}).

Only the standard library. The key never travels but over HTTPS (or to this machine). Values: datetime <-> dates,
int beyond 2**53 <-> big integers, bytes <-> bytes, as IshedDB keeps them.
"""

from __future__ import annotations

import base64
import functools
import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator
from urllib.parse import quote, unquote, urlsplit

__all__ = ["connect", "IshedError", "GridFSBucket", "ClientSession", "ChangeStream", "IndexModel", "ASCENDING", "DESCENDING", "TEXT", "InsertOneResult", "InsertManyResult", "UpdateResult", "DeleteResult"]

_CLUSTER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,63}$")
_SAFE_INT = 2**53 - 1
MAX_DOCUMENTS = 100_000


class IshedError(Exception):
    """A refusal from IshedDB: its code (invalid-argument, not-found, permission-denied…) and message."""

    def __init__(self, code: str, message: str, status: int | None = None, details=None):
        super().__init__(f"{code}: {message}")
        self.code, self.message, self.status, self.details = code, message, status, details


def _to_wire(value: Any) -> Any:
    if isinstance(value, datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return {"~date": moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")}
    if isinstance(value, bool) or value is None or isinstance(value, (str, float)):
        return value
    if isinstance(value, int):
        return value if abs(value) <= _SAFE_INT else {"~int": str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {"~bytes": base64.b64encode(bytes(value)).decode()}
    if isinstance(value, dict):
        return {str(k): _to_wire(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_wire(v) for v in value]
    raise IshedError("invalid-argument", f"a value of type {type(value).__name__} cannot be kept")


def _from_wire(value: Any) -> Any:
    if isinstance(value, list):
        return [_from_wire(v) for v in value]
    if isinstance(value, dict):
        if len(value) == 1:
            (k, v), = value.items()
            if k == "~date":
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            if k == "~int":
                return int(v)
            if k == "~bytes":
                return base64.b64decode(v)
        return {k: _from_wire(v) for k, v in value.items()}
    return value


@dataclass(frozen=True)
class InsertOneResult:
    inserted_id: str
    acknowledged: bool = True


@dataclass(frozen=True)
class InsertManyResult:
    inserted_ids: list
    acknowledged: bool = True


@dataclass(frozen=True)
class UpdateResult:
    matched_count: int
    modified_count: int
    upserted_id: str | None
    acknowledged: bool = True


@dataclass(frozen=True)
class DeleteResult:
    deleted_count: int
    acknowledged: bool = True


@dataclass(frozen=True)
class BulkWriteResult:
    inserted_count: int
    matched_count: int
    modified_count: int
    deleted_count: int
    upserted_count: int
    upserted_ids: dict
    acknowledged: bool = True


# The operations of bulk_write, as PyMongo's: InsertOne(doc), UpdateOne(filter, update, upsert=False), …
class InsertOne:
    def __init__(self, document: dict):
        self._op = {"insertOne": {"document": document}}


class UpdateOne:
    def __init__(self, filter: dict, update: dict, upsert: bool = False):
        self._op = {"updateOne": {"filter": filter, "update": update, "upsert": upsert}}


class UpdateMany:
    def __init__(self, filter: dict, update: dict):
        self._op = {"updateMany": {"filter": filter, "update": update}}


class ReplaceOne:
    def __init__(self, filter: dict, replacement: dict, upsert: bool = False):
        self._op = {"replaceOne": {"filter": filter, "replacement": replacement, "upsert": upsert}}


class DeleteOne:
    def __init__(self, filter: dict):
        self._op = {"deleteOne": {"filter": filter}}


class DeleteMany:
    def __init__(self, filter: dict):
        self._op = {"deleteMany": {"filter": filter}}


ASCENDING = 1
DESCENDING = -1
TEXT = "text"


def _index_keys(keys):
    """PyMongo's forms of an index's keys: "field", [("a", ASCENDING), ("b", DESCENDING)], [("titre", TEXT)]."""
    if isinstance(keys, str):
        return [[keys, ASCENDING]]
    if isinstance(keys, dict):
        return [[k, d] for k, d in keys.items()]
    return [[k, d] for k, d in keys]


class IndexModel:
    """As PyMongo's IndexModel(keys, unique=…, name=…, partialFilterExpression=…, weights=…, default_language=…)."""

    def __init__(self, keys, **kwargs):
        self.document = {"key": _index_keys(keys), **kwargs}


def _sort_spec(key_or_list, direction=None) -> dict:
    """PyMongo's forms: sort("age", -1), sort([("age", -1), ("nom", 1)]), or a dict."""
    if isinstance(key_or_list, dict):
        return dict(key_or_list)
    if isinstance(key_or_list, str):
        return {key_or_list: 1 if direction is None else direction}
    return {k: d for k, d in key_or_list}


class Cursor:
    """As PyMongo's cursor: .sort(), .limit(), .skip(), then iterate (or list(...)). Read 1000 at a time."""

    def __init__(self, call, path: str, filter: dict | None, projection, sort, limit: int, skip: int):
        self._call, self._path = call, path
        self._filter = filter or {}
        self._projection = projection if not isinstance(projection, (list, tuple)) else {f: 1 for f in projection}
        self._sort = _sort_spec(sort) if sort is not None else None
        self._limit, self._skip = limit, skip

    def sort(self, key_or_list, direction=None) -> "Cursor":
        self._sort = _sort_spec(key_or_list, direction)
        return self

    def limit(self, n: int) -> "Cursor":
        self._limit = n
        return self

    def skip(self, n: int) -> "Cursor":
        self._skip = n
        return self

    def to_list(self) -> list:
        out: list = []
        token = None
        while True:
            want = min(1000, self._limit - len(out)) if self._limit else 1000
            body = {"filter": _to_wire(self._filter), "limit": want}
            # The next page starts after the last one read (its token), never at an offset that moves with writes.
            if token:
                body["pageToken"] = token
            else:
                body["skip"] = self._skip
            if self._sort:
                body["sort"] = self._sort
            if self._projection:
                body["projection"] = self._projection
            page = self._call("POST", f"{self._path}:find", body)
            out.extend(_from_wire(d) for d in page["documents"])
            token = page.get("nextPageToken")
            if not token or (self._limit and len(out) >= self._limit):
                return out
            if len(out) >= MAX_DOCUMENTS:
                raise IshedError("resource-exhausted", f"more than {MAX_DOCUMENTS} documents: narrow the filter")

    def __iter__(self) -> Iterator[dict]:
        return iter(self.to_list())

    def explain(self) -> dict:
        """As PyMongo's cursor.explain(): the find's plan and what running it examined (MongoDB's explain)."""
        find = {"find": unquote(self._path.rsplit("/", 1)[1]), "filter": _to_wire(self._filter)}
        if self._sort:
            find["sort"] = self._sort
        if self._projection:
            find["projection"] = self._projection
        if self._skip:
            find["skip"] = self._skip
        if self._limit:
            find["limit"] = self._limit
        return self._call("POST", f"{self._path}:explain", {"explain": find})


class ClientSession:
    """As PyMongo's: with client.start_session() as s: s.with_transaction(lambda s: ...), or with s.start_transaction(): ...

    Each method of a collection given session=s runs in its transaction, held by the server (60 seconds at most,
    20 without a request); a conflict with another transaction answers "aborted", and with_transaction runs it again.
    """

    def __init__(self, call, v1: str):
        self._call, self._v1, self._id = call, v1, None

    @property
    def in_transaction(self) -> bool:
        return self._id is not None

    def start_transaction(self) -> "_TransactionContext":
        if self._id is not None:
            raise IshedError("failed-precondition", "a transaction is already started in this session")
        self._id = self._call("POST", f"{self._v1}:beginTransaction", {})["transaction"]
        return _TransactionContext(self)

    def _bind(self, call):
        header = {"X-Ished-Transaction": self._id}
        return lambda method, path, body: call(method, path, body, header)

    def commit_transaction(self) -> None:
        if self._id is None:
            raise IshedError("failed-precondition", "no transaction started in this session")
        tx, self._id = self._id, None
        self._call("POST", f"{self._v1}:commit", {"transaction": tx})

    def abort_transaction(self) -> None:
        if self._id is None:
            return
        tx, self._id = self._id, None
        try:
            self._call("POST", f"{self._v1}:rollback", {"transaction": tx})
        except IshedError as e:
            if e.code != "not-found":  # already ended by the server (an error, or too long): nothing kept
                raise

    def with_transaction(self, callback, *, max_attempts: int = 5):
        for attempt in range(1, max_attempts + 1):
            self.start_transaction()
            try:
                out = callback(self)
                self.commit_transaction()
                return out
            except BaseException as e:
                try:
                    self.abort_transaction()
                except IshedError:
                    pass
                if not (isinstance(e, IshedError) and e.code == "aborted") or attempt >= max_attempts:
                    raise

    def end_session(self) -> None:
        self.abort_transaction()

    def __enter__(self) -> "ClientSession":
        return self

    def __exit__(self, *exc) -> None:
        self.end_session()


class _TransactionContext:
    def __init__(self, session: ClientSession):
        self._session = session

    def __enter__(self) -> "_TransactionContext":
        return self

    def __exit__(self, kind, value, tb) -> None:
        if self._session.in_transaction:
            if kind is None:
                self._session.commit_transaction()
            else:
                self._session.abort_transaction()


def _with_session(method):
    """session=… as PyMongo's: the method (and its cursor, read later) runs in the session's transaction."""

    @functools.wraps(method)
    def wrapper(self, *args, session: ClientSession | None = None, **kwargs):
        if session is not None and session.in_transaction:
            self = Collection(session._bind(self._call), self._path)
        return method(self, *args, **kwargs)

    return wrapper


class ChangeStream:
    """As PyMongo's: with coll.watch([{"$match": {...}}]) as stream: for change in stream: ... (try_next(), resume_token).

    Opened at once: the changes made from then on are seen. Each request waits up to max_await_time_ms (10 s by
    default, 20 s at most) on the server. A collection dropped or renamed ends its stream ("invalidate").
    """

    def __init__(self, call, path: str, pipeline=None, *, full_document=None, full_document_before_change=None, resume_after=None,
                 start_after=None, start_at_operation_time=None, max_await_time_ms=None, batch_size=None):
        self._call, self._path = call, path
        self._body = {"pipeline": _to_wire(list(pipeline or []))}
        if full_document:
            self._body["fullDocument"] = full_document
        # As PyMongo's: the document before each change ("whenAvailable", "required"), kept when the collection has
        # changeStreamPreAndPostImages.
        if full_document_before_change:
            self._body["fullDocumentBeforeChange"] = full_document_before_change
        if batch_size:
            self._body["batchSize"] = batch_size
        self._wait = 10_000 if max_await_time_ms is None else max_await_time_ms
        self._position = resume_after or start_after
        self._at = start_at_operation_time
        self._token = self._position
        self._buffer: list = []
        self.alive = True
        if self._position is None and self._at is None:
            self._fetch(0)

    @property
    def resume_token(self):
        """The token to resume after the last change given (resume_after=)."""
        return self._token

    def _fetch(self, wait: int) -> None:
        body = dict(self._body, maxAwaitTimeMS=wait)
        if self._position is not None:
            body["resumeAfter"] = self._position
        elif self._at is not None:
            body["startAtOperationTime"] = _to_wire(self._at)
        r = self._call("POST", self._path, body)
        self._at = None
        self._position = r["resumeToken"]
        self._buffer.extend(_from_wire(c) for c in r["changes"])
        if not self._buffer:
            self._token = r["resumeToken"]

    def _take(self) -> dict:
        change = self._buffer.pop(0)
        self._token = change["_id"] if self._buffer else self._position
        if change["operationType"] == "invalidate":
            self.alive = False
        return change

    def try_next(self) -> dict | None:
        """The next change if one comes within a second (or max_await_time_ms, if shorter), else None."""
        if not self._buffer and self.alive:
            self._fetch(min(self._wait, 1000))
        return self._take() if self._buffer else None

    def next(self) -> dict:
        while True:
            if self._buffer:
                return self._take()
            if not self.alive:
                raise StopIteration
            self._fetch(self._wait)

    __next__ = next

    def __iter__(self) -> "ChangeStream":
        return self

    def close(self) -> None:
        self.alive = False
        self._buffer = []

    def __enter__(self) -> "ChangeStream":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class Collection:
    def __init__(self, call, path: str):
        self._call, self._path = call, path

    def drop(self) -> None:
        """As PyMongo's drop(): the collection and its documents, or the view."""
        self._call("POST", f"{self._path}:drop", {})

    def create_index(self, keys, **kwargs) -> str:
        """As PyMongo's: create_index([("a", ASCENDING), ("b", DESCENDING)], unique=True); [("titre", TEXT)] with
        weights= and default_language=; partialFilterExpression=. Answers its name."""
        return self._call("POST", f"{self._path}:createIndex", _to_wire({"key": _index_keys(keys), **kwargs}))["name"]

    def create_indexes(self, indexes: list) -> list:
        return self._call("POST", f"{self._path}:createIndexes", _to_wire({"indexes": [m.document for m in indexes]}))["names"]

    def list_indexes(self) -> list:
        """The indexes, as MongoDB gives them ({"v", "key", "name", "unique", …})."""
        return [_from_wire(i) for i in self._call("POST", f"{self._path}:listIndexes", {})["indexes"]]

    def index_information(self) -> dict:
        return {i["name"]: {"key": list(i["key"].items()), **{k: v for k, v in i.items() if k not in ("name", "key")}} for i in self.list_indexes()}

    def drop_index(self, index_or_name) -> None:
        index = index_or_name if isinstance(index_or_name, str) else dict(_index_keys(index_or_name))
        self._call("POST", f"{self._path}:dropIndex", {"index": index})

    def drop_indexes(self) -> None:
        self._call("POST", f"{self._path}:dropIndexes", {})

    def watch(self, pipeline=None, **options) -> ChangeStream:
        """As PyMongo's collection.watch(): the collection's changes as they happen."""
        return ChangeStream(self._call, f"{self._path}:watch", pipeline, **options)

    @_with_session
    def find(self, filter: dict | None = None, projection=None, *, sort=None, limit: int = 0, skip: int = 0) -> Cursor:
        return Cursor(self._call, self._path, filter, projection, sort, limit, skip)

    @_with_session
    def find_one(self, filter: dict | None = None, projection=None, *, sort=None) -> dict | None:
        found = self.find(filter, projection, sort=sort, limit=1).to_list()
        return found[0] if found else None

    @_with_session
    def count_documents(self, filter: dict | None = None) -> int:
        return self._call("POST", f"{self._path}:countDocuments", {"filter": _to_wire(filter or {})})["count"]

    @_with_session
    def aggregate(self, pipeline: list, *, reveal: list | None = None) -> list:
        """As PyMongo's aggregate: the pipeline run by the server; every result, read 1000 at a time."""
        out: list = []
        skip = 0
        while True:
            body = {"pipeline": _to_wire(pipeline), "limit": 1000, "skip": skip}
            if reveal:
                body["reveal"] = reveal
            page = self._call("POST", f"{self._path}:aggregate", body)
            out.extend(_from_wire(d) for d in page["documents"])
            if not page["more"]:
                return out
            if len(out) >= MAX_DOCUMENTS:
                raise IshedError("resource-exhausted", f"more than {MAX_DOCUMENTS} documents: add $limit")
            skip += 1000

    @_with_session
    def estimated_document_count(self) -> int:
        """The collection's number of documents, from the statistics (at once on a big collection)."""
        return self._call("POST", f"{self._path}:estimatedDocumentCount", {})["count"]

    @_with_session
    def distinct(self, key: str, filter: dict | None = None) -> list:
        """The different values of a field among the documents found (a list's items one by one)."""
        values = self._call("POST", f"{self._path}:distinct", {"key": key, "filter": _to_wire(filter or {})})["values"]
        return [_from_wire(v) for v in values]

    @_with_session
    def insert_one(self, document: dict) -> InsertOneResult:
        r = self._call("POST", f"{self._path}:insertOne", {"document": _to_wire(document)})
        return InsertOneResult(r["insertedId"])

    @_with_session
    def insert_many(self, documents: list) -> InsertManyResult:
        ids: list = []
        for start in range(0, len(documents), 1000):
            r = self._call("POST", f"{self._path}:insertMany", {"documents": [_to_wire(d) for d in documents[start:start + 1000]]})
            ids.extend(r["insertedIds"][str(i)] for i in range(len(r["insertedIds"])))
        return InsertManyResult(ids)

    def _update(self, method: str, filter: dict, change: dict, key: str, upsert: bool) -> UpdateResult:
        body = {"filter": _to_wire(filter), key: _to_wire(change)}
        if method != "updateMany":
            body["upsert"] = upsert
        r = self._call("POST", f"{self._path}:{method}", body)
        return UpdateResult(r["matchedCount"], r["modifiedCount"], r.get("upsertedId"))

    @_with_session
    def update_one(self, filter: dict, update: dict, upsert: bool = False) -> UpdateResult:
        return self._update("updateOne", filter, update, "update", upsert)

    @_with_session
    def update_many(self, filter: dict, update: dict) -> UpdateResult:
        return self._update("updateMany", filter, update, "update", False)

    @_with_session
    def replace_one(self, filter: dict, replacement: dict, upsert: bool = False) -> UpdateResult:
        return self._update("replaceOne", filter, replacement, "replacement", upsert)

    @_with_session
    def delete_one(self, filter: dict | None = None) -> DeleteResult:
        return DeleteResult(self._call("POST", f"{self._path}:deleteOne", {"filter": _to_wire(filter or {})})["deletedCount"])

    @_with_session
    def bulk_write(self, requests: list, ordered: bool = True) -> BulkWriteResult:
        """As PyMongo's bulk_write: the operations in order; ordered=False goes on past a refusal. A refusal raises
        IshedError, its details holding writeErrors and what was done."""
        r = self._call("POST", f"{self._path}:bulkWrite", {"operations": [_to_wire(x._op) for x in requests], "ordered": ordered})
        return BulkWriteResult(r["insertedCount"], r["matchedCount"], r["modifiedCount"], r["deletedCount"], r["upsertedCount"], {int(k): v for k, v in r["upsertedIds"].items()})

    @_with_session
    def delete_many(self, filter: dict | None = None) -> DeleteResult:
        return DeleteResult(self._call("POST", f"{self._path}:deleteMany", {"filter": _to_wire(filter or {})})["deletedCount"])


class Database:
    def __init__(self, call, path: str, raw=None):
        self._call, self._path, self._raw = call, path, raw

    def watch(self, pipeline=None, **options) -> ChangeStream:
        """As PyMongo's db.watch(): the changes of every collection of the base."""
        return ChangeStream(self._call, f"{self._path}:watch", pipeline, **options)

    def collection(self, name: str) -> Collection:
        return Collection(self._call, f"{self._path}/collections/{quote(name, safe='')}")

    __getitem__ = collection
    get_collection = collection

    def list_collection_names(self) -> list:
        return [c["name"] for c in self._call("GET", f"{self._path}/collections", None)]

    def list_collections(self) -> list:
        """As PyMongo's list_collections(): name, type ("collection" or "view"), options, info."""
        return [
            {"name": c["name"], "type": c["type"],
             "options": {"viewOn": c["viewOn"], "pipeline": c["pipeline"]} if c["type"] == "view" else {},
             "info": {"readOnly": c["type"] == "view"}}
            for c in self._call("GET", f"{self._path}/collections", None)
        ]

    def create_collection(self, name: str, **kwargs) -> Collection:
        """As PyMongo's create_collection(name, viewOn=..., pipeline=...): an empty collection, or a view."""
        self._call("POST", f"{self._path}/collections/{quote(name, safe='')}:create", _to_wire(kwargs))
        return self.collection(name)

    def drop_collection(self, name_or_collection) -> None:
        """As PyMongo's drop_collection(): the collection and its documents, or the view."""
        name = name_or_collection if isinstance(name_or_collection, str) else unquote(name_or_collection._path.rsplit("/", 1)[1])
        self._call("POST", f"{self._path}/collections/{quote(name, safe='')}:drop", {})

    def command(self, command, value=1, **kwargs) -> dict:
        """As PyMongo's db.command(): create, collMod (a view's viewOn and pipeline), drop, explain."""
        document = {command: value, **kwargs} if isinstance(command, str) else {**command, **kwargs}
        return _from_wire(self._call("POST", f"{self._path}:runCommand", _to_wire(document)))


class Client:
    def __init__(self, call, v1: str, raw=None):
        self._call, self._v1, self._raw = call, v1, raw

    def db(self, name: str) -> Database:
        return Database(self._call, f"{self._v1}/bases/{quote(name, safe='')}", self._raw)

    __getitem__ = db
    get_database = db

    def watch(self, pipeline=None, **options) -> ChangeStream:
        """As PyMongo's client.watch(): the changes of the whole cluster."""
        return ChangeStream(self._call, f"{self._v1}:watch", pipeline, **options)

    def start_session(self) -> ClientSession:
        return ClientSession(self._call, self._v1)

    def list_databases(self) -> list:
        """As PyMongo's client.list_databases(): each base's name, sizeOnDisk and empty (a key limited to some: those)."""
        return self._call("GET", f"{self._v1}/bases", None)["databases"]

    def list_database_names(self) -> list:
        """As PyMongo's client.list_database_names()."""
        return [d["name"] for d in self.list_databases()]


def connect(url: str, key: str, *, cluster: str | None = None, timeout: float = 30.0) -> Client:
    """A client for one project (its key), in one of its clusters (by default the first, "Cluster0")."""
    parts = urlsplit(url)
    local = parts.hostname in ("127.0.0.1", "localhost", "::1")
    if parts.scheme != "https" and not (parts.scheme == "http" and local):
        raise IshedError("invalid-argument", "IshedDB is reached over HTTPS (plain HTTP only to this machine)")
    if not isinstance(key, str) or not key.startswith("ished_sk_"):
        raise IshedError("invalid-argument", "a key ished_sk_… is needed")
    if cluster is not None and not _CLUSTER.match(cluster):
        raise IshedError("invalid-argument", 'cluster: 1 to 64 letters, digits or "-"')
    root = url.rstrip("/")
    v1 = "/v1" if cluster is None else f"/v1/clusters/{cluster}"

    def call(method: str, path: str, body: Any, extra: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        headers = {"Authorization": f"Bearer {key}", **(extra or {})}
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(root + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                text = response.read().decode()
                return json.loads(text) if text else None
        except urllib.error.HTTPError as e:
            try:
                err = json.loads(e.read().decode()).get("error", {})
            except ValueError:
                err = {}
            raise IshedError(err.get("code", "internal"), err.get("message", f"HTTP {e.code}"), e.code, err.get("details")) from None

    def raw(method: str, where: str, data: bytes | None, extra: dict, auth: bool = True, stream: bool = False):
        """The files' bytes (gridfs.py): an upload session's own address (no key sent to it), or a download with the
        key; answers (status, the response when [stream] and successful, the JSON answer)."""
        absolute = where.startswith("http://") or where.startswith("https://")
        target = where if absolute else root + where
        t = urlsplit(target)
        if t.scheme != "https" and not (t.scheme == "http" and t.hostname in ("127.0.0.1", "localhost", "::1")):
            raise IshedError("invalid-argument", "a file's address is reached over HTTPS only")
        headers = {**({"Authorization": f"Bearer {key}"} if auth else {}), **extra}
        request = urllib.request.Request(target, data=data, method=method, headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=timeout)
        except urllib.error.HTTPError as e:
            try:
                return e.code, None, json.loads(e.read().decode())
            except ValueError:
                return e.code, None, {}
        if stream:
            return response.status, response, {}
        with response:
            text = response.read().decode()
            return response.status, None, json.loads(text) if text else {}

    return Client(call, v1, raw)


from .gridfs import GridFSBucket, GridIn, GridOut  # noqa: E402  (as PyMongo's gridfs module, beside the client)
