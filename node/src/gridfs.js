// IshedDB's files from Node.js, as MongoDB's GridFSBucket: openUploadStream, openDownloadStream(ByName), find,
// delete, rename, drop, with the same names and arguments. The bytes go to IshedDB in pieces (a resumable upload,
// server/src/files.js) and come back as a stream; a file's document is one of the collection <bucket>.files.
// IshedDB's own: upload() and download() at once, signedUrl() for a browser, startUpload() for a browser's upload.

import { Readable, Writable } from 'node:stream';

// What index.js gives each base (its calls, the errors and the values' conversions): nothing imported from it.
const FILES = Symbol.for('ished.files');

const SEGMENT = 256 * 1024;
const PIECE = 8 * 1024 * 1024; // what goes in one request (a multiple of 256 KB)
const enc = encodeURIComponent;

async function failed(io, res) {
  const text = await res.text();
  let e = {};
  try {
    e = JSON.parse(text)?.error ?? {};
  } catch {}
  return new io.IshedError(e.code ?? 'internal', e.message ?? `HTTP ${res.status}`, { status: res.status, requestId: e.requestId, details: e.details ?? null });
}

// A file's document as GridFS gives it: _id, length, chunkSize, uploadDate, filename, metadata, contentType, sha256.
const gridDocument = (io, wire) => {
  const doc = io.fromWire(wire.data ?? wire);
  return { _id: wire.id ?? doc._id, ...doc };
};

export class GridFSBucket {
  /** As MongoDB's new GridFSBucket(db, { bucketName }): [db] is client.db(name). */
  constructor(db, { bucketName = 'fs' } = {}) {
    if (!db?.[FILES]) throw new TypeError('GridFSBucket(db): a base of IshedDB, client.db(name)');
    if (typeof bucketName !== 'string' || !/^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,93}$/.test(bucketName)) {
      throw new db[FILES].IshedError('invalid-argument', 'bucketName: 1 to 94 letters, digits, "_", "-" or "."');
    }
    this._db = db;
    this._io = db[FILES];
    this.bucketName = bucketName;
    this._path = `${this._io.path}/buckets/${enc(bucketName)}`;
  }

  /** The documents of the files, as MongoDB's bucket.find(filter, options): a cursor (toArray, for await…of). */
  find(filter = {}, options = {}) {
    return this._db.collection(`${this.bucketName}.files`).find(filter, options);
  }

  /**
   * Starts an upload and gives its session: { id, uploadUrl, chunkSize, expiresAt } (or { id, file } for an empty
   * file). For a browser: give it uploadUrl (no key in it), and name its page's [origin].
   */
  async startUpload(filename, { length = null, contentType, metadata, id, sha256, origin } = {}) {
    const body = { filename, length, ...(contentType ? { contentType } : {}), ...(metadata ? { metadata: this._io.toWire(metadata) } : {}), ...(id !== undefined ? { id: String(id) } : {}), ...(sha256 ? { sha256 } : {}), ...(origin ? { origin } : {}) };
    return this._io.call('POST', `${this._path}/uploads`, body);
  }

  // One piece to a session: its address carries its own right (no key sent there).
  async _put(session, bytes, first, total) {
    const path = new URL(session.uploadUrl, 'http://ished.invalid').pathname;
    const res = await this._io.raw(path, {
      method: 'PUT',
      headers: { 'Content-Range': `bytes ${first}-${first + bytes.length - 1}/${total ?? '*'}`, 'Content-Type': 'application/octet-stream' },
      body: bytes,
    });
    if (!res.ok) throw await failed(this._io, res);
    return res.json();
  }

  /**
   * As MongoDB's openUploadStream(filename, { metadata, contentType, id }): a writable stream; its `id` is the file's,
   * and 'finish' comes once IshedDB has checked the whole file. [length] given at once makes no difference but one
   * request less.
   */
  openUploadStream(filename, options = {}) {
    return new UploadStream(this, filename, options);
  }

  /** As MongoDB's openUploadStreamWithId(id, filename, options). */
  openUploadStreamWithId(id, filename, options = {}) {
    return new UploadStream(this, filename, { ...options, id });
  }

  /** Sends a whole file at once ([bytes]: a Buffer, or a stream / async iterable): its document. */
  async upload(filename, bytes, options = {}) {
    const stream = this.openUploadStream(filename, { ...options, ...(Buffer.isBuffer(bytes) ? { length: bytes.length } : {}) });
    const done = new Promise((resolve, reject) => {
      stream.on('finish', () => resolve(stream.file));
      stream.on('error', reject);
    });
    Readable.from(Buffer.isBuffer(bytes) ? [bytes] : bytes).pipe(stream);
    return done;
  }

  /** As MongoDB's openDownloadStream(id, { start, end }): a readable stream of the file's bytes (end excluded). */
  openDownloadStream(id, { start, end } = {}) {
    const self = this;
    return Readable.from(
      (async function* () {
        const range = start !== undefined || end !== undefined ? `bytes=${start ?? 0}-${end !== undefined ? end - 1 : ''}` : null;
        const res = await self._io.raw(`${self._path}/files/${enc(String(id))}:download`, { headers: { ...self._io.auth, ...(range ? { Range: range } : {}) }, signal: null });
        if (!res.ok) throw await failed(self._io, res);
        if (!res.body) return;
        for await (const chunk of res.body) yield Buffer.from(chunk);
      })(),
    );
  }

  /** As MongoDB's openDownloadStreamByName(filename, { revision }): -1 the latest (the default), 0 the first… */
  openDownloadStreamByName(filename, { revision = -1, start, end } = {}) {
    const self = this;
    return Readable.from(
      (async function* () {
        const cursor = self.find({ filename }).sort({ uploadDate: revision >= 0 ? 1 : -1 }).skip(revision >= 0 ? revision : -revision - 1).limit(1);
        const [doc] = await cursor.toArray();
        if (!doc) throw new self._io.IshedError('not-found', `no file named ${filename} (revision ${revision})`);
        yield* self.openDownloadStream(doc._id, { start, end });
      })(),
    );
  }

  /** A whole file's bytes at once (a Buffer). */
  async download(id) {
    const parts = [];
    for await (const chunk of this.openDownloadStream(id)) parts.push(chunk);
    return Buffer.concat(parts);
  }

  /**
   * A signed address for a browser, without a key: [expiresIn] seconds (7 days at most), [disposition] inline or
   * attachment; [preview]: the address of its PDF preview (a Word, PowerPoint or Excel file shown in the page).
   */
  async signedUrl(id, { expiresIn = 3600, disposition = 'attachment', preview = false } = {}) {
    return this._io.call('POST', `${this._path}/files/${enc(String(id))}:signUrl`, { expiresIn, disposition, ...(preview ? { preview: true } : {}) });
  }

  /**
   * A Word, PowerPoint, Excel or OpenDocument file's PDF preview, made once by IshedDB: { state: 'ready', length,
   * stream } once made; else { state }: 'preparing' (being made: ask again in a few seconds), 'unsupported' (no
   * preview for this type: show or download the file itself), 'too-large', 'failed' (with its error), 'unavailable'.
   */
  async preview(id) {
    const res = await this._io.raw(`${this._path}/files/${enc(String(id))}:preview`, { headers: { ...this._io.auth }, signal: null });
    if (res.ok && String(res.headers.get('content-type')).startsWith('application/pdf')) {
      const body = res.body;
      return { state: 'ready', length: Number(res.headers.get('content-length')), stream: Readable.from((async function* () { for await (const c of body) yield Buffer.from(c); })()) };
    }
    if ([202, 413, 415, 422, 503].includes(res.status) && String(res.headers.get('content-type')).startsWith('application/json')) return res.json();
    throw await failed(this._io, res);
  }

  /** As MongoDB's bucket.delete(id). */
  async delete(id) {
    await this._io.call('POST', `${this._path}/files/${enc(String(id))}:delete`, {});
  }

  /** As MongoDB's bucket.rename(id, filename). */
  async rename(id, filename) {
    await this._io.call('POST', `${this._path}/files/${enc(String(id))}:rename`, { filename });
  }

  /** As MongoDB's bucket.drop(): every file of the bucket and its collection. */
  async drop() {
    await this._io.call('POST', `${this._path}:drop`, {});
  }
}

// The writable stream of an upload: pieces of 8 MB, the last one telling the length; a small file in one request.
class UploadStream extends Writable {
  constructor(bucket, filename, options) {
    super();
    this._bucket = bucket;
    this._filename = filename;
    this._options = options;
    this._pending = [];
    this._pendingBytes = 0;
    this._sent = 0;
    this._session = null;
    this.id = options.id !== undefined ? String(options.id) : null;
    this.file = null;
  }

  async _open(length) {
    this._session = await this._bucket.startUpload(this._filename, { ...this._options, length });
    this.id = this._session.id;
  }

  // Sends what is waiting, keeping at least a byte for the last piece (which tells the length).
  async _drain(final) {
    const all = Buffer.concat(this._pending);
    const keep = final ? 0 : Math.max(1, (all.length % SEGMENT) || SEGMENT);
    for (let at = 0; all.length - at > keep; ) {
      const size = Math.min(PIECE, all.length - at - keep);
      const piece = all.subarray(at, at + size);
      const last = final && at + size === all.length;
      const total = last ? this._sent + piece.length : (this._options.length ?? null);
      const out = await this._bucket._put(this._session, piece, this._sent, last ? total : (this._options.length ?? null));
      this._sent += piece.length;
      at += size;
      if (out.file) this.file = gridDocument(this._bucket._io, out.file);
    }
    this._pending = [all.subarray(all.length - (final ? 0 : keep))].filter((b) => b.length);
    this._pendingBytes = this._pending.reduce((n, b) => n + b.length, 0);
  }

  _write(chunk, encoding, done) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding);
    this._pending.push(bytes);
    this._pendingBytes += bytes.length;
    if (this._pendingBytes < PIECE + SEGMENT) return done();
    (async () => {
      if (!this._session) await this._open(this._options.length ?? null);
      await this._drain(false);
    })().then(() => done(), done);
  }

  _final(done) {
    (async () => {
      // A small file (or an empty one): its length known, one request.
      if (!this._session) await this._open(this._pendingBytes);
      if (this._session.file) this.file = gridDocument(this._bucket._io, this._session.file);
      else await this._drain(true);
      if (!this.file) throw new this._bucket._io.IshedError('internal', 'the upload ended without its file');
    })().then(() => done(), done);
  }
}
