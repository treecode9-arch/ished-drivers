// IshedDB's library for Node.js servers.
//
//   import { connect, remove, increment, serverTime } from '@ished/client';
//   const db = connect({ url: 'https://api.ished.dev', key: process.env.ISHED_KEY }).base('DB');
//   const clients = await db.collection('clients').where('ville', '==', 'Dakar').orderBy('nom').get();
//
// Values: JavaScript dates, BigInt and Buffer travel as IshedDB's tagged values and come back as such.
// The key is a server secret: the library refuses to send it over plain HTTP, except to this machine.

import { GridFSBucket as GridFSBucketOf } from './gridfs.js';

export class IshedError extends Error {
  constructor(code, message, { status = null, requestId = null, details = null } = {}) {
    super(message);
    this.name = 'IshedError';
    this.code = code;
    this.status = status;
    this.requestId = requestId;
    // A document refused by the collection's rules: what it breaks ([{ path, rule, expected?, got? }]), as errInfo.
    this.details = details;
  }
}

const SENTINEL = Symbol('ished.sentinel');
const sentinel = (wire) => ({ [SENTINEL]: wire });
/** In an update: removes the field. */
export const remove = () => sentinel({ '~remove': true });
/** In an update: adds [n] to the number (0 when the field is missing). */
export const increment = (n = 1) => sentinel({ '~increment': n });
/** In an update or a definition's default: the server's time. */
export const serverTime = () => sentinel({ '~serverTime': true });
/** Another field of the same document, to compare with: where('dépensé', '>', field('budget')) (MongoDB's $expr). */
export const field = (path) => ({ '~field': path });
/** Multiplies a number (a missing field becomes 0), as MongoDB's $mul. */
export const multiply = (n) => sentinel({ '~multiply': n });
/** Keeps the greater of the field and [value] (MongoDB's $max); atMost keeps the smaller ($min). */
export const atLeast = (value) => sentinel({ '~atLeast': toWire(value) });
export const atMost = (value) => sentinel({ '~atMost': toWire(value) });
/** Moves the field to another name (MongoDB's $rename). */
export const rename = (to) => sentinel({ '~renameTo': to });
/** Adds the values the list does not hold yet, as Firestore's arrayUnion (MongoDB's $addToSet). */
export const arrayUnion = (...values) => sentinel({ '~arrayUnion': values.map(toWire) });
/** Removes every item equal to one of the values, as Firestore's arrayRemove (MongoDB's $pull, $pullAll). */
export const arrayRemove = (...values) => sentinel({ '~arrayRemove': values.map(toWire) });
/** Adds values to the list (MongoDB's $push): { position, slice, sort } as its $position, $slice, $sort. */
export const arrayAppend = (values, { position, slice, sort } = {}) =>
  sentinel({ '~arrayAppend': { values: values.map(toWire), ...(position !== undefined ? { position } : {}), ...(slice !== undefined ? { slice } : {}), ...(sort !== undefined ? { sort } : {}) } });
/** Removes the list's last item (1) or its first (-1), as MongoDB's $pop. */
export const arrayPop = (end = 1) => sentinel({ '~arrayPop': end });

/** A JavaScript value as IshedDB's wire value. */
export function toWire(value) {
  if (value === null) return null;
  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime())) throw new IshedError('invalid-argument', 'invalid date');
    return { '~date': value.toISOString() };
  }
  if (typeof value === 'bigint') return { '~int': value.toString() };
  if (value instanceof Uint8Array) return { '~bytes': Buffer.from(value).toString('base64') };
  if (Array.isArray(value)) return value.map((v) => (v === undefined ? null : toWire(v)));
  // A JavaScript regular expression in a MongoDB filter ({ nom: /^aw/i }), as the driver sends it.
  if (value instanceof RegExp) return { $regex: value.source, $options: value.flags.replace(/[^imsx]/g, '') };
  if (typeof value === 'object') {
    if (value[SENTINEL]) return value[SENTINEL];
    if (typeof value.toHexString === 'function') return value.toHexString(); // an ObjectId from older code
    const out = {};
    for (const [k, v] of Object.entries(value)) if (v !== undefined) out[k] = toWire(v);
    return out;
  }
  return value;
}

/** A wire value as a JavaScript value. */
export function fromWire(value) {
  if (Array.isArray(value)) return value.map(fromWire);
  if (value !== null && typeof value === 'object') {
    const keys = Object.keys(value);
    if (keys.length === 1) {
      if (keys[0] === '~date') return new Date(value['~date']);
      if (keys[0] === '~int') return BigInt(value['~int']);
      if (keys[0] === '~bytes') return Buffer.from(value['~bytes'], 'base64');
    }
    return Object.fromEntries(keys.map((k) => [k, fromWire(value[k])]));
  }
  return value;
}

const document = (wire) =>
  wire && {
    id: wire.id,
    data: fromWire(wire.data),
    createTime: new Date(wire.createTime),
    updateTime: new Date(wire.updateTime),
  };

const enc = encodeURIComponent;
// A collection's options, as MongoDB's listCollections gives them: a view's, a capped collection's, or none.
const optionsOf = (c) => {
  if (c.type === 'view') return { viewOn: c.viewOn, pipeline: c.pipeline };
  if (c.capped) return { capped: true, size: c.capped.size, ...(c.capped.max ? { max: c.capped.max } : {}) };
  if (c.timeseries) {
    const t = c.timeseries;
    const timeseries = { timeField: t.timeField, ...(t.metaField ? { metaField: t.metaField } : {}), ...(t.granularity ? { granularity: t.granularity } : {}), bucketMaxSpanSeconds: t.bucketMaxSpanSeconds, ...(t.bucketRoundingSeconds ? { bucketRoundingSeconds: t.bucketRoundingSeconds } : {}) };
    return { timeseries, ...(t.expireAfterSeconds !== null ? { expireAfterSeconds: t.expireAfterSeconds } : {}) };
  }
  return {};
};
// What a GridFSBucket (gridfs.js) reads of a base; Symbol.for, so that gridfs.js needs nothing from here.
const FILES = Symbol.for('ished.files');
export const MAX_DOCUMENTS = 100_000;
/** As MongoDB's GridFSBucket: a base's files (gridfs.js). */
export const GridFSBucket = GridFSBucketOf;

// The pace of a key, or of a file's address (100 requests a second, 200 at once), is refused before anything is done.
const PACED = 'too many requests: slow down';
const repeatable = (body) => body === undefined || body === null || typeof body === 'string' || body instanceof ArrayBuffer || ArrayBuffer.isView(body);

/**
 * [send]'s answer; a request refused by the pace goes again after a short wait, growing (25 ms to 1 s, at random
 * around it), [waitMs] at most in all, so that a burst slows down instead of failing. Nothing was done the first time:
 * a write is not made twice. Any other answer as it came (its body read once, given back whole).
 */
async function paced(send, waitMs) {
  for (let waited = 0, pause = 25; ; pause = Math.min(pause * 2, 1000)) {
    const res = await send();
    if (res.status !== 429) return res;
    const text = await res.text();
    let message = null;
    try {
      message = JSON.parse(text)?.error?.message ?? null;
    } catch {
      // Not IshedDB's answer: given back as it is.
    }
    if (message !== PACED || waited >= waitMs) return new Response(text, { status: res.status, statusText: res.statusText, headers: res.headers });
    const ms = Math.round(pause * (0.5 + Math.random()));
    await new Promise((resolve) => setTimeout(resolve, ms));
    waited += ms;
  }
}

/**
 * Connects to IshedDB with a project key. [cluster]: the cluster to work in, as in Atlas (by default the project's
 * first cluster, its "Cluster0"). [paceWaitMs]: how long, in all, a request refused by the pace of its key (100 a
 * second, 200 at once) is sent again before its error comes (10 s; 0: at once).
 */
export function connect({ url, key, cluster = null, timeoutMs = 30_000, paceWaitMs = 10_000, fetch: doFetch = globalThis.fetch }) {
  const endpoint = new URL(url);
  const local = ['127.0.0.1', 'localhost', '[::1]'].includes(endpoint.hostname);
  if (endpoint.protocol !== 'https:' && !(endpoint.protocol === 'http:' && local)) {
    throw new IshedError('invalid-argument', 'IshedDB is reached over HTTPS (plain HTTP only to this machine)');
  }
  if (typeof key !== 'string' || !key.startsWith('ished_sk_')) {
    throw new IshedError('invalid-argument', 'a key ished_sk_… is needed');
  }
  if (cluster !== null && !(typeof cluster === 'string' && /^[A-Za-z0-9][A-Za-z0-9-]{0,63}$/.test(cluster))) {
    throw new IshedError('invalid-argument', 'cluster: 1 to 64 letters, digits or "-", starting with a letter or a digit');
  }
  const root = endpoint.toString().replace(/\/$/, '');
  const v1 = cluster === null ? '/v1' : `/v1/clusters/${cluster}`;

  // [signal]: a request its caller may stop (a change stream closed while it waits).
  async function call(method, path, body, headers = {}, signal = null) {
    const send = () =>
      doFetch(`${root}${path}`, {
        method,
        headers: { Authorization: `Bearer ${key}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...headers },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: signal ? AbortSignal.any([AbortSignal.timeout(timeoutMs), signal]) : AbortSignal.timeout(timeoutMs),
      });
    const res = await paced(send, paceWaitMs);
    const text = await res.text();
    const json = text ? JSON.parse(text) : null;
    if (!res.ok) {
      const e = json?.error ?? {};
      throw new IshedError(e.code ?? 'internal', e.message ?? `HTTP ${res.status}`, { status: res.status, requestId: e.requestId, details: e.details ?? null });
    }
    return json;
  }

  // What a GridFSBucket (gridfs.js) needs of its base: the calls, and plain requests for the bytes.
  // A piece sent waits 10 minutes at most; a download as long as its bytes come (signal: null). Sent again after the
  // pace's refusal when its body can be (bytes, not a stream).
  const raw = (path, { signal, ...init } = {}) => {
    const send = () => doFetch(`${root}${path}`, { ...init, ...(signal === null ? {} : { signal: AbortSignal.timeout(Math.max(timeoutMs, 600_000)) }) });
    return repeatable(init.body) ? paced(send, paceWaitMs) : send();
  };
  const base = (name) => ({
    [FILES]: { call, raw, path: `${v1}/bases/${enc(name)}`, auth: { Authorization: `Bearer ${key}` }, IshedError, fromWire, toWire },
    collection: (collection) => new Collection(call, `${v1}/bases/${enc(name)}/collections/${enc(collection)}`),
    collections: () => call('GET', `${v1}/bases/${enc(name)}/collections`),
    /**
     * As MongoDB's db.createCollection(name, options): an empty collection, or a view ({ viewOn, pipeline }: read
     * only, computed when read). Answers the collection.
     */
    createCollection: async (collection, options = {}) => {
      await call('POST', `${v1}/bases/${enc(name)}/collections/${enc(collection)}:create`, toWire(options));
      return new Collection(call, `${v1}/bases/${enc(name)}/collections/${enc(collection)}`);
    },
    /** As MongoDB's db.dropCollection(name): a collection and its documents, or a view. Answers true. */
    dropCollection: async (collection) => (await call('POST', `${v1}/bases/${enc(name)}/collections/${enc(collection)}:drop`, {})).ok === 1,
    /** As MongoDB's db.listCollections(): { name, type ("collection" or "view"), options, info: { readOnly } }. */
    listCollections: () => {
      const all = async () =>
        (await call('GET', `${v1}/bases/${enc(name)}/collections`)).map((c) => ({
          name: c.name,
          type: c.type,
          options: optionsOf(c),
          info: { readOnly: c.type === 'view' },
        }));
      return {
        toArray: all,
        async *[Symbol.asyncIterator]() {
          for (const x of await all()) yield x;
        },
      };
    },
    /** As MongoDB's db.command(document): create, collMod (a view's viewOn and pipeline), drop, explain. */
    command: async (document) => fromWire(await call('POST', `${v1}/bases/${enc(name)}:runCommand`, toWire(document))),
    /** As MongoDB's db.watch(): the changes of every collection of the base. */
    watch: (pipeline = [], options = {}) => new ChangeStream(call, `${v1}/bases/${enc(name)}:watch`, pipeline, options),
  });
  /** As MongoDB's listDatabases: { databases: [{ name, sizeOnDisk, empty }], totalSize, ok } (a limited key: its bases). */
  const listDatabases = () => call('GET', `${v1}/bases`);
  return {
    /** A database of the cluster, as MongoDB's client.db(name). */
    db: base,
    base,
    listDatabases,
    /** As MongoDB's client.db().admin(): listDatabases(). */
    admin: () => ({ listDatabases }),
    /** As MongoDB's client.watch(): the changes of the whole cluster. */
    watch: (pipeline = [], options = {}) => new ChangeStream(call, `${v1}:watch`, pipeline, options),
    /** As MongoDB's client.startSession(): withTransaction(fn), or startTransaction / commitTransaction / abortTransaction. */
    startSession: () => new ClientSession(call, v1),
    /**
     * As Firestore's runTransaction: [fn](t) reads and writes through t (get, create, set, update, delete), all
     * committed together or none; run again (up to [maxAttempts] times) when it meets another transaction.
     */
    runTransaction: async (fn, { maxAttempts = 5 } = {}) => {
      for (let attempt = 1; ; attempt += 1) {
        const session = new ClientSession(call, v1);
        session.startTransaction();
        const t = new Transaction(session);
        try {
          const out = await fn(t);
          await Promise.all(t._pending);
          await session.commitTransaction();
          return out;
        } catch (e) {
          await session.abortTransaction().catch(() => {});
          if (!(e instanceof IshedError && e.code === 'aborted') || attempt >= maxAttempts) throw e;
        }
      }
    },
    /** The cluster's access journal (admin key). */
    audit: (limit = 100) => call('GET', `${v1}/audit?limit=${limit}`),
    verifyAudit: () => call('GET', `${v1}/audit:verify`),
  };
}

const FILTER = Symbol('ished.filter');
function conditionOf(field, op, value) {
  if (op === 'exists' || op === 'missing') return [field, op];
  if (op === 'matches') {
    if (value instanceof RegExp) {
      const flags = value.flags.replace(/[^imsx]/g, '');
      return flags ? [field, op, value.source, flags] : [field, op, value.source];
    }
    return [field, op, value];
  }
  return [field, op, toWire(value)];
}
const filterOf = (f) => {
  if (!f?.[FILTER]) throw new IshedError('invalid-argument', 'Filter.or, and and not take filters made with Filter.where, or, and, not');
  return f[FILTER];
};

/**
 * What to compute over the documents a query finds, as Firestore's AggregateField: count(), sum(field),
 * average(field); and min(field), max(field). query.aggregate({ total: AggregateField.sum('prix') }).get().
 */
export const AggregateField = {
  count: () => ({ $count: {} }),
  sum: (field) => ({ $sum: `$${field}` }),
  average: (field) => ({ $avg: `$${field}` }),
  min: (field) => ({ $min: `$${field}` }),
  max: (field) => ({ $max: `$${field}` }),
};

/**
 * As MongoDB's ClientSession: a transaction held by the server (60 seconds at most), its operations given
 * { session }. withTransaction(fn) runs fn again when the transaction meets another one, as MongoDB's drivers do.
 */
class ClientSession {
  constructor(call, v1) {
    this._call = call;
    this._v1 = v1;
    this._id = null;
    this._starting = null;
  }

  get inTransaction() {
    return this._starting !== null;
  }

  startTransaction() {
    if (this._starting) throw new IshedError('failed-precondition', 'a transaction is already started in this session');
    this._starting = this._call('POST', `${this._v1}:beginTransaction`, {}).then((r) => (this._id = r.transaction));
  }

  // A collection's call inside this session's transaction.
  _bind(call) {
    return async (method, path, body) => {
      if (!this._starting) return call(method, path, body);
      const id = await this._starting;
      return call(method, path, body, { 'X-Ished-Transaction': id });
    };
  }

  async commitTransaction() {
    if (!this._starting) throw new IshedError('failed-precondition', 'no transaction started in this session');
    const id = await this._starting;
    this._starting = null;
    await this._call('POST', `${this._v1}:commit`, { transaction: id });
  }

  async abortTransaction() {
    if (!this._starting) return;
    const starting = this._starting;
    this._starting = null;
    const id = await starting.catch(() => null);
    if (id) await this._call('POST', `${this._v1}:rollback`, { transaction: id }).catch((e) => (e.code === 'not-found' ? null : Promise.reject(e)));
  }

  async withTransaction(fn, { maxAttempts = 5 } = {}) {
    for (let attempt = 1; ; attempt += 1) {
      this.startTransaction();
      try {
        const out = await fn(this);
        await this.commitTransaction();
        return out;
      } catch (e) {
        await this.abortTransaction().catch(() => {});
        if (!(e instanceof IshedError && e.code === 'aborted') || attempt >= maxAttempts) throw e;
      }
    }
  }

  async endSession() {
    await this.abortTransaction();
  }
}

/** As Firestore's Transaction: get (a document or a query), create, set, update, delete, in one transaction. */
class Transaction {
  constructor(session) {
    this._session = session;
    this._pending = [];
  }

  get(refOrQuery) {
    const call = this._session._bind(refOrQuery._call);
    if (refOrQuery instanceof DocumentRef) return new DocumentRef(call, refOrQuery._path, refOrQuery.id).get();
    return new Query(call, refOrQuery._path, refOrQuery._state).get();
  }

  #write(ref, work) {
    const p = work(new DocumentRef(this._session._bind(ref._call), ref._path, ref.id));
    this._pending.push(p.catch(() => {}));
    return p;
  }

  create(ref, data) {
    return this.#write(ref, (r) => r.create(data));
  }

  set(ref, data) {
    return this.#write(ref, (r) => r.set(data));
  }

  update(ref, changes) {
    return this.#write(ref, (r) => r.update(changes));
  }

  delete(ref) {
    return this.#write(ref, (r) => r.delete());
  }
}

/** As MongoDB's aggregation cursor: toArray(), or for await…of; read 1000 at a time. */
/**
 * As MongoDB's ChangeStream: the changes of a collection, a base or the cluster, as they happen.
 *   for await (const change of collection.watch([{ $match: { operationType: 'insert' } }])) { … }
 *   stream.on('change', (change) => …); next(), tryNext(), resumeToken, close().
 * Opened at once: the changes made from then on are seen. Each request waits up to maxAwaitTimeMS (10 s by default,
 * 20 s at most) on the server. A collection dropped or renamed ends its stream (an "invalidate" change, then closed).
 */
export class ChangeStream {
  constructor(call, path, pipeline = [], { resumeAfter, startAfter, startAtOperationTime, fullDocument, fullDocumentBeforeChange, batchSize, maxAwaitTimeMS = 10_000 } = {}) {
    this._call = call;
    this._path = path;
    this._body = { pipeline: toWire(pipeline), ...(fullDocument ? { fullDocument } : {}), ...(fullDocumentBeforeChange ? { fullDocumentBeforeChange } : {}), ...(batchSize ? { batchSize } : {}) };
    this._wait = maxAwaitTimeMS;
    this._position = resumeAfter ?? startAfter ?? null;
    this._at = startAtOperationTime ?? null;
    this._token = this._position;
    this._buffer = [];
    this._listeners = new Map();
    this.closed = false;
    // Closing the stream ends the request that waits: the server frees its place among the waiting ones at once.
    this._abort = new AbortController();
    this._opening = this._position || this._at ? null : this._fetch(0);
    this._opening?.catch(() => {});
  }

  /** The token to resume after the last change given (resumeAfter). */
  get resumeToken() {
    return this._token;
  }

  async _fetch(wait) {
    const body = { ...this._body, maxAwaitTimeMS: wait };
    if (this._position) body.resumeAfter = this._position;
    else if (this._at) body.startAtOperationTime = toWire(this._at);
    let r;
    try {
      r = await this._call('POST', this._path, body, {}, this._abort.signal);
    } catch (e) {
      if (this.closed) return;
      // As the drivers: one more try after a network error, from the same place.
      if (e instanceof IshedError && !['unavailable', 'deadline-exceeded'].includes(e.code)) throw e;
      r = await this._call('POST', this._path, body, {}, this._abort.signal);
    }
    this._at = null;
    this._position = r.resumeToken;
    for (const c of r.changes) this._buffer.push(fromWire(c));
    if (!this._buffer.length) this._token = r.resumeToken;
  }

  async _ready() {
    if (!this._opening) return;
    const opening = this._opening;
    this._opening = null;
    await opening;
  }

  _take() {
    const change = this._buffer.shift();
    this._token = this._buffer.length ? change._id : this._position;
    if (change.operationType === 'invalidate') this.closed = true;
    return change;
  }

  /** The next change, waiting for it; null once the stream is closed. */
  async next() {
    await this._ready();
    for (;;) {
      if (this._buffer.length) return this._take();
      if (this.closed) return null;
      await this._fetch(this._wait);
    }
  }

  /** The next change if one comes within a second (or maxAwaitTimeMS, if shorter), else null. */
  async tryNext() {
    await this._ready();
    if (!this._buffer.length && !this.closed) await this._fetch(Math.min(this._wait, 1000));
    return this._buffer.length ? this._take() : null;
  }

  /** Whether a change comes (waiting for it); false once the stream is closed. */
  async hasNext() {
    await this._ready();
    while (!this._buffer.length && !this.closed) await this._fetch(this._wait);
    return this._buffer.length > 0;
  }

  async *[Symbol.asyncIterator]() {
    try {
      for (;;) {
        const change = await this.next();
        if (change === null) return;
        yield change;
      }
    } finally {
      // Leaving the loop (break) closes the stream, as MongoDB's driver.
      await this.close();
    }
  }

  /** 'change' (each change), 'error', 'close': listening starts with the first 'change' listener. */
  on(event, listener) {
    if (!this._listeners.has(event)) this._listeners.set(event, []);
    this._listeners.get(event).push(listener);
    if (event === 'change' && !this._running) this._run();
    return this;
  }

  _emit(event, value) {
    for (const l of this._listeners.get(event) ?? []) l(value);
  }

  async _run() {
    this._running = true;
    try {
      while (!this.closed) {
        const change = await this.next();
        if (change !== null && this._listeners.has('change')) this._emit('change', change);
      }
    } catch (error) {
      this.closed = true;
      // As an EventEmitter: an error nobody listens to is thrown.
      if (this._listeners.has('error')) this._emit('error', error);
      else queueMicrotask(() => {
        throw error;
      });
    }
    this._emit('close');
  }

  /** Stops the stream (a request already waiting ends on its own, within maxAwaitTimeMS). */
  async close() {
    this.closed = true;
    this._buffer = [];
    this._abort.abort();
  }
}

/**
 * Listening as Firestore's onSnapshot, on top of the collection's change stream (watch): the stream opened first
 * (nothing written in between is missed), the documents read and given, then after every batch of changes the
 * documents read again and given when they differ, with what changed (docChanges: removed, then added and modified
 * in the new order, each with its old and new place, as Firestore's). A collection dropped or renamed: a new stream,
 * the listening goes on. Answers the function that stops it.
 */
function listen(call, collectionPath, read, { pipeline = [], one = false }, onNext, onError) {
  if (typeof onNext !== 'function') throw new IshedError('invalid-argument', 'onSnapshot takes the function that receives each snapshot');
  let stopped = false;
  let stream = null;
  let previous = null; // [{ id, key }], in order
  const keyOf = (d) => `${d.updateTime?.getTime?.() ?? ''}|${JSON.stringify(toWire(d.data))}`;
  const open = async () => {
    stream = new ChangeStream(call, `${collectionPath}:watch`, pipeline, { maxAwaitTimeMS: 5000 });
    await stream._ready();
  };
  const deliver = (found) => {
    const docs = one ? (found ? [found] : []) : found;
    const now = docs.map((d) => ({ id: d.id, key: keyOf(d) }));
    if (one) {
      if (previous && previous.length === now.length && (!now.length || previous[0].key === now[0].key)) return;
      previous = now;
      onNext(found);
      return;
    }
    const changes = [];
    const before = new Map((previous ?? []).map((x) => [x.id, x.key]));
    const after = new Set(now.map((x) => x.id));
    const list = (previous ?? []).map((x) => x.id);
    for (const x of previous ?? []) {
      if (after.has(x.id)) continue;
      const at = list.indexOf(x.id);
      list.splice(at, 1);
      changes.push({ type: 'removed', doc: x.doc, oldIndex: at, newIndex: -1 });
    }
    docs.forEach((d, i) => {
      if (!before.has(d.id)) {
        list.splice(i, 0, d.id);
        changes.push({ type: 'added', doc: d, oldIndex: -1, newIndex: i });
      } else if (before.get(d.id) !== now[i].key) {
        const at = list.indexOf(d.id);
        list.splice(at, 1);
        list.splice(i, 0, d.id);
        changes.push({ type: 'modified', doc: d, oldIndex: at, newIndex: i });
      }
    });
    if (previous && !changes.length) return;
    previous = now.map((x, i) => ({ ...x, doc: docs[i] }));
    onNext({
      docs,
      size: docs.length,
      empty: docs.length === 0,
      readTime: new Date(),
      docChanges: () => changes,
      forEach: (fn) => docs.forEach(fn),
    });
  };
  (async () => {
    try {
      await open();
      deliver(await read());
      while (!stopped) {
        const change = await stream.next();
        if (stopped) break;
        if (change === null || stream.closed) await open();
        // Every change already there, then one reading.
        stream._buffer.length = 0;
        if (stopped) break;
        deliver(await read());
      }
    } catch (error) {
      if (stopped) return;
      stopped = true;
      if (typeof onError === 'function') onError(error);
      else queueMicrotask(() => {
        throw error;
      });
    }
  })();
  return () => {
    stopped = true;
    stream?.close();
  };
}

class AggregationCursor {
  constructor(call, path, pipeline, options) {
    this._call = call;
    this._path = path;
    this._pipeline = toWire(pipeline);
    this._options = options;
  }

  /** As MongoDB's aggregate(…).explain(verbosity): the pipeline's plan ($cursor, then its stages). */
  async explain(verbosity) {
    const name = decodeURIComponent(this._path.split('/').pop());
    return this._call('POST', `${this._path}:explain`, { explain: { aggregate: name, pipeline: this._pipeline }, ...verbosityOf(verbosity) });
  }

  async *[Symbol.asyncIterator]() {
    for (let skip = 0; ; skip += 1000) {
      const r = await this._call('POST', `${this._path}:aggregate`, { pipeline: this._pipeline, limit: 1000, skip, ...(this._options.reveal ? { reveal: this._options.reveal } : {}) });
      for (const d of r.documents) yield fromWire(d);
      if (!r.more) return;
    }
  }

  async toArray() {
    const out = [];
    for await (const d of this) {
      out.push(d);
      if (out.length > MAX_DOCUMENTS) throw new IshedError('resource-exhausted', `more than ${MAX_DOCUMENTS} documents: add $limit`);
    }
    return out;
  }
}

/** Groups of conditions, as Firestore's Filter: Filter.where(…), Filter.or(…), Filter.and(…), Filter.not(…). */
export const Filter = {
  where: (field, op, value) => ({ [FILTER]: conditionOf(field, op, value) }),
  or: (...filters) => ({ [FILTER]: { or: filters.map(filterOf) } }),
  and: (...filters) => ({ [FILTER]: { and: filters.map(filterOf) } }),
  not: (filter) => ({ [FILTER]: { not: filterOf(filter) } }),
};

class Query {
  constructor(call, path, state = { where: [], orderBy: [], limit: null, offset: 0, select: null, reveal: [] }) {
    this._call = call;
    this._path = path;
    this._state = state;
  }

  _with(change) {
    return new this.constructor(this._call, this._path, { ...this._state, ...change });
  }

  /**
   * A condition: where('âge', '>', 40), where('email', 'exists'), where('nom', 'matches', /^aw/i); or a group made
   * with Filter, as Firestore's: where(Filter.or(Filter.where('ville', '==', 'Dakar'), Filter.where('âge', '>', 60))).
   */
  where(field, op, value) {
    const condition = field?.[FILTER] ?? conditionOf(field, op, value);
    return this._with({ where: [...this._state.where, condition] });
  }

  orderBy(field, direction = 'asc') {
    return this._with({ orderBy: [...this._state.orderBy, [field, direction]] });
  }

  limit(n) {
    return this._with({ limit: n });
  }

  offset(n) {
    return this._with({ offset: n });
  }

  /** Only these fields in the results. */
  select(...fields) {
    return this._with({ select: fields.flat() });
  }

  /** Also these hidden fields (a definition's `hidden: true`). */
  reveal(...fields) {
    return this._with({ reveal: fields.flat() });
  }

  _body(extra = {}) {
    const { where, orderBy, select, reveal } = this._state;
    return { where, orderBy, ...(select ? { select } : {}), ...(reveal.length ? { reveal } : {}), ...extra };
  }

  /** Every document found (page after page), up to the limit. */
  async get() {
    const wanted = this._state.limit ?? MAX_DOCUMENTS;
    const out = [];
    for await (const page of this.pages({ size: Math.min(1000, wanted) })) {
      out.push(...page.slice(0, wanted - out.length));
      if (out.length >= wanted) break;
    }
    return out;
  }

  /**
   * The documents found, a page at a time ({ size }: 1 to 1000), as Google's APIs page their lists (pageToken):
   * each document once, in the query's order, even when documents are added or deleted between two pages.
   *   for await (const page of clients.where('ville', '==', 'Dakar').orderBy('nom').pages({ size: 200 })) { … }
   */
  async *pages({ size = 1000 } = {}) {
    let pageToken = null;
    let offset = this._state.offset;
    do {
      const r = await this._call('POST', `${this._path}:query`, this._body({ limit: size, ...(pageToken ? { pageToken } : { offset }) }));
      offset = 0;
      if (r.documents.length) yield r.documents.map(document);
      pageToken = r.nextPageToken ?? null;
    } while (pageToken);
  }

  /**
   * As Firestore's query.onSnapshot(onNext, onError): onNext(snapshot) at once with the documents found ({ docs,
   * size, empty, readTime, docChanges(), forEach }), then each time a change of the collection changes them.
   * Answers the function that stops listening. A query with a limit keeps the listening light.
   */
  onSnapshot(onNext, onError) {
    return listen(this._call, this._path, () => this.get(), {}, onNext, onError);
  }

  /** The first document found, or null. */
  async first() {
    return (await this.limit(1).get())[0] ?? null;
  }

  async count() {
    return (await this._call('POST', `${this._path}:count`, this._body())).count;
  }

  async exists() {
    return (await this._call('POST', `${this._path}:exists`, this._body())).exists;
  }

  /**
   * A MongoDB pipeline ([{ $group: … }, …]): a cursor, as MongoDB's aggregate. Or, as Firestore's aggregation
   * queries, fields to compute over the documents found: aggregate({ n: AggregateField.count() }).get() → .data().
   */
  aggregate(spec, { reveal, session } = {}) {
    if (Array.isArray(spec)) return new AggregationCursor(session ? session._bind(this._call) : this._call, this._path, [...(this._state.where.length ? [{ where: this._state.where }] : []), ...spec], { reveal });
    const fields = spec;
    return {
      get: async () => {
        const [doc] = await new AggregationCursor(this._call, this._path, [{ where: this._state.where }, { $group: { _id: null, ...fields } }], { reveal }).toArray();
        const empty = (f) => (f.$count || f.$sum ? 0 : null);
        const values = Object.fromEntries(Object.entries(fields).map(([k, f]) => [k, doc ? doc[k] : empty(f)]));
        return { data: () => values };
      },
    };
  }

  /** The different values of [field] among the documents found (a list's items one by one). */
  async distinct(field) {
    const r = await this._call('POST', `${this._path}:distinct`, { path: field, where: this._state.where, ...(this._state.reveal.length ? { reveal: this._state.reveal } : {}) });
    return r.values.map(fromWire);
  }

  /** Changes every document found ('statut': 'vu', relances: increment(1)); answers how many. */
  async updateAll(changes) {
    const r = await this._call('POST', `${this._path}:updateAll`, { where: this._state.where, ...(this._state.reveal.length ? { reveal: this._state.reveal } : {}), changes: toWire(changes) });
    return r.updated;
  }

  /** How many documents per value of [field]: [{ value, count }]. */
  async countBy(field, { order = 'desc', limit = 100 } = {}) {
    const r = await this._call('POST', `${this._path}:countBy`, { where: this._state.where, path: field, order, limit });
    return r.groups.map((g) => ({ value: fromWire(g.value), count: g.count }));
  }

  /**
   * Changes the first document found, in one step; with { create: true }, makes it when none is found, with the
   * changes of { onInsert } too (made only then, as MongoDB's $setOnInsert).
   */
  async updateFirst(changes, { create = false, onInsert } = {}) {
    const r = await this._call('POST', `${this._path}:updateFirst`, {
      where: this._state.where,
      orderBy: this._state.orderBy,
      reveal: this._state.reveal,
      changes: toWire(changes),
      create,
      ...(onInsert ? { onInsert: toWire(onInsert) } : {}),
    });
    return document(r.document);
  }

  /** Deletes every document found; answers how many. */
  async delete() {
    return (await this._call('POST', `${this._path}:deleteWhere`, { where: this._state.where })).deleted;
  }

  /** Deletes the first document found; answers 1 or 0. */
  async deleteFirst() {
    return (await this._call('POST', `${this._path}:deleteWhere`, { where: this._state.where, orderBy: this._state.orderBy, first: true }))
      .deleted;
  }
}

class Collection extends Query {
  /**
   * As MongoDB's createIndex(keys, options): { a: 1, b: -1 } (compound), options unique, name,
   * partialFilterExpression; or a text index { titre: 'text' } with weights and default_language. Answers its name.
   */
  async createIndex(keys, options = {}) {
    return (await this._call('POST', `${this._path}:createIndex`, toWire({ key: keys, ...options }))).name;
  }

  /** As MongoDB's createIndexes([{ key, name, unique, … }]): answers their names. */
  async createIndexes(specs) {
    return (await this._call('POST', `${this._path}:createIndexes`, toWire({ indexes: specs }))).names;
  }

  /** As MongoDB's indexes(): the collection's indexes ({ v, key, name, unique, partialFilterExpression… }). */
  async indexes() {
    return (await this._call('POST', `${this._path}:listIndexes`, {})).indexes;
  }

  /** As MongoDB's listIndexes(): a cursor of the indexes (toArray, for await). */
  listIndexes() {
    const all = () => this.indexes();
    return {
      toArray: all,
      async *[Symbol.asyncIterator]() {
        for (const x of await all()) yield x;
      },
    };
  }

  /** As MongoDB's collection.drop(): the collection and its documents, or the view. Answers true. */
  async drop() {
    return (await this._call('POST', `${this._path}:drop`, {})).ok === 1;
  }

  /** As MongoDB's dropIndex(name or keys). */
  async dropIndex(index) {
    return this._call('POST', `${this._path}:dropIndex`, { index });
  }

  /** As MongoDB's dropIndexes(): all but _id_. */
  async dropIndexes() {
    return this._call('POST', `${this._path}:dropIndexes`, {});
  }

  /** As MongoDB's collection.watch(pipeline, options): the collection's changes as they happen (ChangeStream). */
  watch(pipeline = [], options = {}) {
    return new ChangeStream(this._call, `${this._path}:watch`, pipeline, options);
  }

  doc(id) {
    return new DocumentRef(this._call, `${this._path}/documents/${enc(id)}`, id);
  }

  /** Creates a document with a new id. */
  async add(data) {
    return document(await this._call('POST', `${this._path}/documents`, { data: toWire(data) }));
  }

  /** The collection's definition (fields, unique, indexes, expireAt, timestamps, validation): admin key. */
  async define(definition) {
    return this._call('PUT', `${this._path}/definition`, { definition: toWire(definition) });
  }

  async definition() {
    return this._call('GET', `${this._path}/definition`);
  }

  // ---- MongoDB's methods, for code written for MongoDB's drivers (the server translates them, mongo-api.js) ----

  /** A cursor over the documents a MongoDB filter finds: .sort(), .limit(), .skip(), .project(), .toArray(). */
  find(filter = {}, options = {}) {
    return new FindCursor(this._callFor(options), this._path, filter, options);
  }

  async findOne(filter = {}, options = {}) {
    const r = await this._callFor(options)('POST', `${this._path}:findOne`, mongoBody(filter, options));
    return r.document ? fromWire(r.document) : null;
  }

  /** As MongoDB's: the collection's number of documents, from the statistics (at once on a big collection). */
  async estimate() {
    return (await this._call('POST', `${this._path}:estimate`, {})).count;
  }

  async estimatedDocumentCount(options = {}) {
    return (await this._callFor(options)('POST', `${this._path}:estimatedDocumentCount`, {})).count;
  }

  /** As MongoDB's: the collection's options, as listCollections gives them ({ capped: true, size, max } if capped). */
  async options() {
    const at = this._path.lastIndexOf('/');
    const name = decodeURIComponent(this._path.slice(at + 1));
    const found = (await this._call('GET', this._path.slice(0, at))).find((c) => c.name === name);
    return found ? optionsOf(found) : {};
  }

  /** As MongoDB's: whether the collection is capped. */
  async isCapped() {
    return (await this.options()).capped === true;
  }

  /** As MongoDB's distinct(key, filter); without a filter, the conditions given by where(…). */
  async distinct(key, filter, options = {}) {
    if (filter === undefined) return super.distinct(key);
    return (await this._callFor(options)('POST', `${this._path}:distinct`, { key, filter: toWire(filter) })).values.map(fromWire);
  }

  // The call of a MongoDB method: in its session's transaction when { session } is given.
  _callFor(options) {
    return options?.session ? options.session._bind(this._call) : this._call;
  }

  async countDocuments(filter = {}, options = {}) {
    return (await this._callFor(options)('POST', `${this._path}:countDocuments`, { filter: toWire(filter) })).count;
  }

  /** Adds a document; its `_id` (a text) is kept when given, made otherwise. */
  async insertOne(doc, options = {}) {
    return this._callFor(options)('POST', `${this._path}:insertOne`, { document: toWire(doc) });
  }

  /** Adds documents, 1000 per request (each request all or nothing, as MongoDB's ordered inserts stop at a failure). */
  async insertMany(docs, options = {}) {
    const insertedIds = {};
    for (let start = 0; start < docs.length; start += 1000) {
      const r = await this._callFor(options)('POST', `${this._path}:insertMany`, { documents: docs.slice(start, start + 1000).map(toWire) });
      for (const [i, id] of Object.entries(r.insertedIds)) insertedIds[start + Number(i)] = id;
    }
    return { acknowledged: true, insertedCount: Object.keys(insertedIds).length, insertedIds };
  }

  async updateOne(filter, update, options = {}) {
    return this._callFor(options)('POST', `${this._path}:updateOne`, { filter: toWire(filter), update: toWire(update), upsert: options.upsert === true });
  }

  async updateMany(filter, update, options = {}) {
    return this._callFor(options)('POST', `${this._path}:updateMany`, { filter: toWire(filter), update: toWire(update) });
  }

  async replaceOne(filter, replacement, options = {}) {
    return this._callFor(options)('POST', `${this._path}:replaceOne`, { filter: toWire(filter), replacement: toWire(replacement), upsert: options.upsert === true });
  }

  async deleteOne(filter = {}, options = {}) {
    return this._callFor(options)('POST', `${this._path}:deleteOne`, { filter: toWire(filter) });
  }

  async deleteMany(filter = {}, options = {}) {
    return this._callFor(options)('POST', `${this._path}:deleteMany`, { filter: toWire(filter) });
  }

  /**
   * As MongoDB's bulkWrite: [{ insertOne: { document } }, { updateOne: { filter, update, upsert } }, …], in order;
   * { ordered: false } goes on past a refusal. A refusal throws, its details holding writeErrors and what was done.
   */
  async bulkWrite(operations, options = {}) {
    return this._callFor(options)('POST', `${this._path}:bulkWrite`, { operations: toWire(operations), ordered: options.ordered !== false });
  }

  /** Imports documents as they are, ids kept ([{ id, data }], at most 1000 per call): admin key. */
  async import(documents) {
    const r = await this._call('POST', `${this._path}:import`, {
      documents: documents.map((d) => ({ id: d.id, data: toWire(d.data) })),
    });
    return r.imported;
  }
}

// MongoDB's verbosities, and the driver's booleans (true: allPlansExecution; false: queryPlanner).
const verbosityOf = (v) => (v === undefined ? {} : { verbosity: v === true ? 'allPlansExecution' : v === false ? 'queryPlanner' : v });

const mongoBody = (filter, { sort, projection, limit, skip, reveal, pageToken } = {}) => ({
  filter: toWire(filter),
  ...(sort ? { sort } : {}),
  ...(projection ? { projection } : {}),
  ...(limit ? { limit } : {}),
  ...(skip ? { skip } : {}),
  ...(reveal ? { reveal } : {}),
  ...(pageToken ? { pageToken } : {}),
});

/** As MongoDB's cursor: options set by chained calls, the documents read by toArray() or for await…of. */
class FindCursor {
  constructor(call, path, filter, options) {
    this._call = call;
    this._path = path;
    this._filter = filter;
    this._options = { ...options };
  }

  sort(sort) {
    this._options.sort = sort;
    return this;
  }

  limit(n) {
    this._options.limit = n;
    return this;
  }

  skip(n) {
    this._options.skip = n;
    return this;
  }

  project(projection) {
    this._options.projection = projection;
    return this;
  }

  /**
   * As MongoDB's cursor.explain(verbosity): the find's plan (queryPlanner.winningPlan, and with "executionStats" or
   * "allPlansExecution", the default, executionStats: documents and index keys examined, time).
   */
  async explain(verbosity) {
    const name = decodeURIComponent(this._path.split('/').pop());
    const { filter, sort, projection, limit, skip } = mongoBody(this._filter, this._options);
    return this._call('POST', `${this._path}:explain`, { explain: { find: name, filter, ...(sort ? { sort } : {}), ...(projection ? { projection } : {}), ...(skip ? { skip } : {}), ...(limit ? { limit } : {}) }, ...verbosityOf(verbosity) });
  }

  /** Every document found, read 1000 at a time (at most 100 000, as the rest of this library). */
  async toArray() {
    const out = [];
    for await (const doc of this) {
      out.push(doc);
      if (out.length > MAX_DOCUMENTS) throw new IshedError('resource-exhausted', `more than ${MAX_DOCUMENTS} documents: narrow the filter`);
    }
    return out;
  }

  /** The documents one by one, read a page of 1000 at a time as they are used (MongoDB's batches). */
  async *[Symbol.asyncIterator]() {
    const { limit = 0, skip = 0 } = this._options;
    let given = 0;
    let pageToken = null;
    do {
      const want = limit ? Math.min(1000, limit - given) : 1000;
      const r = await this._call('POST', `${this._path}:find`, mongoBody(this._filter, { ...this._options, ...(pageToken ? { pageToken } : { skip }), limit: want }));
      for (const d of r.documents) {
        yield fromWire(d);
        given += 1;
      }
      pageToken = limit && given >= limit ? null : (r.nextPageToken ?? null);
    } while (pageToken);
  }
}

class DocumentRef {
  constructor(call, path, id) {
    this._call = call;
    this._path = path;
    this.id = id;
  }

  /**
   * As Firestore's documentRef.onSnapshot(onNext, onError): onNext(document or null) at once, then each time the
   * document changes (null once deleted). Answers the function that stops listening.
   */
  onSnapshot(onNext, onError) {
    const collection = this._path.slice(0, this._path.lastIndexOf('/documents/'));
    return listen(this._call, collection, () => this.get(), { pipeline: [{ $match: { 'documentKey._id': this.id } }], one: true }, onNext, onError);
  }

  /** The document, or null. Options: { reveal: [...], select: [...] }. */
  async get({ reveal = [], select = [] } = {}) {
    const params = new URLSearchParams();
    if (reveal.length) params.set('reveal', reveal.join(','));
    if (select.length) params.set('select', select.join(','));
    try {
      return document(await this._call('GET', `${this._path}${params.size ? `?${params}` : ''}`));
    } catch (error) {
      if (error.code === 'not-found') return null;
      throw error;
    }
  }

  /** Creates the document with this id; fails when it exists. */
  async create(data) {
    const collectionPath = this._path.slice(0, this._path.lastIndexOf('/documents/'));
    return document(await this._call('POST', `${collectionPath}/documents`, { id: this.id, data: toWire(data) }));
  }

  /** Creates the document or replaces all its data. */
  async set(data) {
    return document(await this._call('PUT', this._path, { data: toWire(data) }));
  }

  /** Changes some fields ('adresse.ville': 'Dakar', notes: remove(), vues: increment(1)). */
  async update(changes) {
    return document(await this._call('PATCH', this._path, { changes: toWire(changes) }));
  }

  /** Deletes the document; answers whether it existed. */
  async delete() {
    return (await this._call('DELETE', this._path)).deleted;
  }
}
