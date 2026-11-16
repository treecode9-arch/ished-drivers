// IshedDB for .NET, written as MongoDB's C# driver is:
//   var clients = new IshedClient(url, key).GetDatabase("boutique").GetCollection("clients");
//   var dakar = clients.Find(new Document { ["ville"] = "Dakar" }).Sort(new Document { ["age"] = -1 }).ToList();
// Only .NET's own libraries. The key never travels but over HTTPS (or to this machine). Values: DateTime <-> dates,
// long beyond 2**53 <-> big integers, byte[] <-> bytes.
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Ished;

/// <summary>A document, a filter, a sort or an update — as MongoDB's BsonDocument (its keys keep their order).</summary>
public sealed class Document : Dictionary<string, object?>
{
    public Document() { }
    public Document(IDictionary<string, object?> other) : base(other) { }
}

/// <summary>A refusal from IshedDB: its code (invalid-argument, not-found…) and message.</summary>
public sealed class IshedException(string code, string message, int? status = null) : Exception($"{code}: {message}")
{
    public string Code { get; } = code;
    public string Reason { get; } = message;
    public int? Status { get; } = status;
}

public sealed record UpdateResult(long MatchedCount, long ModifiedCount, string? UpsertedId);
public sealed record DeleteResult(long DeletedCount);
public sealed record BulkWriteResult(long InsertedCount, long MatchedCount, long ModifiedCount, long DeletedCount, long UpsertedCount);

/// <summary>One operation of BulkWrite, as the driver's WriteModel.</summary>
public abstract class WriteModel { internal abstract Document Operation(); }
public sealed class InsertOneModel(Document document) : WriteModel { internal override Document Operation() => new() { ["insertOne"] = new Document { ["document"] = document } }; }
public sealed class UpdateOneModel(Document filter, Document update) : WriteModel
{
    public bool IsUpsert { get; init; }
    internal override Document Operation() => new() { ["updateOne"] = new Document { ["filter"] = filter, ["update"] = update, ["upsert"] = IsUpsert } };
}
public sealed class UpdateManyModel(Document filter, Document update) : WriteModel { internal override Document Operation() => new() { ["updateMany"] = new Document { ["filter"] = filter, ["update"] = update } }; }
public sealed class ReplaceOneModel(Document filter, Document replacement) : WriteModel
{
    public bool IsUpsert { get; init; }
    internal override Document Operation() => new() { ["replaceOne"] = new Document { ["filter"] = filter, ["replacement"] = replacement, ["upsert"] = IsUpsert } };
}
public sealed class DeleteOneModel(Document filter) : WriteModel { internal override Document Operation() => new() { ["deleteOne"] = new Document { ["filter"] = filter } }; }
public sealed class DeleteManyModel(Document filter) : WriteModel { internal override Document Operation() => new() { ["deleteMany"] = new Document { ["filter"] = filter } }; }
public sealed class UpdateOptions { public bool IsUpsert { get; set; } }

internal static class Wire
{
    private const long SafeInt = (1L << 53) - 1;

    public static void Write(Utf8JsonWriter w, object? v)
    {
        switch (v)
        {
            case null: w.WriteNullValue(); break;
            case string s: w.WriteStringValue(s); break;
            case bool b: w.WriteBooleanValue(b); break;
            case DateTime d:
                w.WriteStartObject(); w.WriteString("~date", d.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'")); w.WriteEndObject(); break;
            case DateTimeOffset o:
                Write(w, o.UtcDateTime); break;
            case byte[] bytes:
                w.WriteStartObject(); w.WriteString("~bytes", Convert.ToBase64String(bytes)); w.WriteEndObject(); break;
            case int i: w.WriteNumberValue(i); break;
            case long l when Math.Abs(l) <= SafeInt: w.WriteNumberValue(l); break;
            case long l:
                w.WriteStartObject(); w.WriteString("~int", l.ToString()); w.WriteEndObject(); break;
            case double f: w.WriteNumberValue(f); break;
            case decimal m: w.WriteNumberValue(m); break;
            case IDictionary<string, object?> map:
                w.WriteStartObject();
                foreach (var (k, x) in map) { w.WritePropertyName(k); Write(w, x); }
                w.WriteEndObject(); break;
            case System.Collections.IEnumerable list:
                w.WriteStartArray();
                foreach (var x in list) Write(w, x);
                w.WriteEndArray(); break;
            default: throw new IshedException("invalid-argument", $"a value of type {v.GetType().Name} cannot be kept");
        }
    }

    public static string Json(object? v)
    {
        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream)) Write(w, v);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    public static object? Read(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Object => ReadObject(e),
        JsonValueKind.Array => e.EnumerateArray().Select(Read).ToList(),
        JsonValueKind.String => e.GetString(),
        JsonValueKind.Number => e.TryGetInt64(out var l) ? (object)l : e.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        _ => null,
    };

    private static object? ReadObject(JsonElement e)
    {
        var props = e.EnumerateObject().ToList();
        if (props.Count == 1)
        {
            var (k, v) = (props[0].Name, props[0].Value);
            if (k == "~date") return DateTime.Parse(v.GetString()!, null, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal);
            if (k == "~int") return long.Parse(v.GetString()!);
            if (k == "~bytes") return Convert.FromBase64String(v.GetString()!);
        }
        var doc = new Document();
        foreach (var p in props) doc[p.Name] = Read(p.Value);
        return doc;
    }
}

/// <summary>As the driver's IFindFluent: Sort, Limit, Skip, Project, then ToList or FirstOrDefault. Read 1000 at a time.</summary>
public sealed class FindFluent
{
    public const int MaxDocuments = 100_000;
    private readonly IshedCollection collection;
    private readonly Document filter;
    private Document? sort, projection;
    private int limit, skip;

    internal FindFluent(IshedCollection collection, Document filter) { this.collection = collection; this.filter = filter; }

    public FindFluent Sort(Document s) { sort = s; return this; }
    public FindFluent Limit(int n) { limit = n; return this; }
    public FindFluent Skip(int n) { skip = n; return this; }
    public FindFluent Project(Document p) { projection = p; return this; }
    public Document? FirstOrDefault() { limit = 1; return ToList().FirstOrDefault(); }

    public List<Document> ToList()
    {
        var output = new List<Document>();
        string? token = null;
        while (true)
        {
            var want = limit > 0 ? Math.Min(1000, limit - output.Count) : 1000;
            var body = new Document { ["filter"] = filter, ["limit"] = want };
            // The next page starts after the last one read (its token), never at an offset that moves with writes.
            if (token is not null) body["pageToken"] = token; else body["skip"] = skip;
            if (sort is not null) body["sort"] = sort;
            if (projection is not null) body["projection"] = projection;
            var page = collection.Call("find", body);
            output.AddRange(((List<object?>)page["documents"]!).Cast<Document>());
            token = page.TryGetValue("nextPageToken", out var next) ? next as string : null;
            if (token is null || (limit > 0 && output.Count >= limit)) return output;
            if (output.Count >= MaxDocuments) throw new IshedException("resource-exhausted", $"more than {MaxDocuments} documents: narrow the filter");
        }
    }
}

public sealed class IshedCollection
{
    private readonly IshedClient client;
    private readonly string path;
    private readonly ClientSessionHandle? session;

    internal IshedCollection(IshedClient client, string path, ClientSessionHandle? session = null) { this.client = client; this.path = path; this.session = session; }

    internal Document Call(string method, Document body) => (Document)client.Send(HttpMethod.Post, $"{path}:{method}", body, session?.Transaction)!;

    /// <summary>As the driver's Drop(): the collection and its documents, or the view.</summary>
    public void Drop() => Call("drop", new Document());

    // The driver's overloads, session first: the request runs in the session's transaction.
    private IshedCollection In(ClientSessionHandle s) => new(client, path, s);
    public FindFluent Find(ClientSessionHandle session, Document filter) => In(session).Find(filter);
    public long CountDocuments(ClientSessionHandle session, Document filter) => In(session).CountDocuments(filter);
    public List<Document> Aggregate(ClientSessionHandle session, IEnumerable<Document> pipeline) => In(session).Aggregate(pipeline);
    public BulkWriteResult BulkWrite(ClientSessionHandle session, IEnumerable<WriteModel> requests, bool isOrdered = true) => In(session).BulkWrite(requests, isOrdered);
    public List<object?> Distinct(ClientSessionHandle session, string field, Document? filter = null) => In(session).Distinct(field, filter);
    public void InsertOne(ClientSessionHandle session, Document document) => In(session).InsertOne(document);
    public void InsertMany(ClientSessionHandle session, IEnumerable<Document> documents) => In(session).InsertMany(documents);
    public UpdateResult UpdateOne(ClientSessionHandle session, Document filter, Document update, UpdateOptions? options = null) => In(session).UpdateOne(filter, update, options);
    public UpdateResult UpdateMany(ClientSessionHandle session, Document filter, Document update) => In(session).UpdateMany(filter, update);
    public UpdateResult ReplaceOne(ClientSessionHandle session, Document filter, Document replacement, UpdateOptions? options = null) => In(session).ReplaceOne(filter, replacement, options);
    public DeleteResult DeleteOne(ClientSessionHandle session, Document filter) => In(session).DeleteOne(filter);
    public DeleteResult DeleteMany(ClientSessionHandle session, Document filter) => In(session).DeleteMany(filter);

    /// <summary>As the driver's Watch: the collection's changes as they happen.</summary>
    public ChangeStreamCursor Watch(IEnumerable<Document>? pipeline = null, ChangeStreamOptions? options = null) => new(client, $"{path}:watch", pipeline, options);

    /// <summary>As the driver's collection.Indexes.</summary>
    public IndexManager Indexes => new(this);

    public FindFluent Find(Document filter) => new(this, filter);
    public long CountDocuments(Document filter) => (long)Call("countDocuments", new Document { ["filter"] = filter })["count"]!;

    /// <summary>As the driver's Aggregate: the pipeline run by the server; every result, read 1000 at a time.</summary>
    public List<Document> Aggregate(IEnumerable<Document> pipeline)
    {
        var output = new List<Document>();
        var stages = pipeline.Cast<object?>().ToList();
        for (var skip = 0; ; skip += 1000)
        {
            var page = Call("aggregate", new Document { ["pipeline"] = stages, ["limit"] = 1000, ["skip"] = skip });
            output.AddRange(((List<object?>)page["documents"]!).Cast<Document>());
            if (!(bool)page["more"]!) return output;
            if (output.Count >= FindFluent.MaxDocuments) throw new IshedException("resource-exhausted", $"more than {FindFluent.MaxDocuments} documents: add $limit");
        }
    }

    /// <summary>As the driver's BulkWrite: the operations in order; isOrdered false goes on past a refusal.</summary>
    public BulkWriteResult BulkWrite(IEnumerable<WriteModel> requests, bool isOrdered = true)
    {
        var r = Call("bulkWrite", new Document { ["operations"] = requests.Select(m => (object?)m.Operation()).ToList(), ["ordered"] = isOrdered });
        return new BulkWriteResult((long)r["insertedCount"]!, (long)r["matchedCount"]!, (long)r["modifiedCount"]!, (long)r["deletedCount"]!, (long)r["upsertedCount"]!);
    }

    /// <summary>The collection's number of documents, from the statistics (at once on a big collection).</summary>
    public long EstimatedDocumentCount() => (long)Call("estimatedDocumentCount", new Document())["count"]!;

    /// <summary>The different values of a field among the documents found (a list's items one by one).</summary>
    public List<object?> Distinct(string field, Document? filter = null) =>
        (List<object?>)Call("distinct", new Document { ["key"] = field, ["filter"] = filter ?? new Document() })["values"]!;

    /// <summary>Adds a document; as the driver does, its "_id" is set in it when it had none.</summary>
    public void InsertOne(Document document) => document["_id"] = (string)Call("insertOne", new Document { ["document"] = document })["insertedId"]!;

    public void InsertMany(IEnumerable<Document> documents)
    {
        foreach (var chunk in documents.Chunk(1000))
        {
            var ids = (Document)Call("insertMany", new Document { ["documents"] = chunk.ToList() })["insertedIds"]!;
            for (var i = 0; i < chunk.Length; i++) chunk[i]["_id"] = (string)ids[i.ToString()]!;
        }
    }

    private UpdateResult Change(string method, Document filter, string key, Document value, bool? upsert)
    {
        var body = new Document { ["filter"] = filter, [key] = value };
        if (upsert is not null) body["upsert"] = upsert;
        var r = Call(method, body);
        return new UpdateResult((long)r["matchedCount"]!, (long)r["modifiedCount"]!, r.GetValueOrDefault("upsertedId") as string);
    }

    public UpdateResult UpdateOne(Document filter, Document update, UpdateOptions? options = null) => Change("updateOne", filter, "update", update, options?.IsUpsert ?? false);
    public UpdateResult UpdateMany(Document filter, Document update) => Change("updateMany", filter, "update", update, null);
    public UpdateResult ReplaceOne(Document filter, Document replacement, UpdateOptions? options = null) => Change("replaceOne", filter, "replacement", replacement, options?.IsUpsert ?? false);
    public DeleteResult DeleteOne(Document filter) => new((long)Call("deleteOne", new Document { ["filter"] = filter })["deletedCount"]!);
    public DeleteResult DeleteMany(Document filter) => new((long)Call("deleteMany", new Document { ["filter"] = filter })["deletedCount"]!);
}

/// <summary>
/// A session, as the driver's: using var s = client.StartSession(); s.WithTransaction((s, ct) => coll.UpdateOne(s, f, u));
/// The server holds the transaction (60 seconds at most, 20 without a request); a conflict with another transaction
/// answers "aborted", and WithTransaction runs the callback again (5 times at most).
/// </summary>
public sealed class ClientSessionHandle : IDisposable
{
    private readonly IshedClient client;
    private readonly string v1;

    internal ClientSessionHandle(IshedClient client, string v1) { this.client = client; this.v1 = v1; }

    internal string? Transaction { get; private set; }

    public bool IsInTransaction => Transaction is not null;

    public void StartTransaction()
    {
        if (Transaction is not null) throw new IshedException("failed-precondition", "a transaction is already started in this session");
        Transaction = (string)((Document)client.Send(HttpMethod.Post, $"{v1}:beginTransaction", new Document())!)["transaction"]!;
    }

    public void CommitTransaction()
    {
        var tx = Transaction ?? throw new IshedException("failed-precondition", "no transaction started in this session");
        Transaction = null;
        client.Send(HttpMethod.Post, $"{v1}:commit", new Document { ["transaction"] = tx });
    }

    public void AbortTransaction()
    {
        if (Transaction is not { } tx) return;
        Transaction = null;
        try
        {
            client.Send(HttpMethod.Post, $"{v1}:rollback", new Document { ["transaction"] = tx });
        }
        catch (IshedException e) when (e.Code == "not-found")
        {
            // already ended by the server (an error, or too long): nothing kept
        }
    }

    public TResult WithTransaction<TResult>(Func<ClientSessionHandle, CancellationToken, TResult> callback, CancellationToken cancellationToken = default)
    {
        for (var attempt = 1; ; attempt++)
        {
            StartTransaction();
            try
            {
                var result = callback(this, cancellationToken);
                CommitTransaction();
                return result;
            }
            catch (Exception e)
            {
                try { AbortTransaction(); } catch (IshedException) { /* the first error is the one given back */ }
                if (e is not IshedException { Code: "aborted" } || attempt >= 5) throw;
            }
        }
    }

    public void Dispose() => AbortTransaction();
}

/// <summary>What an update change carries, as the driver's option: nothing more, or the document as it is now.</summary>
public enum ChangeStreamFullDocumentOption { Default, UpdateLookup, WhenAvailable, Required }

/// <summary>The document before each change, as the driver's option (kept when the collection has
/// changeStreamPreAndPostImages).</summary>
public enum ChangeStreamFullDocumentBeforeChangeOption { Default, Off, WhenAvailable, Required }

/// <summary>The kind of a change, as the driver's ChangeStreamOperationType.</summary>
public enum ChangeStreamOperationType { Insert, Update, Replace, Delete, Invalidate, Rename, Drop, DropDatabase, Other }

/// <summary>The driver's options: FullDocument, ResumeAfter or StartAfter (a resume token), StartAtOperationTime,
/// MaxAwaitTime (10 s by default, 20 s at most), BatchSize.</summary>
public sealed class ChangeStreamOptions
{
    public ChangeStreamFullDocumentOption FullDocument { get; set; }
    public ChangeStreamFullDocumentBeforeChangeOption FullDocumentBeforeChange { get; set; }
    public Document? ResumeAfter { get; set; }
    public Document? StartAfter { get; set; }
    public DateTime? StartAtOperationTime { get; set; }
    public TimeSpan? MaxAwaitTime { get; set; }
    public int? BatchSize { get; set; }
}

/// <summary>What an update changed: the fields set (dotted paths) and the fields removed.</summary>
public sealed record ChangeStreamUpdateDescription(Document UpdatedFields, string[] RemovedFields);

/// <summary>A change, as the driver's ChangeStreamDocument.</summary>
public sealed class ChangeStreamDocument<TDocument> where TDocument : class
{
    private ChangeStreamDocument(Document raw, TDocument? fullDocument) { BackingDocument = raw; FullDocument = fullDocument; }

    internal static ChangeStreamDocument<Document> Of(Document raw) => new(raw, raw.GetValueOrDefault("fullDocument") as Document);

    /// <summary>The change as the server gave it.</summary>
    public Document BackingDocument { get; }
    public TDocument? FullDocument { get; }
    /// <summary>The document before the change (fullDocumentBeforeChange), or null.</summary>
    public Document? FullDocumentBeforeChange => BackingDocument.GetValueOrDefault("fullDocumentBeforeChange") as Document;
    public ChangeStreamOperationType OperationType => (BackingDocument["operationType"] as string) switch
    {
        "insert" => ChangeStreamOperationType.Insert,
        "update" => ChangeStreamOperationType.Update,
        "replace" => ChangeStreamOperationType.Replace,
        "delete" => ChangeStreamOperationType.Delete,
        "invalidate" => ChangeStreamOperationType.Invalidate,
        "rename" => ChangeStreamOperationType.Rename,
        "drop" => ChangeStreamOperationType.Drop,
        "dropDatabase" => ChangeStreamOperationType.DropDatabase,
        _ => ChangeStreamOperationType.Other,
    };
    public Document? DocumentKey => BackingDocument.GetValueOrDefault("documentKey") as Document;
    /// <summary>The token to resume after this change (ResumeAfter).</summary>
    public Document ResumeToken => (Document)BackingDocument["_id"]!;
    public DateTime? WallTime => BackingDocument.GetValueOrDefault("wallTime") as DateTime?;
    public string? DatabaseName => (BackingDocument.GetValueOrDefault("ns") as Document)?.GetValueOrDefault("db") as string;
    public string? CollectionName => (BackingDocument.GetValueOrDefault("ns") as Document)?.GetValueOrDefault("coll") as string;
    /// <summary>Where a renamed collection went.</summary>
    public string? RenameTo => (BackingDocument.GetValueOrDefault("to") as Document)?.GetValueOrDefault("coll") as string;
    public ChangeStreamUpdateDescription? UpdateDescription => BackingDocument.GetValueOrDefault("updateDescription") is Document u
        ? new((Document)u["updatedFields"]!, ((List<object?>)u["removedFields"]!).Cast<string>().ToArray())
        : null;
}

/// <summary>
/// An open change stream, as the driver's cursor: foreach (var change in cursor.ToEnumerable()) { … } (waits for the
/// changes), or MoveNext() and Current (one wait, its batch possibly empty); GetResumeToken(). Opened at once: the
/// changes made from then on are seen. A collection dropped or renamed ends it ("invalidate").
/// </summary>
public sealed class ChangeStreamCursor : IDisposable
{
    private readonly IshedClient client;
    private readonly string path;
    private readonly Document body;
    private readonly long waitMs;
    private Document? position;
    private DateTime? at;
    private Document? token;
    private bool closed;
    private readonly Queue<Document> buffer = new();

    internal ChangeStreamCursor(IshedClient client, string path, IEnumerable<Document>? pipeline, ChangeStreamOptions? options)
    {
        this.client = client;
        this.path = path;
        body = new Document { ["pipeline"] = (pipeline ?? []).Select(d => (object?)d).ToList() };
        if (options?.FullDocument is { } fd && fd != ChangeStreamFullDocumentOption.Default) body["fullDocument"] = fd switch { ChangeStreamFullDocumentOption.UpdateLookup => "updateLookup", ChangeStreamFullDocumentOption.WhenAvailable => "whenAvailable", _ => "required" };
        if (options?.FullDocumentBeforeChange is { } fb && fb != ChangeStreamFullDocumentBeforeChangeOption.Default) body["fullDocumentBeforeChange"] = fb switch { ChangeStreamFullDocumentBeforeChangeOption.Off => "off", ChangeStreamFullDocumentBeforeChangeOption.WhenAvailable => "whenAvailable", _ => "required" };
        if (options?.BatchSize is { } n) body["batchSize"] = n;
        waitMs = (long)(options?.MaxAwaitTime ?? TimeSpan.FromSeconds(10)).TotalMilliseconds;
        position = options?.ResumeAfter ?? options?.StartAfter;
        at = options?.StartAtOperationTime;
        token = position;
        Current = [];
        if (position is null && at is null) Fetch(0);
    }

    /// <summary>The batch the last MoveNext gave.</summary>
    public IEnumerable<ChangeStreamDocument<Document>> Current { get; private set; }

    private void Fetch(long wait)
    {
        var request = new Document(body) { ["maxAwaitTimeMS"] = wait };
        if (position is not null) request["resumeAfter"] = position;
        else if (at is not null) request["startAtOperationTime"] = at.Value;
        var r = (Document)client.Send(HttpMethod.Post, path, request)!;
        at = null;
        position = (Document)r["resumeToken"]!;
        foreach (var c in ((List<object?>)r["changes"]!).Cast<Document>()) buffer.Enqueue(c);
        if (buffer.Count == 0) token = position;
    }

    // One change: the resume token follows the changes as they are given.
    private ChangeStreamDocument<Document> Take()
    {
        var change = buffer.Dequeue();
        token = buffer.Count > 0 ? (Document)change["_id"]! : position;
        if (change["operationType"] as string == "invalidate") closed = true;
        return ChangeStreamDocument<Document>.Of(change);
    }

    /// <summary>Waits up to MaxAwaitTime for changes (Current); false once the stream is closed.</summary>
    public bool MoveNext()
    {
        if (closed && buffer.Count == 0) return false;
        if (buffer.Count == 0) Fetch(waitMs);
        var batch = new List<ChangeStreamDocument<Document>>();
        while (buffer.Count > 0) batch.Add(Take());
        Current = batch;
        return true;
    }

    /// <summary>Every change as it comes, waiting for it, until the stream is closed.</summary>
    public IEnumerable<ChangeStreamDocument<Document>> ToEnumerable()
    {
        for (;;)
        {
            while (buffer.Count == 0)
            {
                if (closed) yield break;
                Fetch(waitMs);
            }
            yield return Take();
        }
    }

    /// <summary>The token to resume after the last change given (ResumeAfter).</summary>
    public Document? GetResumeToken() => token;

    public void Dispose() => closed = true;
}

/// <summary>As the driver's DocumentValidationLevel: which writes a collection's validator checks.</summary>
public enum DocumentValidationLevel { Strict, Moderate, Off }

/// <summary>As the driver's DocumentValidationAction: a document that breaks the validator refused or written.</summary>
public enum DocumentValidationAction { Error, Warn }

/// <summary>As the driver's CreateCollectionOptions: the validator ({ $jsonSchema: … }), its level and its action.</summary>
public sealed class CreateCollectionOptions
{
    public Document? Validator { get; set; }
    public DocumentValidationLevel? ValidationLevel { get; set; }
    public DocumentValidationAction? ValidationAction { get; set; }
}

/// <summary>An index's options, as the driver's CreateIndexOptions.</summary>
public sealed class CreateIndexOptions
{
    public bool? Unique { get; set; }
    public string? Name { get; set; }
    public Document? PartialFilterExpression { get; set; }
    public Document? Weights { get; set; }
    public string? DefaultLanguage { get; set; }
    /// <summary>A wildcard index's fields ({ attrs: 1 } only those, { secret: 0 } all but those).</summary>
    public Document? WildcardProjection { get; set; }
}

/// <summary>An index to make, as the driver's CreateIndexModel: its keys (in order) and options.</summary>
public sealed class CreateIndexModel(Document keys, CreateIndexOptions? options = null)
{
    internal Document Spec()
    {
        var spec = new Document { ["key"] = keys };
        if (options?.Unique == true) spec["unique"] = true;
        if (options?.Name is { } n) spec["name"] = n;
        if (options?.PartialFilterExpression is { } f) spec["partialFilterExpression"] = f;
        if (options?.Weights is { } w) spec["weights"] = w;
        if (options?.DefaultLanguage is { } l) spec["default_language"] = l;
        if (options?.WildcardProjection is { } wp) spec["wildcardProjection"] = wp;
        return spec;
    }
}

/// <summary>The collection's indexes, as the driver's collection.Indexes: CreateOne, CreateMany, List, DropOne, DropAll.</summary>
public sealed class IndexManager
{
    private readonly IshedCollection collection;

    internal IndexManager(IshedCollection collection) { this.collection = collection; }

    public string CreateOne(CreateIndexModel model) => (string)collection.Call("createIndex", model.Spec())["name"]!;
    public IEnumerable<string> CreateMany(IEnumerable<CreateIndexModel> models) =>
        ((List<object?>)collection.Call("createIndexes", new Document { ["indexes"] = models.Select(m => (object?)m.Spec()).ToList() })["names"]!).Cast<string>().ToList();
    /// <summary>The indexes, as MongoDB gives them (v, key, name, unique…).</summary>
    public List<Document> List() => ((List<object?>)collection.Call("listIndexes", new Document())["indexes"]!).Cast<Document>().ToList();
    public void DropOne(string name) => collection.Call("dropIndex", new Document { ["index"] = name });
    public void DropAll() => collection.Call("dropIndexes", new Document());
}

public sealed class IshedDatabase
{
    private readonly IshedClient client;
    private readonly string path;

    internal IshedDatabase(IshedClient client, string path) { this.client = client; this.path = path; }

    internal IshedClient Client => client;
    internal string Path => path;

    /// <summary>As the driver's Watch: the changes of every collection of the base.</summary>
    public ChangeStreamCursor Watch(IEnumerable<Document>? pipeline = null, ChangeStreamOptions? options = null) => new(client, $"{path}:watch", pipeline, options);

    public IshedCollection GetCollection(string name) => new(client, $"{path}/collections/{Uri.EscapeDataString(name)}");

    public List<string> ListCollectionNames() =>
        ((List<object?>)client.Send(HttpMethod.Get, $"{path}/collections", null)!).Cast<Document>().Select(c => (string)c["name"]!).ToList();

    /// <summary>As the driver's CreateView(viewName, viewOn, pipeline): read only, computed when read.</summary>
    public void CreateView(string viewName, string viewOn, IEnumerable<Document> pipeline) =>
        client.Send(HttpMethod.Post, $"{path}/collections/{Uri.EscapeDataString(viewName)}:create", new Document { ["viewOn"] = viewOn, ["pipeline"] = pipeline.Cast<object?>().ToList() });

    /// <summary>As the driver's CreateCollection(name): an empty collection.</summary>
    public void CreateCollection(string name, CreateCollectionOptions? options = null)
    {
        var body = new Document();
        if (options?.Validator is { } v) body["validator"] = v;
        if (options?.ValidationLevel is { } l) body["validationLevel"] = l.ToString().ToLowerInvariant();
        if (options?.ValidationAction is { } a) body["validationAction"] = a.ToString().ToLowerInvariant();
        client.Send(HttpMethod.Post, $"{path}/collections/{Uri.EscapeDataString(name)}:create", body);
    }

    /// <summary>As the driver's DropCollection(name): the collection and its documents, or the view.</summary>
    public void DropCollection(string name) => client.Send(HttpMethod.Post, $"{path}/collections/{Uri.EscapeDataString(name)}:drop", new Document());

    /// <summary>As the driver's RunCommand: create, collMod (a view's viewOn and pipeline), drop, explain.</summary>
    public Document RunCommand(Document command) => (Document)client.Send(HttpMethod.Post, $"{path}:runCommand", command)!;

    /// <summary>As the driver's ListCollections(): name, type ("collection" or "view"), options, info.</summary>
    public List<Document> ListCollections() =>
        ((List<object?>)client.Send(HttpMethod.Get, $"{path}/collections", null)!).Cast<Document>().Select(c =>
        {
            var view = (string)c["type"]! == "view";
            return new Document
            {
                ["name"] = c["name"],
                ["type"] = c["type"],
                ["options"] = view ? new Document { ["viewOn"] = c["viewOn"], ["pipeline"] = c["pipeline"] } : new Document(),
                ["info"] = new Document { ["readOnly"] = view },
            };
        }).ToList();
}

/// <summary>A client for one project (its key), in one of its clusters (by default the first, "Cluster0").</summary>
public sealed partial class IshedClient
{
    private readonly HttpClient http;
    private readonly string root, v1;

    public IshedClient(string url, string key, string? cluster = null, TimeSpan? timeout = null)
    {
        var uri = new Uri(url);
        var local = uri.Host is "127.0.0.1" or "localhost" or "[::1]" or "::1";
        if (uri.Scheme != "https" && !(uri.Scheme == "http" && local)) throw new IshedException("invalid-argument", "IshedDB is reached over HTTPS (plain HTTP only to this machine)");
        if (!key.StartsWith("ished_sk_", StringComparison.Ordinal)) throw new IshedException("invalid-argument", "a key ished_sk_… is needed");
        if (cluster is not null && !ClusterName().IsMatch(cluster)) throw new IshedException("invalid-argument", "cluster: 1 to 64 letters, digits or \"-\"");
        root = url.TrimEnd('/');
        v1 = cluster is null ? "/v1" : $"/v1/clusters/{cluster}";
        http = new HttpClient { Timeout = timeout ?? TimeSpan.FromSeconds(30) };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", key);
        KeepKey(key);
    }

    [GeneratedRegex("^[A-Za-z0-9][A-Za-z0-9-]{0,63}$")]
    private static partial Regex ClusterName();

    public IshedDatabase GetDatabase(string name) => new(this, $"{v1}/bases/{Uri.EscapeDataString(name)}");

    /// <summary>As the driver's Watch: the changes of the whole cluster.</summary>
    public ChangeStreamCursor Watch(IEnumerable<Document>? pipeline = null, ChangeStreamOptions? options = null) => new(this, $"{v1}:watch", pipeline, options);

    /// <summary>A session, as the driver's: its transactions are held by the server.</summary>
    public ClientSessionHandle StartSession() => new(this, v1);

    /// <summary>As the driver's ListDatabases(): each base's name, sizeOnDisk and empty (a key limited to some: those).</summary>
    public List<Document> ListDatabases() => ((List<object?>)((Document)Send(HttpMethod.Get, $"{v1}/bases", null)!)["databases"]!).Cast<Document>().ToList();

    /// <summary>As the driver's ListDatabaseNames().</summary>
    public List<string> ListDatabaseNames() => ListDatabases().Select(d => (string)d["name"]!).ToList();

    internal object? Send(HttpMethod method, string path, Document? body, string? transaction = null)
    {
        using var request = new HttpRequestMessage(method, root + path);
        if (transaction is not null) request.Headers.Add("X-Ished-Transaction", transaction);
        if (body is not null) request.Content = new StringContent(Wire.Json(body), Encoding.UTF8, "application/json");
        using var response = http.Send(request);
        var text = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        object? parsed = null;
        if (text.Length > 0)
        {
            using var json = JsonDocument.Parse(text);
            parsed = Wire.Read(json.RootElement);
        }
        if (!response.IsSuccessStatusCode)
        {
            var error = (parsed as Document)?.GetValueOrDefault("error") as Document;
            throw new IshedException(error?.GetValueOrDefault("code") as string ?? "internal", error?.GetValueOrDefault("message") as string ?? $"HTTP {(int)response.StatusCode}", (int)response.StatusCode);
        }
        return parsed;
    }
}
