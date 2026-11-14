// IshedDB's files from .NET, as the driver's MongoDB.Driver.GridFS.GridFSBucket: UploadFromStream / UploadFromBytes,
// OpenUploadStream, DownloadToStream / DownloadAsBytes / OpenDownloadStream (by id, or ByName with a revision),
// Find, Delete, Rename, Drop — the same names and arguments. The bytes go to IshedDB in pieces (a resumable upload, as
// Cloud Storage's) and come back as a stream; a file's document is one of the collection <bucket>.files. IshedDB's
// own: SignedUrl for a browser.
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Ished;

/// <summary>As the driver's GridFSBucketOptions: BucketName (by default "fs").</summary>
public sealed class GridFSBucketOptions { public string BucketName { get; set; } = "fs"; }

/// <summary>As the driver's GridFSUploadOptions: Metadata (and IshedDB's ContentType).</summary>
public sealed class GridFSUploadOptions
{
    public Document? Metadata { get; set; }
    public string? ContentType { get; set; }
}

/// <summary>As the driver's GridFSDownloadByNameOptions: Revision (0 the first one sent, -1 the last one, the default).</summary>
public sealed class GridFSDownloadByNameOptions { public int Revision { get; set; } = -1; }

/// <summary>A file being written, as the driver's GridFSUploadStream: Write, then Close (sent then); Id.</summary>
public sealed class GridFSUploadStream : Stream
{
    private readonly GridFSBucket bucket;
    private readonly string filename;
    private readonly GridFSUploadOptions? options;
    private readonly MemoryStream buffer = new();
    private bool sent;

    internal GridFSUploadStream(GridFSBucket bucket, string? id, string filename, GridFSUploadOptions? options)
    {
        this.bucket = bucket;
        Id = id;
        this.filename = filename;
        this.options = options;
    }

    /// <summary>The file's id: the one given, or IshedDB's once closed.</summary>
    public string? Id { get; private set; }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => !sent;
    public override long Length => buffer.Length;
    public override long Position { get => buffer.Position; set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] b, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] b, int offset, int count) => buffer.Write(b, offset, count);

    protected override void Dispose(bool disposing)
    {
        if (disposing && !sent)
        {
            sent = true;
            buffer.Position = 0;
            Id = bucket.Upload(Id, filename, buffer, options);
        }
        base.Dispose(disposing);
    }
}

/// <summary>A file being read, as the driver's GridFSDownloadStream: Read, and FileInfo (its document).</summary>
public sealed class GridFSDownloadStream : Stream
{
    private readonly HttpResponseMessage response;
    private readonly Stream body;

    internal GridFSDownloadStream(HttpResponseMessage response, Document fileInfo)
    {
        this.response = response;
        body = response.Content.ReadAsStream();
        FileInfo = fileInfo;
    }

    /// <summary>The file's document: _id, length, filename, metadata, sha256…</summary>
    public Document FileInfo { get; }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => Convert.ToInt64(FileInfo["length"]);
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] b, int offset, int count) => body.Read(b, offset, count);
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] b, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            body.Dispose();
            response.Dispose();
        }
        base.Dispose(disposing);
    }
}

/// <summary>A bucket of files, as the driver's GridFSBucket(database, options).</summary>
public sealed partial class GridFSBucket
{
    private const int Piece = 8 * 1024 * 1024; // one request (a multiple of IshedDB's 256 KB segments)
    private readonly IshedDatabase database;
    private readonly string path;

    public GridFSBucket(IshedDatabase database, GridFSBucketOptions? options = null)
    {
        BucketName = options?.BucketName ?? "fs";
        if (!Name().IsMatch(BucketName)) throw new IshedException("invalid-argument", "BucketName: 1 to 94 letters, digits, \"_\", \"-\" or \".\"");
        this.database = database;
        path = $"{database.Path}/buckets/{Uri.EscapeDataString(BucketName)}";
    }

    public string BucketName { get; }

    [GeneratedRegex("^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,93}$")]
    private static partial Regex Name();

    private IshedCollection Files => database.GetCollection($"{BucketName}.files");
    private string FilePath(string id, string action) => $"{path}/files/{Uri.EscapeDataString(id)}:{action}";

    private static byte[] ReadPiece(Stream source)
    {
        var piece = new byte[Piece];
        var n = 0;
        while (n < Piece)
        {
            var r = source.Read(piece, n, Piece - n);
            if (r == 0) break;
            n += r;
        }
        return piece[..n];
    }

    /// <summary>Sends [source] in pieces of 8 MB, the length told with the last one; answers the file's id.</summary>
    internal string Upload(string? id, string filename, Stream source, GridFSUploadOptions? options)
    {
        var body = new Document { ["filename"] = filename, ["length"] = null };
        if (id is not null) body["id"] = id;
        if (options?.Metadata is not null) body["metadata"] = options.Metadata;
        if (options?.ContentType is not null) body["contentType"] = options.ContentType;
        var piece = ReadPiece(source);
        if (piece.Length == 0) body["length"] = 0L;
        var started = (Document)database.Client.Send(HttpMethod.Post, $"{path}/uploads", body)!;
        var fileId = Convert.ToString(started["id"])!;
        if (started.ContainsKey("file")) return fileId;
        long at = 0;
        while (true)
        {
            var following = ReadPiece(source);
            var last = following.Length == 0;
            var total = last ? (at + piece.Length).ToString() : "*";
            using (database.Client.SendBytes(HttpMethod.Put, (string)started["uploadUrl"]!, piece, $"bytes {at}-{at + piece.Length - 1}/{total}", auth: false)) { }
            at += piece.Length;
            if (last) return fileId;
            piece = following;
        }
    }

    /// <summary>As the driver's UploadFromStream(filename, source, options): the file's id (made by IshedDB).</summary>
    public string UploadFromStream(string filename, Stream source, GridFSUploadOptions? options = null) => Upload(null, filename, source, options);

    /// <summary>As the driver's UploadFromStream(id, filename, source): the file kept under the id given.</summary>
    public void UploadFromStream(string id, string filename, Stream source, GridFSUploadOptions? options = null) => Upload(id, filename, source, options);

    public string UploadFromBytes(string filename, byte[] source, GridFSUploadOptions? options = null) => Upload(null, filename, new MemoryStream(source), options);

    public GridFSUploadStream OpenUploadStream(string filename, GridFSUploadOptions? options = null) => new(this, null, filename, options);

    public GridFSUploadStream OpenUploadStream(string id, string filename, GridFSUploadOptions? options = null) => new(this, id, filename, options);

    /// <summary>As the driver's OpenDownloadStream(id): a stream of the file's bytes, with its document.</summary>
    public GridFSDownloadStream OpenDownloadStream(string id)
    {
        var doc = Files.Find(new Document { ["_id"] = id }).FirstOrDefault() ?? throw new IshedException("not-found", $"no file {id} in the bucket {BucketName}", 404);
        return new GridFSDownloadStream(database.Client.SendBytes(HttpMethod.Get, FilePath(id, "download"), null, null, auth: true), doc);
    }

    public void DownloadToStream(string id, Stream destination)
    {
        using var source = OpenDownloadStream(id);
        source.CopyTo(destination);
    }

    public byte[] DownloadAsBytes(string id)
    {
        using var output = new MemoryStream();
        DownloadToStream(id, output);
        return output.ToArray();
    }

    private string ByName(string filename, GridFSDownloadByNameOptions? options)
    {
        var revision = options?.Revision ?? -1;
        var found = Files.Find(new Document { ["filename"] = filename }).Sort(new Document { ["uploadDate"] = revision >= 0 ? 1 : -1 }).Skip(revision >= 0 ? revision : -revision - 1).Limit(1).FirstOrDefault()
            ?? throw new IshedException("not-found", $"no revision {revision} of the file {filename} in the bucket {BucketName}", 404);
        return Convert.ToString(found["_id"])!;
    }

    /// <summary>As the driver's: a revision by name (the last one by default).</summary>
    public GridFSDownloadStream OpenDownloadStreamByName(string filename, GridFSDownloadByNameOptions? options = null) => OpenDownloadStream(ByName(filename, options));

    public void DownloadToStreamByName(string filename, Stream destination, GridFSDownloadByNameOptions? options = null) => DownloadToStream(ByName(filename, options), destination);

    public byte[] DownloadAsBytesByName(string filename, GridFSDownloadByNameOptions? options = null) => DownloadAsBytes(ByName(filename, options));

    /// <summary>As the driver's Find(filter): the files' documents (the collection &lt;bucket&gt;.files).</summary>
    public FindFluent Find(Document filter) => Files.Find(filter);

    /// <summary>As the driver's Delete(id): the file's document and its bytes.</summary>
    public void Delete(string id) => database.Client.Send(HttpMethod.Post, FilePath(id, "delete"), new Document());

    public void Rename(string id, string newFilename) => database.Client.Send(HttpMethod.Post, FilePath(id, "rename"), new Document { ["filename"] = newFilename });

    /// <summary>As the driver's Drop(): every file of the bucket, and its &lt;bucket&gt;.files collection.</summary>
    public void Drop() => database.Client.Send(HttpMethod.Post, $"{path}:drop", new Document());

    /// <summary>IshedDB's own: a signed address for a browser (no key in it), as Cloud Storage's signed URLs.</summary>
    public string SignedUrl(string id, int expiresIn = 3600) =>
        (string)((Document)database.Client.Send(HttpMethod.Post, FilePath(id, "signUrl"), new Document { ["expiresIn"] = (long)expiresIn, ["disposition"] = "attachment" })!)["url"]!;
}

public sealed partial class IshedClient
{
    // The files' bytes: an HttpClient of their own, WITHOUT the key in its default headers (an upload session's address
    // is its own credential: the key never goes there); the key added to a download only. No short time limit (2 GB).
    private HttpClient? bytesHttp;
    private string? key;

    internal void KeepKey(string k) => key = k;

    internal HttpResponseMessage SendBytes(HttpMethod method, string where, byte[]? body, string? contentRange, bool auth)
    {
        var target = new Uri(where.StartsWith("http://", StringComparison.Ordinal) || where.StartsWith("https://", StringComparison.Ordinal) ? where : root + where);
        var local = target.Host is "127.0.0.1" or "localhost" or "[::1]" or "::1";
        if (target.Scheme != "https" && !(target.Scheme == "http" && local)) throw new IshedException("invalid-argument", "a file's address is reached over HTTPS only");
        bytesHttp ??= new HttpClient { Timeout = TimeSpan.FromHours(6) };
        var request = new HttpRequestMessage(method, target);
        if (auth) request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", key);
        if (body is not null)
        {
            request.Content = new ByteArrayContent(body);
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            if (contentRange is not null) request.Content.Headers.TryAddWithoutValidation("Content-Range", contentRange);
        }
        var response = bytesHttp.Send(request, HttpCompletionOption.ResponseHeadersRead);
        if (!response.IsSuccessStatusCode)
        {
            using (response)
            {
                var text = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                Document? error = null;
                if (text.Length > 0)
                {
                    using var json = JsonDocument.Parse(text);
                    error = (Wire.Read(json.RootElement) as Document)?.GetValueOrDefault("error") as Document;
                }
                throw new IshedException(error?.GetValueOrDefault("code") as string ?? "internal", error?.GetValueOrDefault("message") as string ?? $"HTTP {(int)response.StatusCode}", (int)response.StatusCode);
            }
        }
        return response;
    }
}
