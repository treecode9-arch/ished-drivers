package ished;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * IshedDB's files from Java, as the driver's GridFSBucket: uploadFromStream, openUploadStream, downloadToStream,
 * openDownloadStream (by id, or by name with GridFSDownloadOptions), find, delete, rename, drop. The bytes go to
 * IshedDB in pieces (a resumable upload, as Cloud Storage's) and come back as a stream; a file's document is one of
 * the collection <bucket>.files. IshedDB's own: signedUrl for a browser.
 */
public final class GridFSBucket {
    private static final Pattern NAME = Pattern.compile("^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,93}$");
    private static final int PIECE = 8 * 1024 * 1024; // one request (a multiple of IshedDB's 256 KB segments)
    private final IshedDatabase database;
    private final String bucketName;
    private final String path;

    GridFSBucket(IshedDatabase database, String bucketName) {
        if (bucketName == null || !NAME.matcher(bucketName).matches()) throw new IshedException("invalid-argument", "bucketName: 1 to 94 letters, digits, \"_\", \"-\" or \".\"", 0);
        this.database = database;
        this.bucketName = bucketName;
        this.path = database.path() + "/buckets/" + IshedClient.encode(bucketName);
    }

    public String getBucketName() { return bucketName; }

    private String file(Object id, String action) { return path + "/files/" + IshedClient.encode(String.valueOf(id)) + ":" + action; }

    private static byte[] piece(InputStream in) {
        try {
            return in.readNBytes(PIECE);
        } catch (IOException e) {
            throw new IshedException("unavailable", e.getMessage(), 0);
        }
    }

    private static void drain(HttpResponse<InputStream> res) {
        try (InputStream in = res.body()) {
            in.transferTo(OutputStream.nullOutputStream());
        } catch (IOException e) {
            throw new IshedException("unavailable", e.getMessage(), 0);
        }
    }

    /** Sends [source] in pieces of 8 MB, the length told with the last one; answers the file's id. */
    Object upload(Object id, String filename, InputStream source, GridFSUploadOptions options) {
        Document body = new Document("filename", filename).append("length", null);
        if (id != null) body.append("id", String.valueOf(id));
        if (options != null && options.metadata != null) body.append("metadata", options.metadata);
        if (options != null && options.contentType != null) body.append("contentType", options.contentType);
        byte[] piece = piece(source);
        if (piece.length == 0) body.append("length", 0);
        Document started = (Document) database.client().send("POST", path + "/uploads", body);
        if (started.containsKey("file")) return started.get("id");
        long at = 0;
        while (true) {
            byte[] following = piece(source);
            boolean last = following.length == 0;
            String total = last ? String.valueOf(at + piece.length) : "*";
            drain(database.client().sendBytes("PUT", started.getString("uploadUrl"), piece, Map.of("Content-Range", "bytes " + at + "-" + (at + piece.length - 1) + "/" + total, "Content-Type", "application/octet-stream"), false));
            at += piece.length;
            if (last) return started.get("id");
            piece = following;
        }
    }

    /** As the driver's uploadFromStream(filename, source): the file's id (made by IshedDB). */
    public Object uploadFromStream(String filename, InputStream source) { return upload(null, filename, source, null); }

    public Object uploadFromStream(String filename, InputStream source, GridFSUploadOptions options) { return upload(null, filename, source, options); }

    /** As the driver's uploadFromStream(id, filename, source): the file kept under the id given. */
    public void uploadFromStream(Object id, String filename, InputStream source) { upload(id, filename, source, null); }

    public void uploadFromStream(Object id, String filename, InputStream source, GridFSUploadOptions options) { upload(id, filename, source, options); }

    /** As the driver's openUploadStream(filename): a stream, sent when closed. */
    public GridFSUploadStream openUploadStream(String filename) { return new GridFSUploadStream(this, null, filename, null); }

    public GridFSUploadStream openUploadStream(String filename, GridFSUploadOptions options) { return new GridFSUploadStream(this, null, filename, options); }

    public GridFSUploadStream openUploadStream(Object id, String filename) { return new GridFSUploadStream(this, id, filename, null); }

    /** As the driver's openDownloadStream(id): a stream of the file's bytes, with its document. */
    public GridFSDownloadStream openDownloadStream(Object id) {
        Document doc = database.getCollection(bucketName + ".files").find(new Document("_id", id)).first();
        if (doc == null) throw new IshedException("not-found", "no file " + id + " in the bucket " + bucketName, 404);
        HttpResponse<InputStream> res = database.client().sendBytes("GET", file(id, "download"), null, Map.of(), true);
        return new GridFSDownloadStream(res.body(), doc);
    }

    /** As the driver's openDownloadStream(filename, options): a revision by name (the last one by default). */
    public GridFSDownloadStream openDownloadStream(String filename, GridFSDownloadOptions options) { return openDownloadStream(byName(filename, options)); }

    /** As the driver's downloadToStream(id, destination): the bytes written as they come. */
    public void downloadToStream(Object id, OutputStream destination) {
        try (GridFSDownloadStream in = openDownloadStream(id)) {
            in.transferTo(destination);
        } catch (IOException e) {
            throw new IshedException("unavailable", e.getMessage(), 0);
        }
    }

    public void downloadToStream(String filename, OutputStream destination, GridFSDownloadOptions options) { downloadToStream(byName(filename, options), destination); }

    private Object byName(String filename, GridFSDownloadOptions options) {
        int revision = options == null ? -1 : options.revision;
        Document found = database.getCollection(bucketName + ".files").find(new Document("filename", filename))
            .sort(new Document("uploadDate", revision >= 0 ? 1 : -1)).skip(revision >= 0 ? revision : -revision - 1).limit(1).first();
        if (found == null) throw new IshedException("not-found", "no revision " + revision + " of the file " + filename + " in the bucket " + bucketName, 404);
        return found.get("_id");
    }

    /** As the driver's find(filter): the files' documents (the collection <bucket>.files). */
    public FindIterable find() { return database.getCollection(bucketName + ".files").find(); }

    public FindIterable find(Document filter) { return database.getCollection(bucketName + ".files").find(filter); }

    /** As the driver's delete(id): the file's document and its bytes. */
    public void delete(Object id) { database.client().send("POST", file(id, "delete"), new Document()); }

    public void rename(Object id, String newFilename) { database.client().send("POST", file(id, "rename"), new Document("filename", newFilename)); }

    /** As the driver's drop(): every file of the bucket, and its <bucket>.files collection. */
    public void drop() { database.client().send("POST", path + ":drop", new Document()); }

    /** IshedDB's own: a signed address for a browser (no key in it), as Cloud Storage's signed URLs. */
    public String signedUrl(Object id, int expiresIn) {
        return ((Document) database.client().send("POST", file(id, "signUrl"), new Document("expiresIn", expiresIn).append("disposition", "attachment"))).getString("url");
    }
}
