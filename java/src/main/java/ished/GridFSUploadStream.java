package ished;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

/** A file being written, as the driver's GridFSUploadStream: write, then close (sent then); getId once closed. */
public final class GridFSUploadStream extends OutputStream {
    private final GridFSBucket bucket;
    private final String filename;
    private final GridFSUploadOptions options;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private Object id;
    private boolean closed;

    GridFSUploadStream(GridFSBucket bucket, Object id, String filename, GridFSUploadOptions options) {
        this.bucket = bucket;
        this.id = id;
        this.filename = filename;
        this.options = options;
    }

    @Override
    public void write(int b) { buffer.write(b); }

    @Override
    public void write(byte[] b, int off, int len) { buffer.write(b, off, len); }

    /** As the driver's: no checked exception (the bytes are kept until close). */
    @Override
    public void write(byte[] b) { buffer.write(b, 0, b.length); }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        id = bucket.upload(id, filename, new ByteArrayInputStream(buffer.toByteArray()), options);
    }

    /** The file's id (made by IshedDB once closed, or the one given). */
    public Object getId() { return id; }
}
