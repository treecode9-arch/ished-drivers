package ished;

import java.io.IOException;
import java.io.InputStream;

/** A file being read, as the driver's GridFSDownloadStream: read, close, getGridFSFile (its document). */
public final class GridFSDownloadStream extends InputStream {
    private final InputStream body;
    private final Document file;

    GridFSDownloadStream(InputStream body, Document file) {
        this.body = body;
        this.file = file;
    }

    @Override
    public int read() throws IOException { return body.read(); }

    @Override
    public int read(byte[] b, int off, int len) throws IOException { return body.read(b, off, len); }

    @Override
    public void close() throws IOException { body.close(); }

    /** The file's document: _id, length, filename, metadata, sha256… */
    public Document getGridFSFile() { return file; }
}
