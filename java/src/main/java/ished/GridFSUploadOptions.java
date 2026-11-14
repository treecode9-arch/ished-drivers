package ished;

/** As the driver's GridFSUploadOptions: metadata (and IshedDB's contentType). */
public final class GridFSUploadOptions {
    Document metadata;
    String contentType;

    public GridFSUploadOptions metadata(Document metadata) { this.metadata = metadata; return this; }

    public GridFSUploadOptions contentType(String contentType) { this.contentType = contentType; return this; }
}
