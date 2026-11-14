package ished;

/** As the driver's GridFSDownloadOptions: revision (0 the first one sent, -1 the last one, the default). */
public final class GridFSDownloadOptions {
    int revision = -1;

    public GridFSDownloadOptions revision(int revision) { this.revision = revision; return this; }
}
