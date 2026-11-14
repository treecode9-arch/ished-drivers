package ished;

/** As the driver's GridFSBuckets: GridFSBuckets.create(database) or create(database, bucketName). */
public final class GridFSBuckets {
    private GridFSBuckets() {}

    public static GridFSBucket create(IshedDatabase database) { return new GridFSBucket(database, "fs"); }

    public static GridFSBucket create(IshedDatabase database, String bucketName) { return new GridFSBucket(database, bucketName); }
}
