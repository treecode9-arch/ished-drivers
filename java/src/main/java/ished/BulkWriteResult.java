package ished;

/** As the driver's BulkWriteResult. */
public final class BulkWriteResult {
    private final Document r;

    BulkWriteResult(Document r) { this.r = r; }

    private long n(String k) { return ((Number) r.get(k)).longValue(); }
    public long getInsertedCount() { return n("insertedCount"); }
    public long getMatchedCount() { return n("matchedCount"); }
    public long getModifiedCount() { return n("modifiedCount"); }
    public long getDeletedCount() { return n("deletedCount"); }
    public long getUpsertedCount() { return n("upsertedCount"); }
}
