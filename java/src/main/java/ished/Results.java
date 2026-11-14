package ished;

import java.util.List;

/** As MongoDB's result classes. */
public final class Results {
    private Results() {}

    public record InsertOneResult(String getInsertedId) {
        public String getInsertedId() { return getInsertedId; }
    }

    public record InsertManyResult(List<String> ids) {
        public List<String> getInsertedIds() { return ids; }
    }

    public record UpdateResult(long matched, long modified, String upserted) {
        public long getMatchedCount() { return matched; }
        public long getModifiedCount() { return modified; }
        public String getUpsertedId() { return upserted; }
    }

    public record DeleteResult(long deleted) {
        public long getDeletedCount() { return deleted; }
    }
}
