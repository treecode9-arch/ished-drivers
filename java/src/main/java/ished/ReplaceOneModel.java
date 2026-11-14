package ished;

/** As the driver's ReplaceOneModel: with an UpdateOptions for upsert. */
public final class ReplaceOneModel<T> implements WriteModel<T> {
    private final Document filter;
    private final Document replacement;
    private final boolean upsert;

    public ReplaceOneModel(Document filter, Document replacement) { this(filter, replacement, new UpdateOptions()); }

    public ReplaceOneModel(Document filter, Document replacement, UpdateOptions options) {
        this.filter = filter;
        this.replacement = replacement;
        this.upsert = options.isUpsert();
    }

    @Override
    public Document operation() {
        return new Document("replaceOne", new Document("filter", filter).append("replacement", replacement).append("upsert", upsert));
    }
}
