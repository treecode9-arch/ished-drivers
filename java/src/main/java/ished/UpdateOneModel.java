package ished;

/** As the driver's UpdateOneModel: with an UpdateOptions for upsert. */
public final class UpdateOneModel<T> implements WriteModel<T> {
    private final Document filter;
    private final Document update;
    private final boolean upsert;

    public UpdateOneModel(Document filter, Document update) { this(filter, update, new UpdateOptions()); }

    public UpdateOneModel(Document filter, Document update, UpdateOptions options) {
        this.filter = filter;
        this.update = update;
        this.upsert = options.isUpsert();
    }

    @Override
    public Document operation() {
        return new Document("updateOne", new Document("filter", filter).append("update", update).append("upsert", upsert));
    }
}
