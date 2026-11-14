package ished;

/** As the driver's UpdateManyModel. */
public final class UpdateManyModel<T> implements WriteModel<T> {
    private final Document filter;
    private final Document update;

    public UpdateManyModel(Document filter, Document update) { this.filter = filter; this.update = update; }

    @Override
    public Document operation() { return new Document("updateMany", new Document("filter", filter).append("update", update)); }
}
