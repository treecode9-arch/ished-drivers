package ished;

/** As the driver's DeleteOneModel. */
public final class DeleteOneModel<T> implements WriteModel<T> {
    private final Document filter;

    public DeleteOneModel(Document filter) { this.filter = filter; }

    @Override
    public Document operation() { return new Document("deleteOne", new Document("filter", filter)); }
}
