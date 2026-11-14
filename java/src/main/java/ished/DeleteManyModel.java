package ished;

/** As the driver's DeleteManyModel. */
public final class DeleteManyModel<T> implements WriteModel<T> {
    private final Document filter;

    public DeleteManyModel(Document filter) { this.filter = filter; }

    @Override
    public Document operation() { return new Document("deleteMany", new Document("filter", filter)); }
}
