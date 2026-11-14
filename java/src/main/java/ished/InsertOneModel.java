package ished;

/** As the driver's InsertOneModel. */
public final class InsertOneModel<T> implements WriteModel<T> {
    private final Document document;

    public InsertOneModel(Document document) { this.document = document; }

    @Override
    public Document operation() { return new Document("insertOne", new Document("document", document)); }
}
