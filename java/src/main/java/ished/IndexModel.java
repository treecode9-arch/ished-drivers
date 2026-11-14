package ished;

/** An index to make, as the driver's IndexModel: its keys and options. */
public final class IndexModel {
    final Document keys;
    final IndexOptions options;

    public IndexModel(Document keys) { this(keys, new IndexOptions()); }

    public IndexModel(Document keys, IndexOptions options) {
        this.keys = keys;
        this.options = options;
    }

    Document spec() { return new Document("key", keys).append(options.toDocument()); }
}
