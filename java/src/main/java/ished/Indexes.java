package ished;

/** An index's keys, as the driver's Indexes: ascending("a"), descending("b"), text("titre"), compoundIndex(…). */
public final class Indexes {
    private Indexes() {}

    public static Document ascending(String... fields) { return of(1, fields); }
    public static Document descending(String... fields) { return of(-1, fields); }
    public static Document text(String field) { return new Document(field, "text"); }

    /** The keys in their order. */
    public static Document compoundIndex(Document... parts) {
        Document out = new Document();
        for (Document p : parts) out.putAll(p);
        return out;
    }

    private static Document of(Object direction, String... fields) {
        Document out = new Document();
        for (String f : fields) out.put(f, direction);
        return out;
    }
}
