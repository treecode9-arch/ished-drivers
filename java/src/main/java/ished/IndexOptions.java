package ished;

/**
 * An index's options, as the driver's IndexOptions: unique, name, partialFilterExpression, weights, defaultLanguage,
 * wildcardProjection (a wildcard index's fields: { attrs: 1 } only those, { secret: 0 } all but those).
 */
public final class IndexOptions {
    private final Document options = new Document();

    public IndexOptions unique(boolean unique) { options.put("unique", unique); return this; }
    public IndexOptions name(String name) { options.put("name", name); return this; }
    public IndexOptions partialFilterExpression(Document filter) { options.put("partialFilterExpression", filter); return this; }
    public IndexOptions weights(Document weights) { options.put("weights", weights); return this; }
    public IndexOptions defaultLanguage(String language) { options.put("default_language", language); return this; }
    public IndexOptions wildcardProjection(Document projection) { options.put("wildcardProjection", projection); return this; }

    Document toDocument() { return options; }
}
