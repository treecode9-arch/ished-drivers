package ished;

import java.util.Date;
import java.util.List;

/** A change, as the driver's ChangeStreamDocument: its kind, namespace, document key, document, what changed. */
public final class ChangeStreamDocument<TDocument> {
    private final Document raw;
    private final TDocument fullDocument;

    private ChangeStreamDocument(Document raw, TDocument fullDocument) {
        this.raw = raw;
        this.fullDocument = fullDocument;
    }

    static ChangeStreamDocument<Document> of(Document raw) {
        return new ChangeStreamDocument<>(raw, raw.get("fullDocument") instanceof Document d ? d : null);
    }

    public OperationType getOperationType() { return OperationType.fromString(getOperationTypeString()); }
    public String getOperationTypeString() { return (String) raw.get("operationType"); }
    public TDocument getFullDocument() { return fullDocument; }
    /** The document before the change (fullDocumentBeforeChange), or null. */
    public Document getFullDocumentBeforeChange() { return raw.get("fullDocumentBeforeChange") instanceof Document d ? d : null; }
    public Document getDocumentKey() { return raw.get("documentKey") instanceof Document d ? d : null; }
    /** The token to resume after this change (resumeAfter). */
    public Document getResumeToken() { return (Document) raw.get("_id"); }
    public Date getWallTime() { return raw.get("wallTime") instanceof Date d ? d : null; }

    public MongoNamespace getNamespace() {
        Document ns = (Document) raw.get("ns");
        return ns == null ? null : new MongoNamespace((String) ns.get("db"), (String) ns.get("coll"));
    }

    /** Where a renamed collection went. */
    public MongoNamespace getDestinationNamespace() {
        return raw.get("to") instanceof Document to ? new MongoNamespace((String) to.get("db"), (String) to.get("coll")) : null;
    }

    @SuppressWarnings("unchecked")
    public UpdateDescription getUpdateDescription() {
        if (!(raw.get("updateDescription") instanceof Document u)) return null;
        return new UpdateDescription((Document) u.get("updatedFields"), (List<String>) u.get("removedFields"));
    }

    /** The change as the server gave it. */
    public Document toDocument() { return raw; }
}
