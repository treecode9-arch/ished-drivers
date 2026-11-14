package ished;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A change stream to open, as the driver's: coll.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP), then
 * for (ChangeStreamDocument<Document> change : it) { … } or cursor(). Each request waits up to maxAwaitTime (10 s by
 * default, 20 s at most) on the server.
 */
public final class ChangeStreamIterable implements Iterable<ChangeStreamDocument<Document>> {
    private final IshedClient client;
    private final String path;
    private final List<Document> pipeline;
    private FullDocument fullDocument = FullDocument.DEFAULT;
    private FullDocumentBeforeChange fullDocumentBeforeChange = FullDocumentBeforeChange.DEFAULT;
    private Document resumeAfter;
    private Instant startAt;
    private long maxAwaitMs = 10_000;
    private int batchSize;

    ChangeStreamIterable(IshedClient client, String path, List<Document> pipeline) {
        this.client = client;
        this.path = path;
        this.pipeline = new ArrayList<>(pipeline);
    }

    public ChangeStreamIterable fullDocument(FullDocument f) { fullDocument = f; return this; }
    public ChangeStreamIterable fullDocumentBeforeChange(FullDocumentBeforeChange f) { fullDocumentBeforeChange = f; return this; }
    public ChangeStreamIterable resumeAfter(Document token) { resumeAfter = token; return this; }
    public ChangeStreamIterable startAfter(Document token) { resumeAfter = token; return this; }
    public ChangeStreamIterable startAtOperationTime(Instant at) { startAt = at; return this; }
    public ChangeStreamIterable maxAwaitTime(long time, TimeUnit unit) { maxAwaitMs = unit.toMillis(time); return this; }
    public ChangeStreamIterable batchSize(int n) { batchSize = n; return this; }

    /** Opens the stream: the changes made from then on are seen. */
    public MongoChangeStreamCursor cursor() {
        Document body = new Document("pipeline", pipeline);
        if (fullDocument != FullDocument.DEFAULT) body.append("fullDocument", fullDocument.getValue());
        if (fullDocumentBeforeChange != FullDocumentBeforeChange.DEFAULT) body.append("fullDocumentBeforeChange", fullDocumentBeforeChange.getValue());
        if (batchSize > 0) body.append("batchSize", batchSize);
        return new MongoChangeStreamCursor(client, path, body, maxAwaitMs, resumeAfter, startAt);
    }

    @Override
    public MongoChangeStreamCursor iterator() { return cursor(); }
}
