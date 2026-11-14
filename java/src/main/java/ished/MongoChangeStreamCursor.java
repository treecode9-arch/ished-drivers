package ished;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An open change stream, as the driver's MongoChangeStreamCursor: next() waits for a change, tryNext() gives one if it
 * comes within a second, getResumeToken(), close(). A collection dropped or renamed ends it ("invalidate").
 */
public final class MongoChangeStreamCursor implements Iterator<ChangeStreamDocument<Document>>, AutoCloseable {
    private final IshedClient client;
    private final String path;
    private final Document body;
    private final long waitMs;
    private Document position;
    private Instant at;
    private Document token;
    private final ArrayDeque<Document> buffer = new ArrayDeque<>();
    private boolean closed;

    MongoChangeStreamCursor(IshedClient client, String path, Document body, long waitMs, Document resumeAfter, Instant at) {
        this.client = client;
        this.path = path;
        this.body = body;
        this.waitMs = waitMs;
        this.position = resumeAfter;
        this.at = at;
        this.token = resumeAfter;
        if (resumeAfter == null && at == null) fetch(0);
    }

    @SuppressWarnings("unchecked")
    private void fetch(long wait) {
        Document request = new Document(body).append("maxAwaitTimeMS", wait);
        if (position != null) request.append("resumeAfter", position);
        else if (at != null) request.append("startAtOperationTime", Date.from(at));
        Document r = (Document) client.send("POST", path, request);
        at = null;
        position = (Document) r.get("resumeToken");
        for (Object c : (List<Object>) r.get("changes")) buffer.add((Document) c);
        if (buffer.isEmpty()) token = position;
    }

    private ChangeStreamDocument<Document> take() {
        Document change = buffer.poll();
        token = buffer.isEmpty() ? position : (Document) change.get("_id");
        if ("invalidate".equals(change.get("operationType"))) closed = true;
        return ChangeStreamDocument.of(change);
    }

    /** Whether a change comes (waiting for it); false once the stream is closed. */
    @Override
    public boolean hasNext() {
        while (buffer.isEmpty() && !closed) fetch(waitMs);
        return !buffer.isEmpty();
    }

    @Override
    public ChangeStreamDocument<Document> next() {
        if (!hasNext()) throw new NoSuchElementException("the change stream is closed");
        return take();
    }

    /** A change if one comes within a second (or maxAwaitTime, if shorter), else null. */
    public ChangeStreamDocument<Document> tryNext() {
        if (buffer.isEmpty() && !closed) fetch(Math.min(waitMs, 1000));
        return buffer.isEmpty() ? null : take();
    }

    /** The token to resume after the last change given (resumeAfter). */
    public Document getResumeToken() { return token; }

    @Override
    public void close() {
        closed = true;
        buffer.clear();
    }
}
