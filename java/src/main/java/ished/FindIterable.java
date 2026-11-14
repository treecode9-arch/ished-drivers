package ished;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/** As MongoDB's FindIterable: sort, limit, skip, projection, then into, first or a for loop. Read 1000 at a time. */
public final class FindIterable implements Iterable<Document> {
    public static final int MAX_DOCUMENTS = 100_000;
    private final IshedCollection collection;
    private final Document filter;
    private Document sort;
    private Document projection;
    private int limit;
    private int skip;

    FindIterable(IshedCollection collection, Document filter) {
        this.collection = collection;
        this.filter = filter;
    }

    public FindIterable sort(Document s) { sort = s; return this; }
    public FindIterable limit(int n) { limit = n; return this; }
    public FindIterable skip(int n) { skip = n; return this; }
    public FindIterable projection(Document p) { projection = p; return this; }

    /** As the driver's explain(): the find's plan and what running it examined (allPlansExecution). */
    public Document explain() { return explain(null); }

    /** As the driver's explain(verbosity): QUERY_PLANNER (the plan only), EXECUTION_STATS, ALL_PLANS_EXECUTION. */
    public Document explain(ExplainVerbosity verbosity) {
        Document find = new Document("find", collection.name()).append("filter", filter);
        if (sort != null) find.append("sort", sort);
        if (projection != null) find.append("projection", projection);
        if (skip > 0) find.append("skip", skip);
        if (limit > 0) find.append("limit", limit);
        Document body = new Document("explain", find);
        if (verbosity != null) body.append("verbosity", verbosity.value());
        return collection.call("explain", body);
    }

    public Document first() {
        limit = 1;
        List<Document> found = into(new ArrayList<>());
        return found.isEmpty() ? null : found.get(0);
    }

    @SuppressWarnings("unchecked")
    public <A extends Collection<? super Document>> A into(A target) {
        int count = 0;
        String token = null;
        for (;;) {
            int want = limit > 0 ? Math.min(1000, limit - count) : 1000;
            Document body = new Document("filter", filter).append("limit", want);
            // The next page starts after the last one read (its token), never at an offset that moves with writes.
            if (token != null) body.append("pageToken", token); else body.append("skip", skip);
            if (sort != null) body.append("sort", sort);
            if (projection != null) body.append("projection", projection);
            Document page = collection.call("find", body);
            for (Object d : (List<Object>) page.get("documents")) {
                target.add((Document) d);
                count++;
            }
            token = (String) page.get("nextPageToken");
            if (token == null || (limit > 0 && count >= limit)) return target;
            if (count >= MAX_DOCUMENTS) throw new IshedException("resource-exhausted", "more than " + MAX_DOCUMENTS + " documents: narrow the filter", 0);
        }
    }

    @Override
    public Iterator<Document> iterator() {
        return into(new ArrayList<Document>()).iterator();
    }
}
