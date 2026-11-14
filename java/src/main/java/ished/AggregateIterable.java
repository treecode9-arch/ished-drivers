package ished;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/** As MongoDB's AggregateIterable: into, first or a for loop; the results read 1000 at a time. */
public final class AggregateIterable implements Iterable<Document> {
    private final IshedCollection collection;
    private final List<Document> pipeline;

    AggregateIterable(IshedCollection collection, List<Document> pipeline) {
        this.collection = collection;
        this.pipeline = pipeline;
    }

    @SuppressWarnings("unchecked")
    public <A extends Collection<? super Document>> A into(A target) {
        int count = 0;
        for (int skip = 0; ; skip += 1000) {
            Document page = collection.call("aggregate", new Document("pipeline", pipeline).append("limit", 1000).append("skip", skip));
            for (Object d : (List<Object>) page.get("documents")) {
                target.add((Document) d);
                count++;
            }
            if (!(Boolean) page.get("more")) return target;
            if (count >= FindIterable.MAX_DOCUMENTS) throw new IshedException("resource-exhausted", "more than " + FindIterable.MAX_DOCUMENTS + " documents: add $limit", 0);
        }
    }

    /** As the driver's explain(): the pipeline's plan ($cursor, then its stages). */
    public Document explain() { return explain(null); }

    public Document explain(ExplainVerbosity verbosity) {
        Document body = new Document("explain", new Document("aggregate", collection.name()).append("pipeline", pipeline));
        if (verbosity != null) body.append("verbosity", verbosity.value());
        return collection.call("explain", body);
    }

    public Document first() {
        List<Document> out = into(new ArrayList<>());
        return out.isEmpty() ? null : out.get(0);
    }

    @Override
    public Iterator<Document> iterator() {
        return into(new ArrayList<Document>()).iterator();
    }
}
