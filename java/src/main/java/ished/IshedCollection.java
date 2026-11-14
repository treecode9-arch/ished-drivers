package ished;

import java.util.ArrayList;
import java.util.List;

/** A collection, with MongoDB's methods. */
public final class IshedCollection {
    private final IshedClient client;
    private final String path;
    private final ClientSession session;

    IshedCollection(IshedClient client, String path) { this(client, path, null); }

    private IshedCollection(IshedClient client, String path, ClientSession session) {
        this.client = client;
        this.path = path;
        this.session = session;
    }

    Document call(String method, Document body) {
        return (Document) client.send("POST", path + ":" + method, body, session == null ? null : session.transaction());
    }

    /** The collection's name, as its commands name it. */
    String name() {
        return java.net.URLDecoder.decode(path.substring(path.lastIndexOf('/') + 1).replace("+", "%2B"), java.nio.charset.StandardCharsets.UTF_8);
    }

    /** As the driver's drop(): the collection and its documents, or the view. */
    public void drop() { call("drop", new Document()); }

    /** The collection whose requests run in the session's transaction (the driver's overloads, session first). */
    private IshedCollection in(ClientSession s) { return new IshedCollection(client, path, s); }

    public FindIterable find(ClientSession s) { return in(s).find(); }
    public FindIterable find(ClientSession s, Document filter) { return in(s).find(filter); }
    public long countDocuments(ClientSession s) { return in(s).countDocuments(); }
    public long countDocuments(ClientSession s, Document filter) { return in(s).countDocuments(filter); }
    public BulkWriteResult bulkWrite(ClientSession s, List<? extends WriteModel<Document>> requests) { return in(s).bulkWrite(requests); }
    public BulkWriteResult bulkWrite(ClientSession s, List<? extends WriteModel<Document>> requests, boolean ordered) { return in(s).bulkWrite(requests, ordered); }
    public AggregateIterable aggregate(ClientSession s, List<Document> pipeline) { return in(s).aggregate(pipeline); }
    public List<Object> distinct(ClientSession s, String fieldName) { return in(s).distinct(fieldName); }
    public List<Object> distinct(ClientSession s, String fieldName, Document filter) { return in(s).distinct(fieldName, filter); }
    public Results.InsertOneResult insertOne(ClientSession s, Document document) { return in(s).insertOne(document); }
    public Results.InsertManyResult insertMany(ClientSession s, List<Document> documents) { return in(s).insertMany(documents); }
    public Results.UpdateResult updateOne(ClientSession s, Document filter, Document update) { return in(s).updateOne(filter, update); }
    public Results.UpdateResult updateOne(ClientSession s, Document filter, Document update, UpdateOptions options) { return in(s).updateOne(filter, update, options); }
    public Results.UpdateResult updateMany(ClientSession s, Document filter, Document update) { return in(s).updateMany(filter, update); }
    public Results.UpdateResult replaceOne(ClientSession s, Document filter, Document replacement) { return in(s).replaceOne(filter, replacement); }
    public Results.UpdateResult replaceOne(ClientSession s, Document filter, Document replacement, UpdateOptions options) { return in(s).replaceOne(filter, replacement, options); }
    public Results.DeleteResult deleteOne(ClientSession s, Document filter) { return in(s).deleteOne(filter); }
    public Results.DeleteResult deleteMany(ClientSession s, Document filter) { return in(s).deleteMany(filter); }

    /** As the driver's watch: the collection's changes as they happen. */
    public ChangeStreamIterable watch() { return watch(List.of()); }
    public ChangeStreamIterable watch(List<Document> pipeline) { return new ChangeStreamIterable(client, path + ":watch", pipeline); }

    /** As the driver's createIndex(Indexes.compoundIndex(…), new IndexOptions().unique(true)): answers its name. */
    public String createIndex(Document keys) { return createIndex(keys, new IndexOptions()); }
    public String createIndex(Document keys, IndexOptions options) { return (String) call("createIndex", new IndexModel(keys, options).spec()).get("name"); }

    @SuppressWarnings("unchecked")
    public List<String> createIndexes(List<IndexModel> models) {
        List<Document> specs = new ArrayList<>();
        for (IndexModel m : models) specs.add(m.spec());
        return (List<String>) (List<?>) call("createIndexes", new Document("indexes", specs)).get("names");
    }

    /** The indexes, as MongoDB gives them (v, key, name, unique…): into(list), first(), or a for loop. */
    @SuppressWarnings("unchecked")
    public List<Document> listIndexes() {
        List<Document> out = new ArrayList<>();
        for (Object i : (List<Object>) call("listIndexes", new Document()).get("indexes")) out.add((Document) i);
        return out;
    }

    public void dropIndex(String name) { call("dropIndex", new Document("index", name)); }
    public void dropIndex(Document keys) { call("dropIndex", new Document("index", keys)); }
    public void dropIndexes() { call("dropIndexes", new Document()); }

    public FindIterable find() { return new FindIterable(this, new Document()); }
    public FindIterable find(Document filter) { return new FindIterable(this, filter); }

    public long countDocuments() { return countDocuments(new Document()); }
    public long countDocuments(Document filter) { return ((Number) call("countDocuments", new Document("filter", filter)).get("count")).longValue(); }

    /** As the driver's bulkWrite: the operations in order; ordered false goes on past a refusal. */
    public BulkWriteResult bulkWrite(List<? extends WriteModel<Document>> requests) { return bulkWrite(requests, true); }

    public BulkWriteResult bulkWrite(List<? extends WriteModel<Document>> requests, boolean ordered) {
        List<Document> ops = new java.util.ArrayList<>();
        for (WriteModel<Document> m : requests) ops.add(m.operation());
        return new BulkWriteResult(call("bulkWrite", new Document("operations", ops).append("ordered", ordered)));
    }

    /** As the driver's aggregate: the pipeline run by the server. */
    public AggregateIterable aggregate(List<Document> pipeline) { return new AggregateIterable(this, pipeline); }

    /** The collection's number of documents, from the statistics (at once on a big collection). */
    public long estimatedDocumentCount() { return ((Number) call("estimatedDocumentCount", new Document()).get("count")).longValue(); }

    /** The different values of a field among the documents found (a list's items one by one). */
    public List<Object> distinct(String fieldName) { return distinct(fieldName, new Document()); }

    @SuppressWarnings("unchecked")
    public List<Object> distinct(String fieldName, Document filter) {
        return (List<Object>) call("distinct", new Document("key", fieldName).append("filter", filter)).get("values");
    }

    /** Adds a document; as the driver does, its "_id" is set in it when it had none. */
    public Results.InsertOneResult insertOne(Document document) {
        String id = (String) call("insertOne", new Document("document", document)).get("insertedId");
        document.put("_id", id);
        return new Results.InsertOneResult(id);
    }

    public Results.InsertManyResult insertMany(List<Document> documents) {
        List<String> ids = new ArrayList<>();
        for (int start = 0; start < documents.size(); start += 1000) {
            List<Document> chunk = documents.subList(start, Math.min(start + 1000, documents.size()));
            Document got = (Document) call("insertMany", new Document("documents", chunk)).get("insertedIds");
            for (int i = 0; i < chunk.size(); i++) {
                String id = (String) got.get(Integer.toString(i));
                chunk.get(i).put("_id", id);
                ids.add(id);
            }
        }
        return new Results.InsertManyResult(ids);
    }

    private Results.UpdateResult change(String method, Document filter, String key, Document value, Boolean upsert) {
        Document body = new Document("filter", filter).append(key, value);
        if (upsert != null) body.append("upsert", upsert);
        Document r = call(method, body);
        return new Results.UpdateResult(((Number) r.get("matchedCount")).longValue(), ((Number) r.get("modifiedCount")).longValue(), (String) r.get("upsertedId"));
    }

    public Results.UpdateResult updateOne(Document filter, Document update) { return updateOne(filter, update, new UpdateOptions()); }
    public Results.UpdateResult updateOne(Document filter, Document update, UpdateOptions options) { return change("updateOne", filter, "update", update, options.isUpsert()); }
    public Results.UpdateResult updateMany(Document filter, Document update) { return change("updateMany", filter, "update", update, null); }
    public Results.UpdateResult replaceOne(Document filter, Document replacement) { return replaceOne(filter, replacement, new UpdateOptions()); }
    public Results.UpdateResult replaceOne(Document filter, Document replacement, UpdateOptions options) { return change("replaceOne", filter, "replacement", replacement, options.isUpsert()); }

    public Results.DeleteResult deleteOne(Document filter) { return new Results.DeleteResult(((Number) call("deleteOne", new Document("filter", filter)).get("deletedCount")).longValue()); }
    public Results.DeleteResult deleteMany(Document filter) { return new Results.DeleteResult(((Number) call("deleteMany", new Document("filter", filter)).get("deletedCount")).longValue()); }
}
