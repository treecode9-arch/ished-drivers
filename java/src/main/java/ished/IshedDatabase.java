package ished;

import java.util.ArrayList;
import java.util.List;

/** A database of the cluster, as MongoDB's MongoDatabase. */
public final class IshedDatabase {
    private final IshedClient client;
    private final String path;

    IshedDatabase(IshedClient client, String path) {
        this.client = client;
        this.path = path;
    }

    String path() { return path; }

    IshedClient client() { return client; }

    /** As the driver's watch: the changes of every collection of the base. */
    public ChangeStreamIterable watch() { return watch(List.of()); }
    public ChangeStreamIterable watch(List<Document> pipeline) { return new ChangeStreamIterable(client, path + ":watch", pipeline); }

    public IshedCollection getCollection(String name) {
        return new IshedCollection(client, path + "/collections/" + IshedClient.encode(name));
    }

    /** As the driver's createView(viewName, viewOn, pipeline): read only, computed when read. */
    public void createView(String viewName, String viewOn, List<Document> pipeline) {
        client.send("POST", path + "/collections/" + IshedClient.encode(viewName) + ":create", new Document("viewOn", viewOn).append("pipeline", pipeline));
    }

    /** As the driver's createCollection(name): an empty collection. */
    public void createCollection(String name) {
        client.send("POST", path + "/collections/" + IshedClient.encode(name) + ":create", new Document());
    }

    /** As the driver's createCollection(name, options): with its validator, level and action. */
    public void createCollection(String name, CreateCollectionOptions options) {
        client.send("POST", path + "/collections/" + IshedClient.encode(name) + ":create", options.toDocument());
    }

    /** As the driver's runCommand: create, collMod (a view's viewOn and pipeline), drop, explain. */
    public Document runCommand(Document command) {
        return (Document) client.send("POST", path + ":runCommand", command);
    }

    /** As the driver's listCollections(): name, type ("collection" or "view"), options, info. */
    public List<Document> listCollections() {
        List<Document> out = new ArrayList<>();
        for (Object o : client.list(path + "/collections")) {
            Document c = (Document) o;
            boolean view = "view".equals(c.getString("type"));
            out.add(new Document("name", c.getString("name")).append("type", c.getString("type"))
                .append("options", view ? new Document("viewOn", c.get("viewOn")).append("pipeline", c.get("pipeline")) : new Document())
                .append("info", new Document("readOnly", view)));
        }
        return out;
    }

    public List<String> listCollectionNames() {
        List<String> names = new ArrayList<>();
        for (Object c : client.list(path + "/collections")) names.add(((Document) c).getString("name"));
        return names;
    }
}
