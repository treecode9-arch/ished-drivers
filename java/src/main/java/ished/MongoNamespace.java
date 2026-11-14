package ished;

/** A base and a collection, as the driver's MongoNamespace. */
public final class MongoNamespace {
    private final String databaseName;
    private final String collectionName;

    MongoNamespace(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public String getDatabaseName() { return databaseName; }
    public String getCollectionName() { return collectionName; }
    public String getFullName() { return collectionName == null ? databaseName : databaseName + "." + collectionName; }
}
