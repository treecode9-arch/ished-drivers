package ished;

/** As MongoDB's UpdateOptions: new UpdateOptions().upsert(true). */
public final class UpdateOptions {
    private boolean upsert;

    public UpdateOptions upsert(boolean value) {
        upsert = value;
        return this;
    }

    public boolean isUpsert() { return upsert; }
}
