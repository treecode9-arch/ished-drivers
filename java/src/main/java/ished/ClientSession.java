package ished;

/**
 * A session, as the driver's: try (ClientSession s = client.startSession()) { s.withTransaction(() -> { coll.updateOne(s, f, u); return null; }); }
 * Each method given the session first runs in its transaction. The server holds it (60 seconds at most, 20 without a
 * request); a conflict with another transaction answers "aborted", and withTransaction runs the body again (5 times at most).
 */
public final class ClientSession implements AutoCloseable {
    private final IshedClient client;
    private final String v1;
    private String id;

    ClientSession(IshedClient client, String v1) {
        this.client = client;
        this.v1 = v1;
    }

    public boolean hasActiveTransaction() { return id != null; }

    String transaction() { return id; }

    public void startTransaction() {
        if (id != null) throw new IshedException("failed-precondition", "a transaction is already started in this session", 0);
        id = (String) ((Document) client.send("POST", v1 + ":beginTransaction", new Document(), null)).get("transaction");
    }

    public void commitTransaction() {
        if (id == null) throw new IshedException("failed-precondition", "no transaction started in this session", 0);
        String tx = id;
        id = null;
        client.send("POST", v1 + ":commit", new Document("transaction", tx), null);
    }

    public void abortTransaction() {
        if (id == null) return;
        String tx = id;
        id = null;
        try {
            client.send("POST", v1 + ":rollback", new Document("transaction", tx), null);
        } catch (IshedException e) {
            if (!"not-found".equals(e.getCode())) throw e; // already ended by the server (an error, or too long): nothing kept
        }
    }

    /** The body in a transaction, committed; run again when the server answers "aborted". */
    public <T> T withTransaction(TransactionBody<T> body) {
        for (int attempt = 1; ; attempt++) {
            startTransaction();
            try {
                T out = body.execute();
                commitTransaction();
                return out;
            } catch (RuntimeException | Error e) {
                try {
                    abortTransaction();
                } catch (IshedException ignored) {
                    // the first error is the one given back
                }
                if (!(e instanceof IshedException i && "aborted".equals(i.getCode())) || attempt >= 5) throw e;
            }
        }
    }

    /** Ends the session; an unfinished transaction is undone. */
    @Override
    public void close() { abortTransaction(); }
}
