package ished;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * IshedDB for Java, written as MongoDB's Java driver is:
 * new IshedClient(url, key).getDatabase("clinique").getCollection("patients").find(new Document("ville", "Dakar"))
 * Only Java's own libraries. The key never travels but over HTTPS (or to this machine).
 */
public final class IshedClient {
    private static final Pattern CLUSTER = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9-]{0,63}$");
    private final HttpClient http;
    private final String root;
    private final String v1;
    private final String key;
    private final Duration timeout;

    public IshedClient(String url, String key) { this(url, key, null); }

    /** A client for one project (its key), in one of its clusters (null: the project's first, "Cluster0"). */
    public IshedClient(String url, String key, String cluster) {
        URI uri = URI.create(url);
        String host = uri.getHost();
        boolean local = "127.0.0.1".equals(host) || "localhost".equals(host) || "[::1]".equals(host) || "::1".equals(host);
        if (!"https".equals(uri.getScheme()) && !("http".equals(uri.getScheme()) && local)) {
            throw new IshedException("invalid-argument", "IshedDB is reached over HTTPS (plain HTTP only to this machine)", 0);
        }
        if (key == null || !key.startsWith("ished_sk_")) throw new IshedException("invalid-argument", "a key ished_sk_… is needed", 0);
        if (cluster != null && !CLUSTER.matcher(cluster).matches()) throw new IshedException("invalid-argument", "cluster: 1 to 64 letters, digits or \"-\"", 0);
        this.root = url.replaceAll("/+$", "");
        this.v1 = cluster == null ? "/v1" : "/v1/clusters/" + cluster;
        this.key = key;
        this.timeout = Duration.ofSeconds(30);
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public IshedDatabase getDatabase(String name) {
        return new IshedDatabase(this, v1 + "/bases/" + encode(name));
    }

    static String encode(String name) {
        return URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /** As the driver's watch: the changes of the whole cluster. */
    public ChangeStreamIterable watch() { return watch(List.of()); }
    public ChangeStreamIterable watch(List<Document> pipeline) { return new ChangeStreamIterable(this, v1 + ":watch", pipeline); }

    /** A session, as the driver's: its transactions are held by the server. */
    public ClientSession startSession() { return new ClientSession(this, v1); }

    /** As the driver's listDatabases(): each base's name, sizeOnDisk and empty (a key limited to some: those). */
    public List<Document> listDatabases() {
        List<Document> out = new ArrayList<>();
        for (Object d : (List<?>) ((Document) send("GET", v1 + "/bases", null)).get("databases")) out.add((Document) d);
        return out;
    }

    /** As the driver's listDatabaseNames(). */
    public List<String> listDatabaseNames() {
        List<String> names = new ArrayList<>();
        for (Document d : listDatabases()) names.add(d.getString("name"));
        return names;
    }

    Object send(String method, String path, Document body) { return send(method, path, body, null); }

    Object send(String method, String path, Document body, String transaction) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(root + path)).timeout(timeout).header("Authorization", "Bearer " + key);
        if (transaction != null) b.header("X-Ished-Transaction", transaction);
        if (body == null) b.GET();
        else b.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(Json.write(body), StandardCharsets.UTF_8));
        HttpResponse<String> res;
        try {
            res = http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (java.io.IOException e) {
            throw new IshedException("unavailable", e.getMessage(), 0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IshedException("cancelled", "interrupted", 0);
        }
        Object parsed = res.body().isEmpty() ? null : Json.read(res.body());
        if (res.statusCode() >= 400) {
            Document error = parsed instanceof Document d && d.get("error") instanceof Document e ? e : new Document();
            throw new IshedException(error.get("code") instanceof String c ? c : "internal", error.get("message") instanceof String m ? m : "HTTP " + res.statusCode(), res.statusCode());
        }
        return parsed;
    }

    /**
     * The files' bytes (GridFSBucket): an upload session's own address (auth false: no key sent to it), or a download
     * with the key, its body read as it comes. Over HTTPS (or to this machine) only.
     */
    HttpResponse<java.io.InputStream> sendBytes(String method, String where, byte[] body, java.util.Map<String, String> headers, boolean auth) {
        URI target = URI.create(where.startsWith("http://") || where.startsWith("https://") ? where : root + where);
        String host = target.getHost();
        boolean local = "127.0.0.1".equals(host) || "localhost".equals(host) || "[::1]".equals(host) || "::1".equals(host);
        if (!"https".equals(target.getScheme()) && !("http".equals(target.getScheme()) && local)) {
            throw new IshedException("invalid-argument", "a file's address is reached over HTTPS only", 0);
        }
        HttpRequest.Builder b = HttpRequest.newBuilder(target);
        if (auth) b.header("Authorization", "Bearer " + key);
        headers.forEach(b::header);
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
        HttpResponse<java.io.InputStream> res;
        try {
            res = http.send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
            if (res.statusCode() >= 400) {
                String text;
                try (java.io.InputStream in = res.body()) {
                    text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
                Object parsed = text.isEmpty() ? null : Json.read(text);
                Document error = parsed instanceof Document d && d.get("error") instanceof Document e ? e : new Document();
                throw new IshedException(error.get("code") instanceof String c ? c : "internal", error.get("message") instanceof String m ? m : "HTTP " + res.statusCode(), res.statusCode());
            }
        } catch (java.io.IOException e) {
            throw new IshedException("unavailable", e.getMessage(), 0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IshedException("cancelled", "interrupted", 0);
        }
        return res;
    }

    @SuppressWarnings("unchecked")
    List<Object> list(String path) {
        return (List<Object>) send("GET", path, null);
    }
}
