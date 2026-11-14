package ished;

import java.util.LinkedHashMap;
import java.util.Map;

/** A document, a filter, a sort or an update — as MongoDB's org.bson.Document (its keys keep their order). */
public final class Document extends LinkedHashMap<String, Object> {
    private static final long serialVersionUID = 1L;

    public Document() {}

    public Document(String key, Object value) {
        put(key, value);
    }

    /** Every entry of [other] appended, in its order. */
    public Document append(Document other) {
        putAll(other);
        return this;
    }

    public Document(Map<String, ?> map) {
        putAll(map);
    }

    public Document append(String key, Object value) {
        put(key, value);
        return this;
    }

    public String getString(String key) {
        return (String) get(key);
    }

    public Integer getInteger(String key) {
        Object v = get(key);
        return v == null ? null : ((Number) v).intValue();
    }

    public java.util.Date getDate(String key) {
        return (java.util.Date) get(key);
    }
}
