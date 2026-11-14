package ished;

/** The kind of a change, as the driver's OperationType. */
public enum OperationType {
    INSERT("insert"),
    UPDATE("update"),
    REPLACE("replace"),
    DELETE("delete"),
    DROP("drop"),
    RENAME("rename"),
    DROP_DATABASE("dropDatabase"),
    INVALIDATE("invalidate"),
    OTHER("other");

    private final String value;

    OperationType(String value) { this.value = value; }

    public String getValue() { return value; }

    public static OperationType fromString(String text) {
        for (OperationType t : values()) if (t.value.equals(text)) return t;
        return OTHER;
    }
}
