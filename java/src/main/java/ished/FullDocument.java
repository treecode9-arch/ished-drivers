package ished;

/**
 * What an update change carries, as the driver's FullDocument: nothing more (DEFAULT), the document as it is now
 * (UPDATE_LOOKUP), or as it was right after the update (WHEN_AVAILABLE, REQUIRED: kept when the collection has
 * changeStreamPreAndPostImages).
 */
public enum FullDocument {
    DEFAULT("default"),
    UPDATE_LOOKUP("updateLookup"),
    WHEN_AVAILABLE("whenAvailable"),
    REQUIRED("required");

    private final String value;

    FullDocument(String value) { this.value = value; }

    public String getValue() { return value; }
}
