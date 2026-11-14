package ished;

/**
 * The document before each update, replacement or deletion, as the driver's FullDocumentBeforeChange: not given
 * (DEFAULT, OFF), given when kept (WHEN_AVAILABLE), or required (REQUIRED); kept when the collection has
 * changeStreamPreAndPostImages.
 */
public enum FullDocumentBeforeChange {
    DEFAULT("default"),
    OFF("off"),
    WHEN_AVAILABLE("whenAvailable"),
    REQUIRED("required");

    private final String value;

    FullDocumentBeforeChange(String value) { this.value = value; }

    public String getValue() { return value; }
}
