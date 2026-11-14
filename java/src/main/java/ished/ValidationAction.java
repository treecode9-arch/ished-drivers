package ished;

/** As the driver's ValidationAction: a document that breaks the validator refused (ERROR) or written (WARN). */
public enum ValidationAction {
    ERROR("error"),
    WARN("warn");

    private final String value;

    ValidationAction(String value) { this.value = value; }

    String value() { return value; }
}
