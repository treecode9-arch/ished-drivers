package ished;

/** As the driver's ValidationLevel: which writes a collection's validator checks. */
public enum ValidationLevel {
    OFF("off"),
    STRICT("strict"),
    MODERATE("moderate");

    private final String value;

    ValidationLevel(String value) { this.value = value; }

    String value() { return value; }
}
