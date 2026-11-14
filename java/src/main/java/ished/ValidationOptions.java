package ished;

/** As the driver's ValidationOptions: the validator ({ $jsonSchema: … }), its level and its action. */
public final class ValidationOptions {
    private Document validator;
    private ValidationLevel level;
    private ValidationAction action;

    public ValidationOptions validator(Document validator) { this.validator = validator; return this; }
    public ValidationOptions validationLevel(ValidationLevel level) { this.level = level; return this; }
    public ValidationOptions validationAction(ValidationAction action) { this.action = action; return this; }

    void into(Document body) {
        if (validator != null) body.append("validator", validator);
        if (level != null) body.append("validationLevel", level.value());
        if (action != null) body.append("validationAction", action.value());
    }
}
