package ished;

import java.util.List;

/** What an update changed, as the driver's UpdateDescription: the fields set (dotted paths) and the fields removed. */
public final class UpdateDescription {
    private final Document updatedFields;
    private final List<String> removedFields;

    UpdateDescription(Document updatedFields, List<String> removedFields) {
        this.updatedFields = updatedFields;
        this.removedFields = removedFields;
    }

    public Document getUpdatedFields() { return updatedFields; }
    public List<String> getRemovedFields() { return removedFields; }
}
