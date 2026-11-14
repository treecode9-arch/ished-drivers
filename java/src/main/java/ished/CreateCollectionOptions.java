package ished;

/** As the driver's CreateCollectionOptions: the collection's validation options. */
public final class CreateCollectionOptions {
    private ValidationOptions validation;

    public CreateCollectionOptions validationOptions(ValidationOptions validation) { this.validation = validation; return this; }

    Document toDocument() {
        Document body = new Document();
        if (validation != null) validation.into(body);
        return body;
    }
}
