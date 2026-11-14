package ished;

/** One operation of bulkWrite, as the driver's WriteModel: InsertOneModel, UpdateOneModel, UpdateManyModel,
 * ReplaceOneModel, DeleteOneModel, DeleteManyModel. */
public interface WriteModel<T> {
    Document operation();
}
