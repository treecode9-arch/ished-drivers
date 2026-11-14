package ished;

/** What a transaction does, as the driver's TransactionBody: given to ClientSession.withTransaction. */
@FunctionalInterface
public interface TransactionBody<T> {
    T execute();
}
