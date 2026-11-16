<?php
// IshedDB for PHP, written as the mongodb/mongodb library is:
//   $client->selectDatabase('boutique')->selectCollection('clients')->find(['ville' => 'Dakar'], ['sort' => ['age' => -1]])
// Only PHP's curl and json extensions. The key never travels but over HTTPS (or to this machine). Values:
// DateTimeInterface <-> dates, Ished\Binary <-> bytes, integers beyond 2**53 <-> big integers (as strings in Ished\Int64).
declare(strict_types=1);

namespace Ished;

final class IshedException extends \RuntimeException
{
    public function __construct(public readonly string $ishedCode, string $message, public readonly ?int $status = null)
    {
        parent::__construct("{$ishedCode}: {$message}");
    }
}

final class Binary
{
    public function __construct(public readonly string $data) {}
}

final class Int64
{
    public function __construct(public readonly string $value) {}
}

final class Wire
{
    private const SAFE_INT = 9007199254740991;

    public static function to(mixed $v, bool $object = false): mixed
    {
        if ($v instanceof \DateTimeInterface) {
            $utc = \DateTimeImmutable::createFromInterface($v)->setTimezone(new \DateTimeZone('UTC'));
            return ['~date' => $utc->format('Y-m-d\TH:i:s.v\Z')];
        }
        if ($v instanceof Binary) return ['~bytes' => base64_encode($v->data)];
        if ($v instanceof Int64) return ['~int' => $v->value];
        if (is_int($v)) return abs($v) <= self::SAFE_INT ? $v : ['~int' => (string) $v];
        if (is_array($v)) {
            if ($v === []) return $object ? new \stdClass() : [];
            if (array_is_list($v)) return array_map(fn ($x) => self::to($x), $v);
            $out = [];
            foreach ($v as $k => $x) $out[(string) $k] = self::to($x, is_array($x) && $x === [] && !is_int($k));
            return $out;
        }
        if (is_object($v)) return self::to(get_object_vars($v), true);
        return $v;
    }

    public static function from(mixed $v): mixed
    {
        if (!is_array($v)) return $v;
        if (count($v) === 1) {
            $k = array_key_first($v);
            if ($k === '~date') return new \DateTimeImmutable($v[$k]);
            if ($k === '~int') {
                $n = $v[$k];
                return (strlen(ltrim($n, '-')) < 19 || bccomp_safe($n)) ? (int) $n : new Int64($n);
            }
            if ($k === '~bytes') return new Binary((string) base64_decode($v[$k], true));
        }
        return array_map(fn ($x) => self::from($x), $v);
    }
}

/** True when a 19-digit text still fits in PHP's 64-bit integers. */
function bccomp_safe(string $n): bool
{
    $digits = ltrim($n, '-');
    return strlen($digits) === 19 && strcmp($digits, str_starts_with($n, '-') ? '9223372036854775808' : '9223372036854775807') <= 0;
}

final class InsertOneResult
{
    public function __construct(private readonly string $id) {}
    public function getInsertedId(): string { return $this->id; }
    public function isAcknowledged(): bool { return true; }
}

final class InsertManyResult
{
    public function __construct(private readonly array $ids) {}
    public function getInsertedIds(): array { return $this->ids; }
    public function getInsertedCount(): int { return count($this->ids); }
}

final class UpdateResult
{
    public function __construct(private readonly int $matched, private readonly int $modified, private readonly ?string $upserted) {}
    public function getMatchedCount(): int { return $this->matched; }
    public function getModifiedCount(): int { return $this->modified; }
    public function getUpsertedId(): ?string { return $this->upserted; }
    public function getUpsertedCount(): int { return $this->upserted === null ? 0 : 1; }
}

final class DeleteResult
{
    public function __construct(private readonly int $deleted) {}
    public function getDeletedCount(): int { return $this->deleted; }
}

final class BulkWriteResult
{
    public function __construct(private readonly array $r) {}
    public function getInsertedCount(): int { return $this->r['insertedCount']; }
    public function getMatchedCount(): int { return $this->r['matchedCount']; }
    public function getModifiedCount(): int { return $this->r['modifiedCount']; }
    public function getDeletedCount(): int { return $this->r['deletedCount']; }
    public function getUpsertedCount(): int { return $this->r['upsertedCount']; }
    public function getUpsertedIds(): array { return $this->r['upsertedIds']; }
}

final class Collection
{
    public const MAX_DOCUMENTS = 100000;

    /** @param \Closure(string, string, mixed): mixed $call */
    public function __construct(private readonly \Closure $call, private readonly string $path) {}

    /** As the library's watch: the collection's changes as they happen. */
    public function watch(array $pipeline = [], array $options = []): ChangeStream
    {
        return new ChangeStream($this->call, "{$this->path}:watch", $pipeline, $options);
    }

    /** As the library's drop(): the collection and its documents, or the view. */
    public function drop(array $options = []): array
    {
        return ($this->call)('POST', "{$this->path}:drop", (object) []);
    }

    /**
     * As the library's explain(new Find(…)), explain(new Aggregate(…)), Count, Distinct: the operation's plan, in
     * MongoDB's shape. Option 'verbosity': queryPlanner, executionStats or allPlansExecution (the default).
     */
    public function explain(Explainable $explainable, array $options = []): array
    {
        $body = ['explain' => $explainable->explainCommand()];
        if (isset($options['verbosity'])) $body['verbosity'] = $options['verbosity'];
        return Wire::from(($this->call)('POST', "{$this->path}:explain", $body));
    }

    /**
     * As the library's createIndex(['a' => 1, 'b' => -1], ['unique' => true]); a text index ['titre' => 'text'] with
     * 'weights' and 'default_language'; 'partialFilterExpression'. Answers its name.
     */
    public function createIndex(array|object $key, array $options = []): string
    {
        return ($this->call)('POST', "{$this->path}:createIndex", ['key' => Wire::to($key, true)] + Wire::to($options, true))['name'];
    }

    /** @param array<array> $indexes each ['key' => [...], 'unique' => …]; answers their names. */
    public function createIndexes(array $indexes, array $options = []): array
    {
        $specs = array_map(fn ($i) => ['key' => Wire::to($i['key'], true)] + Wire::to(array_diff_key($i, ['key' => 1]), true), $indexes);
        return ($this->call)('POST', "{$this->path}:createIndexes", ['indexes' => $specs])['names'];
    }

    /** @return \ArrayIterator<IndexInfo> */
    public function listIndexes(array $options = []): \ArrayIterator
    {
        $list = ($this->call)('POST', "{$this->path}:listIndexes", new \stdClass())['indexes'];
        return new \ArrayIterator(array_map(fn ($i) => new IndexInfo(Wire::from($i)), $list));
    }

    /** By its name, or its keys (['a' => 1]). */
    public function dropIndex(string|array|object $indexName, array $options = []): void
    {
        ($this->call)('POST', "{$this->path}:dropIndex", ['index' => is_string($indexName) ? $indexName : Wire::to($indexName, true)]);
    }

    public function dropIndexes(array $options = []): void
    {
        ($this->call)('POST', "{$this->path}:dropIndexes", new \stdClass());
    }

    /** Every document found (read 1000 at a time), as arrays. Options: sort, projection, limit, skip. */
    public function find(array|object $filter = [], array $options = []): \ArrayIterator
    {
        $limit = (int) ($options['limit'] ?? 0);
        $token = null;
        $out = [];
        while (true) {
            $want = $limit > 0 ? min(1000, $limit - count($out)) : 1000;
            $body = ['filter' => Wire::to($filter, true), 'limit' => $want];
            // The next page starts after the last one read (its token), never at an offset that moves with writes.
            if ($token !== null) $body['pageToken'] = $token; else $body['skip'] = (int) ($options['skip'] ?? 0);
            if (isset($options['sort'])) $body['sort'] = Wire::to($options['sort'], true);
            if (isset($options['projection'])) $body['projection'] = Wire::to($options['projection'], true);
            $page = ($this->in($options))('POST', "{$this->path}:find", $body);
            foreach ($page['documents'] as $d) $out[] = Wire::from($d);
            $token = $page['nextPageToken'] ?? null;
            if ($token === null || ($limit > 0 && count($out) >= $limit)) return new \ArrayIterator($out);
            if (count($out) >= self::MAX_DOCUMENTS) throw new IshedException('resource-exhausted', 'more than ' . self::MAX_DOCUMENTS . ' documents: narrow the filter');
        }
    }

    public function findOne(array|object $filter = [], array $options = []): ?array
    {
        $found = iterator_to_array($this->find($filter, ['limit' => 1] + $options));
        return $found[0] ?? null;
    }

    public function countDocuments(array|object $filter = [], array $options = []): int
    {
        return ($this->in($options))('POST', "{$this->path}:countDocuments", ['filter' => Wire::to($filter, true)])['count'];
    }

    /** As the library's aggregate: the pipeline run by the server; every result, read 1000 at a time. */
    public function aggregate(array $pipeline, array $options = []): \ArrayIterator
    {
        $out = [];
        for ($skip = 0; ; $skip += 1000) {
            $page = ($this->in($options))('POST', "{$this->path}:aggregate", ['pipeline' => Wire::to($pipeline), 'limit' => 1000, 'skip' => $skip]);
            foreach ($page['documents'] as $d) $out[] = Wire::from($d);
            if (!$page['more']) return new \ArrayIterator($out);
            if (count($out) >= self::MAX_DOCUMENTS) throw new IshedException('resource-exhausted', 'more than ' . self::MAX_DOCUMENTS . ' documents: add $limit');
        }
    }

    /**
     * As the library's bulkWrite: [['insertOne' => [$doc]], ['updateOne' => [$filter, $update, ['upsert' => true]]],
     * ['updateMany' => [$filter, $update]], ['replaceOne' => [$filter, $doc]], ['deleteOne' => [$filter]], ['deleteMany' => [$filter]]].
     */
    public function bulkWrite(array $operations, array $options = []): BulkWriteResult
    {
        $ops = [];
        foreach ($operations as $op) {
            $name = array_key_first($op);
            $args = $op[$name];
            $ops[] = [$name => match ($name) {
                'insertOne' => ['document' => Wire::to($args[0], true)],
                'updateOne', 'updateMany' => ['filter' => Wire::to($args[0], true), 'update' => Wire::to($args[1], true), 'upsert' => (bool) ($args[2]['upsert'] ?? false)],
                'replaceOne' => ['filter' => Wire::to($args[0], true), 'replacement' => Wire::to($args[1], true), 'upsert' => (bool) ($args[2]['upsert'] ?? false)],
                'deleteOne', 'deleteMany' => ['filter' => Wire::to($args[0], true)],
                default => throw new IshedException('invalid-argument', "unknown bulkWrite operation $name"),
            }];
        }
        $r = ($this->in($options))('POST', "{$this->path}:bulkWrite", ['operations' => $ops, 'ordered' => (bool) ($options['ordered'] ?? true)]);
        return new BulkWriteResult($r);
    }

    /** The collection's number of documents, from the statistics (at once on a big collection). */
    public function estimatedDocumentCount(array $options = []): int
    {
        return ($this->in($options))('POST', "{$this->path}:estimatedDocumentCount", new \stdClass())['count'];
    }

    /** The different values of a field among the documents found (a list's items one by one). */
    public function distinct(string $fieldName, array|object $filter = [], array $options = []): array
    {
        $values = ($this->in($options))('POST', "{$this->path}:distinct", ['key' => $fieldName, 'filter' => Wire::to($filter, true)])['values'];
        return array_map(fn ($v) => Wire::from($v), $values);
    }

    public function insertOne(array|object $document, array $options = []): InsertOneResult
    {
        return new InsertOneResult(($this->in($options))('POST', "{$this->path}:insertOne", ['document' => Wire::to($document, true)])['insertedId']);
    }

    public function insertMany(array $documents, array $options = []): InsertManyResult
    {
        $ids = [];
        foreach (array_chunk($documents, 1000) as $chunk) {
            $r = ($this->in($options))('POST', "{$this->path}:insertMany", ['documents' => array_map(fn ($d) => Wire::to($d, true), $chunk)]);
            for ($i = 0; $i < count($r['insertedIds']); $i++) $ids[] = $r['insertedIds'][(string) $i];
        }
        return new InsertManyResult($ids);
    }

    public function updateOne(array|object $filter, array|object $update, array $options = []): UpdateResult
    {
        return $this->change('updateOne', $filter, 'update', $update, (bool) ($options['upsert'] ?? false), $options);
    }

    public function updateMany(array|object $filter, array|object $update, array $options = []): UpdateResult
    {
        return $this->change('updateMany', $filter, 'update', $update, null, $options);
    }

    public function replaceOne(array|object $filter, array|object $replacement, array $options = []): UpdateResult
    {
        return $this->change('replaceOne', $filter, 'replacement', $replacement, (bool) ($options['upsert'] ?? false), $options);
    }

    public function deleteOne(array|object $filter = [], array $options = []): DeleteResult
    {
        return new DeleteResult(($this->in($options))('POST', "{$this->path}:deleteOne", ['filter' => Wire::to($filter, true)])['deletedCount']);
    }

    public function deleteMany(array|object $filter = [], array $options = []): DeleteResult
    {
        return new DeleteResult(($this->in($options))('POST', "{$this->path}:deleteMany", ['filter' => Wire::to($filter, true)])['deletedCount']);
    }

    private function change(string $method, array|object $filter, string $key, array|object $value, ?bool $upsert, array $options): UpdateResult
    {
        $body = ['filter' => Wire::to($filter, true), $key => Wire::to($value, true)];
        if ($upsert !== null) $body['upsert'] = $upsert;
        $r = ($this->in($options))('POST', "{$this->path}:{$method}", $body);
        return new UpdateResult($r['matchedCount'], $r['modifiedCount'], $r['upsertedId'] ?? null);
    }

    /** The sender of a request: in the session's transaction when the options give one ('session' => $session). */
    private function in(array $options): \Closure
    {
        $session = $options['session'] ?? null;
        return $session instanceof Session ? $session->bind($this->call) : $this->call;
    }
}

/**
 * As the library's session: $session = $client->startSession(); then with_transaction($session, function ($session) use ($c) {
 * $c->updateOne($filter, $update, ['session' => $session]); }). The transaction is held by the server (60 seconds at
 * most, 20 without a request); a conflict with another transaction answers "aborted", and with_transaction runs it again.
 */
final class Session
{
    private ?string $id = null;

    public function __construct(private readonly \Closure $call, private readonly string $v1) {}

    public function isInTransaction(): bool { return $this->id !== null; }

    public function startTransaction(array $options = []): void
    {
        if ($this->id !== null) throw new IshedException('failed-precondition', 'a transaction is already started in this session');
        $this->id = ($this->call)('POST', "{$this->v1}:beginTransaction", new \stdClass())['transaction'];
    }

    /** @internal A sender whose requests run in this session's transaction (none started: the sender as given). */
    public function bind(\Closure $call): \Closure
    {
        if ($this->id === null) return $call;
        $header = 'X-Ished-Transaction: ' . $this->id;
        return static fn (string $method, string $path, mixed $body): mixed => $call($method, $path, $body, [$header]);
    }

    public function commitTransaction(): void
    {
        if ($this->id === null) throw new IshedException('failed-precondition', 'no transaction started in this session');
        $tx = $this->id;
        $this->id = null;
        ($this->call)('POST', "{$this->v1}:commit", ['transaction' => $tx]);
    }

    public function abortTransaction(): void
    {
        if ($this->id === null) return;
        $tx = $this->id;
        $this->id = null;
        try {
            ($this->call)('POST', "{$this->v1}:rollback", ['transaction' => $tx]);
        } catch (IshedException $e) {
            if ($e->ishedCode !== 'not-found') throw $e; // already ended by the server (an error, or too long): nothing kept
        }
    }

    public function endSession(): void { $this->abortTransaction(); }
}

/** As the library's MongoDB\with_transaction: the callback in a transaction, run again on "aborted" (5 times at most). */
function with_transaction(Session $session, callable $callback, array $transactionOptions = []): void
{
    for ($attempt = 1; ; $attempt++) {
        $session->startTransaction($transactionOptions);
        try {
            $callback($session);
            $session->commitTransaction();
            return;
        } catch (\Throwable $e) {
            try {
                $session->abortTransaction();
            } catch (IshedException) {
            }
            if (!($e instanceof IshedException && $e->ishedCode === 'aborted') || $attempt >= 5) throw $e;
        }
    }
}

/**
 * As the library's change stream: $stream = $collection->watch($pipeline, ['fullDocument' => 'updateLookup']);
 * for ($stream->rewind(); true; $stream->next()) { if (!$stream->valid()) continue; $change = $stream->current(); }
 * next() waits up to maxAwaitTimeMS (10 s by default, 20 s at most) on the server; getResumeToken(). Opened at once:
 * the changes made from then on are seen. A collection dropped or renamed ends its stream ("invalidate").
 */
final class ChangeStream implements \Iterator
{
    private array $body;
    private int $wait;
    private mixed $position;
    private mixed $at;
    private mixed $token;
    private array $buffer = [];
    private ?array $current = null;
    private int $key = -1;
    private bool $closed = false;

    public function __construct(private readonly \Closure $call, private readonly string $path, array $pipeline, array $options)
    {
        $this->body = ['pipeline' => Wire::to($pipeline)];
        if (isset($options['fullDocument'])) $this->body['fullDocument'] = $options['fullDocument'];
        if (isset($options['fullDocumentBeforeChange'])) $this->body['fullDocumentBeforeChange'] = $options['fullDocumentBeforeChange'];
        if (isset($options['batchSize'])) $this->body['batchSize'] = (int) $options['batchSize'];
        $this->wait = (int) ($options['maxAwaitTimeMS'] ?? 10000);
        $this->position = $options['resumeAfter'] ?? $options['startAfter'] ?? null;
        $this->at = $options['startAtOperationTime'] ?? null;
        $this->token = $this->position;
        if ($this->position === null && $this->at === null) $this->fetch(0);
    }

    private function fetch(int $wait): void
    {
        $body = $this->body + ['maxAwaitTimeMS' => $wait];
        if ($this->position !== null) $body['resumeAfter'] = $this->position;
        elseif ($this->at !== null) $body['startAtOperationTime'] = Wire::to($this->at);
        $r = ($this->call)('POST', $this->path, $body);
        $this->at = null;
        $this->position = $r['resumeToken'];
        foreach ($r['changes'] as $c) $this->buffer[] = Wire::from($c);
        if (!$this->buffer) $this->token = $r['resumeToken'];
    }

    private function move(int $wait): void
    {
        if (!$this->buffer && !$this->closed) $this->fetch($wait);
        $this->current = array_shift($this->buffer);
        if ($this->current === null) return;
        $this->key++;
        $this->token = $this->buffer ? $this->current['_id'] : $this->position;
        if ($this->current['operationType'] === 'invalidate') $this->closed = true;
    }

    /** The first change already there, without waiting. */
    public function rewind(): void { if ($this->key < 0) $this->move(0); }
    /** The next change, waiting for one up to maxAwaitTimeMS (valid() is false if none came). */
    public function next(): void { $this->move($this->wait); }
    public function valid(): bool { return $this->current !== null; }
    public function current(): ?array { return $this->current; }
    public function key(): ?int { return $this->current === null ? null : $this->key; }
    /** The token to resume after the last change given (resumeAfter). */
    public function getResumeToken(): mixed { return $this->token; }
}

/** An index as the library's IndexInfo: getName(), getKey(), isUnique(), isText(); and read as an array. */
final class IndexInfo implements \ArrayAccess
{
    public function __construct(private readonly array $info) {}
    public function getName(): string { return $this->info['name']; }
    public function getKey(): array { return $this->info['key']; }
    public function isUnique(): bool { return (bool) ($this->info['unique'] ?? false); }
    public function isText(): bool { return isset($this->info['key']['_fts']); }
    public function offsetExists(mixed $offset): bool { return isset($this->info[$offset]); }
    public function offsetGet(mixed $offset): mixed { return $this->info[$offset] ?? null; }
    public function offsetSet(mixed $offset, mixed $value): void { throw new \LogicException('an IndexInfo is read only'); }
    public function offsetUnset(mixed $offset): void { throw new \LogicException('an IndexInfo is read only'); }
}

/** What explain() takes, as the library's MongoDB\Operation\Explainable. */
interface Explainable
{
    /** The command explained ({ find | aggregate | count | distinct: collection, … }). */
    public function explainCommand(): array;
}

/** As MongoDB\Operation\Find: a find to explain (options sort, projection, skip, limit). */
final class Find implements Explainable
{
    public function __construct(private readonly string $databaseName, private readonly string $collectionName, private readonly array|object $filter = [], private readonly array $options = []) {}

    public function explainCommand(): array
    {
        $c = ['find' => $this->collectionName, 'filter' => Wire::to($this->filter, true)];
        foreach (['sort', 'projection'] as $k) if (isset($this->options[$k])) $c[$k] = Wire::to($this->options[$k], true);
        foreach (['skip', 'limit'] as $k) if (!empty($this->options[$k])) $c[$k] = (int) $this->options[$k];
        return $c;
    }
}

/** As MongoDB\Operation\Aggregate: a pipeline to explain. */
final class Aggregate implements Explainable
{
    public function __construct(private readonly string $databaseName, private readonly string $collectionName, private readonly array $pipeline, private readonly array $options = []) {}

    public function explainCommand(): array
    {
        return ['aggregate' => $this->collectionName, 'pipeline' => Wire::to($this->pipeline)];
    }
}

/** As MongoDB\Operation\Count: a count to explain. */
final class Count implements Explainable
{
    public function __construct(private readonly string $databaseName, private readonly string $collectionName, private readonly array|object $filter = [], private readonly array $options = []) {}

    public function explainCommand(): array
    {
        return ['count' => $this->collectionName, 'query' => Wire::to($this->filter, true)];
    }
}

/** As MongoDB\Operation\Distinct: a distinct to explain. */
final class Distinct implements Explainable
{
    public function __construct(private readonly string $databaseName, private readonly string $collectionName, private readonly string $fieldName, private readonly array|object $filter = [], private readonly array $options = []) {}

    public function explainCommand(): array
    {
        return ['distinct' => $this->collectionName, 'key' => $this->fieldName, 'query' => Wire::to($this->filter, true)];
    }
}

/** As the library's CollectionInfo: a collection or a view, from listCollections(). */
final class CollectionInfo implements \ArrayAccess
{
    public function __construct(private readonly array $info) {}
    public function getName(): string { return $this->info['name']; }
    public function getType(): string { return $this->info['type']; }
    public function getOptions(): array { return $this->info['options']; }
    public function getInfo(): array { return $this->info['info']; }
    public function offsetExists(mixed $offset): bool { return isset($this->info[$offset]); }
    public function offsetGet(mixed $offset): mixed { return $this->info[$offset] ?? null; }
    public function offsetSet(mixed $offset, mixed $value): void { throw new \LogicException('a CollectionInfo is read only'); }
    public function offsetUnset(mixed $offset): void { throw new \LogicException('a CollectionInfo is read only'); }
}

/** A base of the cluster, as the library's DatabaseInfo: getName(), getSizeOnDisk(), isEmpty(), or as an array. */
final class DatabaseInfo implements \ArrayAccess
{
    public function __construct(private readonly array $info) {}
    public function getName(): string { return $this->info['name']; }
    public function getSizeOnDisk(): int { return (int) $this->info['sizeOnDisk']; }
    public function isEmpty(): bool { return (bool) $this->info['empty']; }
    public function offsetExists(mixed $offset): bool { return isset($this->info[$offset]); }
    public function offsetGet(mixed $offset): mixed { return $this->info[$offset] ?? null; }
    public function offsetSet(mixed $offset, mixed $value): void { throw new \LogicException('a DatabaseInfo is read only'); }
    public function offsetUnset(mixed $offset): void { throw new \LogicException('a DatabaseInfo is read only'); }
}

final class Database
{
    public function __construct(private readonly \Closure $call, private readonly string $path, private readonly ?\Closure $raw = null) {}

    /** As the library's selectGridFSBucket(['bucketName' => 'fs']): the files of a bucket, as GridFS (GridFS.php). */
    public function selectGridFSBucket(array $options = []): GridFS\Bucket
    {
        return new GridFS\Bucket($this, $this->call, $this->raw, $this->path, $options);
    }

    /** As the library's createCollection($name, ['viewOn' => …, 'pipeline' => […]]): an empty collection, or a view. */
    public function createCollection(string $collectionName, array $options = []): array
    {
        $body = [];
        if (isset($options['viewOn'])) $body['viewOn'] = $options['viewOn'];
        if (isset($options['pipeline'])) $body['pipeline'] = Wire::to($options['pipeline']);
        // Its validator, as the library's: 'validator' => ['$jsonSchema' => […]], 'validationLevel', 'validationAction'.
        if (isset($options['validator'])) $body['validator'] = Wire::to($options['validator'], true);
        foreach (['validationLevel', 'validationAction'] as $k) if (isset($options[$k])) $body[$k] = $options[$k];
        return ($this->call)('POST', "{$this->path}/collections/" . rawurlencode($collectionName) . ':create', (object) $body);
    }

    /** As the library's modifyCollection($name, ['viewOn' => …, 'pipeline' => […]]): a view's source and pipeline (collMod). */
    public function modifyCollection(string $collectionName, array $collectionOptions, array $options = []): array
    {
        return $this->command(['collMod' => $collectionName] + $collectionOptions)->current();
    }

    /** As the library's dropCollection(): the collection and its documents, or the view. */
    public function dropCollection(string $collectionName, array $options = []): array
    {
        return ($this->call)('POST', "{$this->path}/collections/" . rawurlencode($collectionName) . ':drop', (object) []);
    }

    /** As the library's listCollections(): each a CollectionInfo (name, type "collection" or "view", options). */
    public function listCollections(array $options = []): \ArrayIterator
    {
        return new \ArrayIterator(array_map(fn ($c) => new CollectionInfo([
            'name' => $c['name'],
            'type' => $c['type'],
            'options' => $c['type'] === 'view' ? ['viewOn' => $c['viewOn'], 'pipeline' => $c['pipeline']] : [],
            'info' => ['readOnly' => $c['type'] === 'view'],
        ]), ($this->call)('GET', "{$this->path}/collections", null)));
    }

    /** As the library's command(): create, collMod, drop, explain; a cursor of the one reply document. */
    public function command(array|object $command, array $options = []): \ArrayIterator
    {
        return new \ArrayIterator([Wire::from(($this->call)('POST', "{$this->path}:runCommand", Wire::to($command, true)))]);
    }

    /** As the library's watch: the changes of every collection of the base. */
    public function watch(array $pipeline = [], array $options = []): ChangeStream
    {
        return new ChangeStream($this->call, "{$this->path}:watch", $pipeline, $options);
    }

    public function selectCollection(string $name): Collection
    {
        return new Collection($this->call, "{$this->path}/collections/" . rawurlencode($name));
    }

    public function __get(string $name): Collection { return $this->selectCollection($name); }

    /** @return string[] */
    public function listCollectionNames(): array
    {
        return array_map(fn ($c) => $c['name'], ($this->call)('GET', "{$this->path}/collections", null));
    }
}

final class Client
{
    private readonly \Closure $call;
    private readonly \Closure $raw;
    private readonly string $v1;

    /** A client for one project (its key), in one of its clusters (by default the first, "Cluster0"). */
    public function __construct(string $url, string $key, ?string $cluster = null, int $timeout = 30)
    {
        $parts = parse_url($url);
        $local = in_array($parts['host'] ?? '', ['127.0.0.1', 'localhost', '::1', '[::1]'], true);
        if (($parts['scheme'] ?? '') !== 'https' && !(($parts['scheme'] ?? '') === 'http' && $local)) {
            throw new IshedException('invalid-argument', 'IshedDB is reached over HTTPS (plain HTTP only to this machine)');
        }
        if (!str_starts_with($key, 'ished_sk_')) throw new IshedException('invalid-argument', 'a key ished_sk_… is needed');
        if ($cluster !== null && !preg_match('/^[A-Za-z0-9][A-Za-z0-9-]{0,63}$/', $cluster)) throw new IshedException('invalid-argument', 'cluster: 1 to 64 letters, digits or "-"');
        $root = rtrim($url, '/');
        $this->v1 = $cluster === null ? '/v1' : "/v1/clusters/{$cluster}";
        $this->call = static function (string $method, string $path, mixed $body, array $extra = []) use ($root, $key, $timeout): mixed {
            $h = curl_init($root . $path);
            $headers = ["Authorization: Bearer {$key}", ...$extra];
            curl_setopt_array($h, [CURLOPT_RETURNTRANSFER => true, CURLOPT_CUSTOMREQUEST => $method, CURLOPT_TIMEOUT => $timeout]);
            if ($body !== null) {
                $headers[] = 'Content-Type: application/json';
                curl_setopt($h, CURLOPT_POSTFIELDS, json_encode($body, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION));
            }
            curl_setopt($h, CURLOPT_HTTPHEADER, $headers);
            $text = curl_exec($h);
            $status = curl_getinfo($h, CURLINFO_RESPONSE_CODE);
            if ($text === false) throw new IshedException('unavailable', curl_error($h));
            $json = $text === '' ? null : json_decode($text, true, 512, JSON_THROW_ON_ERROR | JSON_BIGINT_AS_STRING);
            if ($status >= 400) {
                $err = $json['error'] ?? [];
                throw new IshedException($err['code'] ?? 'internal', $err['message'] ?? "HTTP {$status}", $status);
            }
            return $json;
        };
        // The files' bytes (GridFS.php): an upload session's own address (no key sent to it), or a download with the
        // key, its bytes written to $sink as they come.
        $this->raw = static function (string $method, string $where, ?string $body, array $extra, bool $auth, $sink) use ($root, $key, $timeout): mixed {
            $target = preg_match('#^https?://#', $where) ? $where : $root . $where;
            $t = parse_url($target);
            $local = in_array($t['host'] ?? '', ['127.0.0.1', 'localhost', '::1', '[::1]'], true);
            if (($t['scheme'] ?? '') !== 'https' && !(($t['scheme'] ?? '') === 'http' && $local)) {
                throw new IshedException('invalid-argument', "a file's address is reached over HTTPS only");
            }
            $h = curl_init($target);
            $headers = [...($auth ? ["Authorization: Bearer {$key}"] : []), ...$extra];
            $error = '';
            curl_setopt_array($h, [CURLOPT_CUSTOMREQUEST => $method, CURLOPT_TIMEOUT => max($timeout, 3600), CURLOPT_HTTPHEADER => $headers]);
            if ($body !== null) curl_setopt($h, CURLOPT_POSTFIELDS, $body);
            curl_setopt($h, CURLOPT_WRITEFUNCTION, static function ($h, string $data) use ($sink, &$error): int {
                if ($sink !== null && curl_getinfo($h, CURLINFO_RESPONSE_CODE) < 400) fwrite($sink, $data);
                else $error .= $data;
                return strlen($data);
            });
            $ok = curl_exec($h);
            $status = curl_getinfo($h, CURLINFO_RESPONSE_CODE);
            if ($ok === false) throw new IshedException('unavailable', curl_error($h));
            if ($status >= 400) {
                $err = json_decode($error, true)['error'] ?? [];
                throw new IshedException($err['code'] ?? 'internal', $err['message'] ?? "HTTP {$status}", $status);
            }
            return $sink === null && $error !== '' ? json_decode($error, true) : null;
        };
    }

    /** As the library's listDatabases(): each base of the cluster (a key limited to some: those). */
    public function listDatabases(array $options = []): \ArrayIterator
    {
        return new \ArrayIterator(array_map(fn ($d) => new DatabaseInfo($d), ($this->call)('GET', "{$this->v1}/bases", null)['databases']));
    }

    /** As the library's listDatabaseNames(). */
    public function listDatabaseNames(array $options = []): \ArrayIterator
    {
        return new \ArrayIterator(array_map(fn ($d) => $d['name'], ($this->call)('GET', "{$this->v1}/bases", null)['databases']));
    }

    public function selectDatabase(string $name): Database
    {
        return new Database($this->call, "{$this->v1}/bases/" . rawurlencode($name), $this->raw);
    }

    /** As the library's watch: the changes of the whole cluster. */
    public function watch(array $pipeline = [], array $options = []): ChangeStream
    {
        return new ChangeStream($this->call, "{$this->v1}:watch", $pipeline, $options);
    }

    public function startSession(): Session
    {
        return new Session($this->call, $this->v1);
    }

    public function __get(string $name): Database { return $this->selectDatabase($name); }
}

require_once __DIR__ . '/GridFS.php';
