<?php
// IshedDB's files from PHP, as the mongodb/mongodb library's GridFS\Bucket ($database->selectGridFSBucket()):
// uploadFromStream, openUploadStream (with getFileIdForStream), downloadToStream(ByName), openDownloadStream(ByName),
// find, findOne, delete, rename, drop — the same names and arguments. The bytes go to IshedDB in pieces (a resumable
// upload, as Cloud Storage's) and come back as a stream; a file's document is one of the collection <bucket>.files.
// IshedDB's own: signedUrl() for a browser.
declare(strict_types=1);

namespace Ished\GridFS;

use Ished\IshedException;
use Ished\Wire;

final class Bucket
{
    private const PIECE = 8 * 1024 * 1024; // one request (a multiple of IshedDB's 256 KB segments)
    private readonly string $path;
    private readonly string $bucketName;

    public function __construct(private readonly \Ished\Database $database, private readonly \Closure $call, private readonly \Closure $raw, string $databasePath, array $options = [])
    {
        $this->bucketName = $options['bucketName'] ?? 'fs';
        if (!preg_match('/^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,93}$/', $this->bucketName)) {
            throw new IshedException('invalid-argument', 'bucketName: 1 to 94 letters, digits, "_", "-" or "."');
        }
        $this->path = "{$databasePath}/buckets/" . rawurlencode($this->bucketName);
    }

    public function getBucketName(): string { return $this->bucketName; }

    // ---- writing ------------------------------------------------------------------------------------------------

    /** As the library's uploadFromStream($filename, $source, ['_id' => …, 'metadata' => […]]): the file's id. */
    public function uploadFromStream(string $filename, $source, array $options = []): string
    {
        if (!is_resource($source)) throw new IshedException('invalid-argument', 'uploadFromStream: a readable stream');
        $body = ['filename' => $filename, 'length' => null];
        if (isset($options['_id'])) $body['id'] = (string) $options['_id'];
        if (isset($options['metadata'])) $body['metadata'] = Wire::to($options['metadata']);
        if (isset($options['contentType'])) $body['contentType'] = $options['contentType'];
        $piece = self::read($source);
        if ($piece === '') $body['length'] = 0;
        $started = ($this->call)('POST', "{$this->path}/uploads", $body);
        if (array_key_exists('file', $started)) return $started['id'];
        $at = 0;
        while (true) {
            $following = self::read($source);
            $last = $following === '';
            $total = $last ? (string) ($at + strlen($piece)) : '*';
            ($this->raw)('PUT', $started['uploadUrl'], $piece, ['Content-Range: bytes ' . $at . '-' . ($at + strlen($piece) - 1) . "/{$total}", 'Content-Type: application/octet-stream'], false, null);
            $at += strlen($piece);
            if ($last) return $started['id'];
            $piece = $following;
        }
    }

    /** As the library's openUploadStream($filename, $options): a writable stream, sent when closed (fclose). */
    public function openUploadStream(string $filename, array $options = [])
    {
        UploadStream::register();
        $id = (string) ($options['_id'] ?? bin2hex(random_bytes(12)));
        $context = stream_context_create(['ished' => ['bucket' => $this, 'filename' => $filename, 'options' => ['_id' => $id] + $options]]);
        return fopen('ished-upload://' . rawurlencode($filename), 'w', false, $context);
    }

    /** As the library's getFileIdForStream($stream): the id of a file being written or read. */
    public function getFileIdForStream($stream): string
    {
        $meta = stream_get_meta_data($stream);
        if (($meta['wrapper_data'] ?? null) instanceof UploadStream) return $meta['wrapper_data']->id;
        throw new IshedException('invalid-argument', 'getFileIdForStream: a stream of this bucket');
    }

    // ---- reading ------------------------------------------------------------------------------------------------

    /** As the library's downloadToStream($id, $destination): the bytes written to $destination as they come. */
    public function downloadToStream(mixed $id, $destination): void
    {
        $this->document($id);
        ($this->raw)('GET', "{$this->path}/files/" . rawurlencode((string) $id) . ':download', null, [], true, $destination);
    }

    /** As the library's openDownloadStream($id): a readable stream of the file's bytes. */
    public function openDownloadStream(mixed $id)
    {
        $stream = fopen('php://temp', 'w+b');
        $this->downloadToStream($id, $stream);
        rewind($stream);
        return $stream;
    }

    /** As the library: revision 0 the first one sent, -1 the last one (default). */
    public function downloadToStreamByName(string $filename, $destination, array $options = []): void
    {
        $this->downloadToStream($this->byName($filename, (int) ($options['revision'] ?? -1)), $destination);
    }

    public function openDownloadStreamByName(string $filename, array $options = [])
    {
        return $this->openDownloadStream($this->byName($filename, (int) ($options['revision'] ?? -1)));
    }

    // ---- the files' documents ---------------------------------------------------------------------------------------

    /** As the library's find($filter, $options): the files' documents (the collection <bucket>.files). */
    public function find(array|object $filter = [], array $options = []): \ArrayIterator
    {
        return $this->database->selectCollection("{$this->bucketName}.files")->find($filter, $options);
    }

    public function findOne(array|object $filter = [], array $options = []): ?array
    {
        foreach ($this->find($filter, ['limit' => 1] + $options) as $doc) return $doc;
        return null;
    }

    /** As the library's delete($id): the file's document and its bytes. */
    public function delete(mixed $id): void
    {
        ($this->call)('POST', "{$this->path}/files/" . rawurlencode((string) $id) . ':delete', (object) []);
    }

    public function rename(mixed $id, string $newFilename): void
    {
        ($this->call)('POST', "{$this->path}/files/" . rawurlencode((string) $id) . ':rename', ['filename' => $newFilename]);
    }

    /** As the library's drop(): every file of the bucket, and its <bucket>.files collection. */
    public function drop(): void
    {
        ($this->call)('POST', "{$this->path}:drop", (object) []);
    }

    /** IshedDB's own: a signed address for a browser (no key in it), as Cloud Storage's signed URLs: url, expiresAt. */
    public function signedUrl(mixed $id, int $expiresIn = 3600, string $disposition = 'attachment'): array
    {
        return ($this->call)('POST', "{$this->path}/files/" . rawurlencode((string) $id) . ':signUrl', ['expiresIn' => $expiresIn, 'disposition' => $disposition]);
    }

    private function document(mixed $id): array
    {
        return $this->findOne(['_id' => $id]) ?? throw new IshedException('not-found', "no file {$id} in the bucket {$this->bucketName}", 404);
    }

    private function byName(string $filename, int $revision): string
    {
        $found = $this->find(['filename' => $filename], ['sort' => ['uploadDate' => $revision >= 0 ? 1 : -1], 'skip' => $revision >= 0 ? $revision : -$revision - 1, 'limit' => 1]);
        foreach ($found as $doc) return (string) $doc['_id'];
        throw new IshedException('not-found', "no revision {$revision} of the file {$filename} in the bucket {$this->bucketName}", 404);
    }

    private static function read($source): string
    {
        $piece = '';
        while (strlen($piece) < self::PIECE && !feof($source)) {
            $chunk = fread($source, self::PIECE - strlen($piece));
            if ($chunk === false || $chunk === '') break;
            $piece .= $chunk;
        }
        return $piece;
    }
}

/** The stream openUploadStream gives: its bytes kept aside, sent to IshedDB when it is closed. */
final class UploadStream
{
    public $context;
    public string $id = '';
    private $buffer;
    private Bucket $bucket;
    private string $filename;
    private array $options;

    public static function register(): void
    {
        if (!in_array('ished-upload', stream_get_wrappers(), true)) stream_wrapper_register('ished-upload', self::class);
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$opened): bool
    {
        $o = stream_context_get_options($this->context)['ished'] ?? null;
        if (!$o) return false;
        [$this->bucket, $this->filename, $this->options] = [$o['bucket'], $o['filename'], $o['options']];
        $this->id = $this->options['_id'];
        $this->buffer = fopen('php://temp', 'w+b');
        return true;
    }

    public function stream_write(string $data): int { return (int) fwrite($this->buffer, $data); }

    public function stream_close(): void
    {
        rewind($this->buffer);
        $this->bucket->uploadFromStream($this->filename, $this->buffer, $this->options);
        fclose($this->buffer);
    }

    public function stream_eof(): bool { return true; }
}
