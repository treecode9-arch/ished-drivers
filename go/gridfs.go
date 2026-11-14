package ished

// IshedDB's files from Go, as the driver's GridFS bucket (v2, db.GridFSBucket()): UploadFromStream(WithID),
// OpenUploadStream(WithID), DownloadToStream(ByName), OpenDownloadStream(ByName), Find, Delete, Rename, Drop — the
// same names and arguments. The bytes go to IshedDB in pieces (a resumable upload, as Cloud Storage's) and come back
// as a stream; a file's document is one of the collection <bucket>.files. IshedDB's own: SignedURL for a browser.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const gridPiece = 8 * 1024 * 1024 // one request (a multiple of IshedDB's 256 KB segments)

var bucketName = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,93}$`)

// rawCaller sends bytes: an upload session's own address (auth false: no key sent to it), or a download with the key.
type rawCaller func(ctx context.Context, method, where string, body []byte, headers map[string]string, auth bool) (*http.Response, error)

// GridFSBucketOptions: Name (by default "fs").
type GridFSBucketOptions struct{ Name string }

// GridFSUploadOptions: Metadata, ContentType.
type GridFSUploadOptions struct {
	Metadata    any
	ContentType string
}

// GridFSNameOptions: Revision (0 the first one sent, -1 the last one, the default).
type GridFSNameOptions struct{ Revision int }

// GridFSBucket is a bucket of files, as the driver's.
type GridFSBucket struct {
	db   *Database
	name string
	path string
}

// GridFSBucket gives the files of a bucket of the database, as the driver's db.GridFSBucket(opts).
func (d *Database) GridFSBucket(opts ...GridFSBucketOptions) (*GridFSBucket, error) {
	name := "fs"
	if len(opts) > 0 && opts[0].Name != "" {
		name = opts[0].Name
	}
	if !bucketName.MatchString(name) {
		return nil, &Error{Code: "invalid-argument", Message: `bucket name: 1 to 94 letters, digits, "_", "-" or "."`}
	}
	return &GridFSBucket{db: d, name: name, path: d.path + "/buckets/" + url.PathEscape(name)}, nil
}

func (b *GridFSBucket) fileURL(id any, action string) string {
	return b.path + "/files/" + url.PathEscape(fmt.Sprint(id)) + ":" + action
}

// failure reads IshedDB's error from a response.
func failure(res *http.Response) error {
	e := &Error{Code: "internal", Message: fmt.Sprintf("HTTP %d", res.StatusCode), Status: res.StatusCode}
	var body struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	if json.NewDecoder(res.Body).Decode(&body) == nil {
		if body.Error.Code != "" {
			e.Code = body.Error.Code
		}
		if body.Error.Message != "" {
			e.Message = body.Error.Message
		}
	}
	return e
}

func readPiece(r io.Reader) ([]byte, error) {
	piece := make([]byte, gridPiece)
	n, err := io.ReadFull(r, piece)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return piece[:n], nil
	}
	return piece[:n], err
}

// upload sends [source] in pieces of 8 MB, the length told with the last one; answers the file's id.
func (b *GridFSBucket) upload(ctx context.Context, id any, filename string, source io.Reader, opts []GridFSUploadOptions) (string, error) {
	body := M{"filename": filename, "length": nil}
	if id != nil {
		body["id"] = fmt.Sprint(id)
	}
	if len(opts) > 0 {
		if opts[0].Metadata != nil {
			body["metadata"] = toWire(opts[0].Metadata)
		}
		if opts[0].ContentType != "" {
			body["contentType"] = opts[0].ContentType
		}
	}
	piece, err := readPiece(source)
	if err != nil {
		return "", err
	}
	if len(piece) == 0 {
		body["length"] = 0
	}
	started, _, err := b.db.call(ctx, "POST", b.path+"/uploads", body)
	if err != nil {
		return "", err
	}
	fileID := fmt.Sprint(started["id"])
	if _, done := started["file"]; done {
		return fileID, nil
	}
	session, _ := started["uploadUrl"].(string)
	at := 0
	for {
		following, err := readPiece(source)
		if err != nil {
			return "", err
		}
		last := len(following) == 0
		total := "*"
		if last {
			total = fmt.Sprint(at + len(piece))
		}
		res, err := b.db.raw(ctx, "PUT", session, piece, map[string]string{"Content-Range": fmt.Sprintf("bytes %d-%d/%s", at, at+len(piece)-1, total), "Content-Type": "application/octet-stream"}, false)
		if err != nil {
			return "", err
		}
		if res.StatusCode >= 400 {
			err := failure(res)
			res.Body.Close()
			return "", err
		}
		res.Body.Close()
		at += len(piece)
		if last {
			return fileID, nil
		}
		piece = following
	}
}

// UploadFromStream sends a file, as the driver's: its id (made by IshedDB).
func (b *GridFSBucket) UploadFromStream(ctx context.Context, filename string, source io.Reader, opts ...GridFSUploadOptions) (string, error) {
	return b.upload(ctx, nil, filename, source, opts)
}

// UploadFromStreamWithID sends a file kept under the id given.
func (b *GridFSBucket) UploadFromStreamWithID(ctx context.Context, fileID any, filename string, source io.Reader, opts ...GridFSUploadOptions) error {
	_, err := b.upload(ctx, fileID, filename, source, opts)
	return err
}

// GridFSUploadStream is a file being written: Write, then Close; FileID once closed (or the id given).
type GridFSUploadStream struct {
	FileID   string
	bucket   *GridFSBucket
	ctx      context.Context
	filename string
	opts     []GridFSUploadOptions
	id       any
	buf      bytes.Buffer
	closed   bool
}

// OpenUploadStream gives a stream to write a file into, sent when it is closed.
func (b *GridFSBucket) OpenUploadStream(ctx context.Context, filename string, opts ...GridFSUploadOptions) (*GridFSUploadStream, error) {
	return &GridFSUploadStream{bucket: b, ctx: ctx, filename: filename, opts: opts}, nil
}

// OpenUploadStreamWithID is OpenUploadStream with the file's id given.
func (b *GridFSBucket) OpenUploadStreamWithID(ctx context.Context, fileID any, filename string, opts ...GridFSUploadOptions) (*GridFSUploadStream, error) {
	return &GridFSUploadStream{bucket: b, ctx: ctx, filename: filename, opts: opts, id: fileID, FileID: fmt.Sprint(fileID)}, nil
}

func (s *GridFSUploadStream) Write(p []byte) (int, error) {
	if s.closed {
		return 0, &Error{Code: "failed-precondition", Message: "this upload stream is closed"}
	}
	return s.buf.Write(p)
}

// Close sends the file.
func (s *GridFSUploadStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	id, err := s.bucket.upload(s.ctx, s.id, s.filename, &s.buf, s.opts)
	s.FileID = id
	return err
}

// GridFSDownloadStream is a file being read: Read, Close, GetFile (its document).
type GridFSDownloadStream struct {
	body io.ReadCloser
	file M
}

func (s *GridFSDownloadStream) Read(p []byte) (int, error) { return s.body.Read(p) }

// Close ends the reading.
func (s *GridFSDownloadStream) Close() error { return s.body.Close() }

// GetFile is the file's document (_id, length, filename, metadata, sha256…).
func (s *GridFSDownloadStream) GetFile() M { return s.file }

// OpenDownloadStream reads a file, as the driver's.
func (b *GridFSBucket) OpenDownloadStream(ctx context.Context, fileID any) (*GridFSDownloadStream, error) {
	file, err := b.db.Collection(b.name+".files").FindOne(ctx, M{"_id": fileID})
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, &Error{Code: "not-found", Message: fmt.Sprintf("no file %v in the bucket %s", fileID, b.name), Status: 404}
	}
	res, err := b.db.raw(ctx, "GET", b.fileURL(fileID, "download"), nil, nil, true)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 400 {
		defer res.Body.Close()
		return nil, failure(res)
	}
	return &GridFSDownloadStream{body: res.Body, file: file}, nil
}

// DownloadToStream writes a file's bytes to [stream] as they come; answers how many.
func (b *GridFSBucket) DownloadToStream(ctx context.Context, fileID any, stream io.Writer) (int64, error) {
	s, err := b.OpenDownloadStream(ctx, fileID)
	if err != nil {
		return 0, err
	}
	defer s.Close()
	return io.Copy(stream, s)
}

func (b *GridFSBucket) byName(ctx context.Context, filename string, opts []GridFSNameOptions) (any, error) {
	revision := -1
	if len(opts) > 0 {
		revision = opts[0].Revision
	}
	order, skip := int64(1), int64(revision)
	if revision < 0 {
		order, skip = -1, int64(-revision-1)
	}
	cur, err := b.db.Collection(b.name+".files").Find(ctx, M{"filename": filename}, FindOptions{Sort: D{{Key: "uploadDate", Value: order}}, Skip: skip, Limit: 1})
	if err != nil {
		return nil, err
	}
	var found []M
	if err := cur.All(ctx, &found); err != nil {
		return nil, err
	}
	if len(found) == 0 {
		return nil, &Error{Code: "not-found", Message: fmt.Sprintf("no revision %d of the file %s in the bucket %s", revision, filename, b.name), Status: 404}
	}
	return found[0]["_id"], nil
}

// OpenDownloadStreamByName reads a revision of a file by its name (the last one by default).
func (b *GridFSBucket) OpenDownloadStreamByName(ctx context.Context, filename string, opts ...GridFSNameOptions) (*GridFSDownloadStream, error) {
	id, err := b.byName(ctx, filename, opts)
	if err != nil {
		return nil, err
	}
	return b.OpenDownloadStream(ctx, id)
}

// DownloadToStreamByName writes a revision of a file, by its name, to [stream].
func (b *GridFSBucket) DownloadToStreamByName(ctx context.Context, filename string, stream io.Writer, opts ...GridFSNameOptions) (int64, error) {
	id, err := b.byName(ctx, filename, opts)
	if err != nil {
		return 0, err
	}
	return b.DownloadToStream(ctx, id, stream)
}

// Find gives the files' documents (the collection <bucket>.files), as the driver's.
func (b *GridFSBucket) Find(ctx context.Context, filter any, opts ...FindOptions) (*Cursor, error) {
	return b.db.Collection(b.name+".files").Find(ctx, filter, opts...)
}

// Delete deletes a file: its document and its bytes.
func (b *GridFSBucket) Delete(ctx context.Context, fileID any) error {
	_, _, err := b.db.call(ctx, "POST", b.fileURL(fileID, "delete"), M{})
	return err
}

// Rename gives a file another name.
func (b *GridFSBucket) Rename(ctx context.Context, fileID any, newFilename string) error {
	_, _, err := b.db.call(ctx, "POST", b.fileURL(fileID, "rename"), M{"filename": newFilename})
	return err
}

// Drop deletes every file of the bucket, and its <bucket>.files collection.
func (b *GridFSBucket) Drop(ctx context.Context) error {
	_, _, err := b.db.call(ctx, "POST", b.path+":drop", M{})
	return err
}

// SignedURL is IshedDB's own: an address for a browser (no key in it), as Cloud Storage's signed URLs.
func (b *GridFSBucket) SignedURL(ctx context.Context, fileID any, expiresIn int, disposition string) (string, error) {
	if disposition == "" {
		disposition = "attachment"
	}
	m, _, err := b.db.call(ctx, "POST", b.fileURL(fileID, "signUrl"), M{"expiresIn": expiresIn, "disposition": disposition})
	if err != nil {
		return "", err
	}
	u, _ := m["url"].(string)
	return u, nil
}

// newRawCaller: the files' bytes, over HTTPS (or to this machine), the key only where it is asked.
func newRawCaller(root, key string, client *http.Client) rawCaller {
	return func(ctx context.Context, method, where string, body []byte, headers map[string]string, auth bool) (*http.Response, error) {
		target := where
		if !strings.HasPrefix(where, "http://") && !strings.HasPrefix(where, "https://") {
			target = root + where
		}
		u, err := url.Parse(target)
		if err != nil {
			return nil, err
		}
		local := u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"
		if u.Scheme != "https" && !(u.Scheme == "http" && local) {
			return nil, &Error{Code: "invalid-argument", Message: "a file's address is reached over HTTPS only"}
		}
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, target, reader)
		if err != nil {
			return nil, err
		}
		if auth {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return client.Do(req)
	}
}
