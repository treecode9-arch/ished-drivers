// Package ished is IshedDB for Go, written as MongoDB's Go driver is:
//
//	coll := client.Database("clinique").Collection("patients")
//	cur, err := coll.Find(ctx, ished.M{"ville": "Dakar"}, ished.FindOptions{Sort: ished.D{{Key: "age", Value: -1}}})
//
// Only Go's standard library. The key never travels but over HTTPS (or to this machine). Values: time.Time <-> dates,
// integers beyond 2**53 <-> big integers, []byte <-> bytes.
package ished

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// M is a document or a filter; D an ordered one (for a sort); E one of its elements — as in MongoDB's bson package.
type (
	M map[string]any
	E struct {
		Key   string
		Value any
	}
	D []E
)

// MarshalJSON keeps D's order.
func (d D) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, e := range d {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(e.Key)
		v, err := json.Marshal(toWire(e.Value))
		if err != nil {
			return nil, err
		}
		b.Write(k)
		b.WriteByte(':')
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// Error is a refusal from IshedDB: its code (invalid-argument, not-found…) and message.
type Error struct {
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

const safeInt = 1<<53 - 1

// MaxDocuments is the most a Find reads (narrow the filter beyond).
const MaxDocuments = 100_000

func toWire(v any) any {
	switch x := v.(type) {
	case time.Time:
		return M{"~date": x.UTC().Format("2006-01-02T15:04:05.000Z")}
	case []byte:
		return M{"~bytes": base64.StdEncoding.EncodeToString(x)}
	case int:
		return wireInt(int64(x))
	case int32:
		return int64(x)
	case int64:
		return wireInt(x)
	case M:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = toWire(e)
		}
		return out
	case map[string]any:
		return toWire(M(x))
	case D:
		return x
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = toWire(e)
		}
		return out
	case []M:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = toWire(e)
		}
		return out
	case []string:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = e
		}
		return out
	}
	return v
}

func wireInt(n int64) any {
	if n > safeInt || n < -safeInt {
		return M{"~int": strconv.FormatInt(n, 10)}
	}
	return n
}

func fromWire(v any) any {
	switch x := v.(type) {
	case []any:
		for i, e := range x {
			x[i] = fromWire(e)
		}
		return x
	case map[string]any:
		if len(x) == 1 {
			if s, ok := x["~date"].(string); ok {
				t, err := time.Parse(time.RFC3339Nano, s)
				if err == nil {
					return t
				}
			}
			if s, ok := x["~int"].(string); ok {
				if n, err := strconv.ParseInt(s, 10, 64); err == nil {
					return n
				}
			}
			if s, ok := x["~bytes"].(string); ok {
				if b, err := base64.StdEncoding.DecodeString(s); err == nil {
					return b
				}
			}
		}
		out := M{}
		for k, e := range x {
			out[k] = fromWire(e)
		}
		return out
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		f, _ := x.Float64()
		return f
	}
	return v
}

type caller func(ctx context.Context, method, path string, body any) (map[string]any, []any, error)

// Client is a client for one project (its key), in one of its clusters.
type Client struct {
	call caller
	raw  rawCaller
	v1   string
}

// Options: Cluster (by default the project's first, "Cluster0"), Timeout.
type Options struct {
	Cluster string
	Timeout time.Duration
}

var clusterName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,63}$`)

// Connect makes a client; the key never travels but over HTTPS (or to this machine).
func Connect(rawURL, key string, opts ...Options) (*Client, error) {
	o := Options{Timeout: 30 * time.Second}
	if len(opts) > 0 {
		o.Cluster = opts[0].Cluster
		if opts[0].Timeout > 0 {
			o.Timeout = opts[0].Timeout
		}
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, &Error{Code: "invalid-argument", Message: "a valid address is needed"}
	}
	local := u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return nil, &Error{Code: "invalid-argument", Message: "IshedDB is reached over HTTPS (plain HTTP only to this machine)"}
	}
	if !strings.HasPrefix(key, "ished_sk_") {
		return nil, &Error{Code: "invalid-argument", Message: "a key ished_sk_… is needed"}
	}
	v1 := "/v1"
	if o.Cluster != "" {
		if !clusterName.MatchString(o.Cluster) {
			return nil, &Error{Code: "invalid-argument", Message: `cluster: 1 to 64 letters, digits or "-"`}
		}
		v1 = "/v1/clusters/" + o.Cluster
	}
	root := strings.TrimRight(rawURL, "/")
	httpClient := &http.Client{Timeout: o.Timeout}
	call := func(ctx context.Context, method, path string, body any) (map[string]any, []any, error) {
		var reader io.Reader
		if body != nil {
			b, err := json.Marshal(body)
			if err != nil {
				return nil, nil, err
			}
			reader = bytes.NewReader(b)
		}
		req, err := http.NewRequestWithContext(ctx, method, root+path, reader)
		if err != nil {
			return nil, nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if s := SessionFromContext(ctx); s != nil && s.id != "" {
			req.Header.Set("X-Ished-Transaction", s.id)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := httpClient.Do(req)
		if err != nil {
			return nil, nil, err
		}
		defer res.Body.Close()
		dec := json.NewDecoder(res.Body)
		dec.UseNumber()
		var parsed any
		if err := dec.Decode(&parsed); err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
		if res.StatusCode >= 400 {
			e := &Error{Code: "internal", Message: fmt.Sprintf("HTTP %d", res.StatusCode), Status: res.StatusCode}
			if m, ok := parsed.(map[string]any); ok {
				if inner, ok := m["error"].(map[string]any); ok {
					if c, ok := inner["code"].(string); ok {
						e.Code = c
					}
					if msg, ok := inner["message"].(string); ok {
						e.Message = msg
					}
				}
			}
			return nil, nil, e
		}
		if list, ok := parsed.([]any); ok {
			return nil, list, nil
		}
		m, _ := parsed.(map[string]any)
		return m, nil, nil
	}
	// The files' bytes (gridfs.go): no time limit of their own (a file of 2 GB), the context's only.
	return &Client{call: call, raw: newRawCaller(root, key, &http.Client{}), v1: v1}, nil
}

// Session is a session, as the driver's: sess, _ := client.StartSession(); defer sess.EndSession(ctx); then
// sess.WithTransaction(ctx, func(ctx context.Context) (any, error) { … coll.UpdateOne(ctx, …) … }): every call given
// that ctx runs in the transaction. The server holds it (60 seconds at most, 20 without a request); a conflict with
// another transaction answers "aborted", and WithTransaction runs the function again (5 times at most).
type Session struct {
	call caller
	v1   string
	id   string
}

type sessionKey struct{}

// NewSessionContext gives a ctx whose calls run in the session's transaction (as the driver's).
func NewSessionContext(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, s)
}

// SessionFromContext is the session a ctx carries, or nil.
func SessionFromContext(ctx context.Context) *Session {
	s, _ := ctx.Value(sessionKey{}).(*Session)
	return s
}

// StartSession starts a session (nothing is sent before its transaction).
func (c *Client) StartSession() (*Session, error) { return &Session{call: c.call, v1: c.v1}, nil }

// StartTransaction opens a transaction on the server.
func (s *Session) StartTransaction() error {
	if s.id != "" {
		return &Error{Code: "failed-precondition", Message: "a transaction is already started in this session"}
	}
	m, _, err := s.call(context.Background(), "POST", s.v1+":beginTransaction", M{})
	if err != nil {
		return err
	}
	s.id, _ = m["transaction"].(string)
	return nil
}

// CommitTransaction keeps all that the transaction wrote.
func (s *Session) CommitTransaction(ctx context.Context) error {
	if s.id == "" {
		return &Error{Code: "failed-precondition", Message: "no transaction started in this session"}
	}
	tx := s.id
	s.id = ""
	_, _, err := s.call(ctx, "POST", s.v1+":commit", M{"transaction": tx})
	return err
}

// AbortTransaction undoes all that the transaction wrote.
func (s *Session) AbortTransaction(ctx context.Context) error {
	if s.id == "" {
		return nil
	}
	tx := s.id
	s.id = ""
	_, _, err := s.call(ctx, "POST", s.v1+":rollback", M{"transaction": tx})
	var e *Error
	if errors.As(err, &e) && e.Code == "not-found" { // already ended by the server (an error, or too long): nothing kept
		return nil
	}
	return err
}

// WithTransaction runs fn in a transaction, commits it, and runs it again when the server answers "aborted".
func (s *Session) WithTransaction(ctx context.Context, fn func(ctx context.Context) (any, error)) (any, error) {
	inner := NewSessionContext(ctx, s)
	for attempt := 1; ; attempt++ {
		if err := s.StartTransaction(); err != nil {
			return nil, err
		}
		out, err := func() (any, error) {
			defer func() {
				if r := recover(); r != nil {
					_ = s.AbortTransaction(ctx)
					panic(r)
				}
			}()
			return fn(inner)
		}()
		if err == nil {
			err = s.CommitTransaction(ctx)
		}
		if err == nil {
			return out, nil
		}
		_ = s.AbortTransaction(ctx)
		var e *Error
		if !errors.As(err, &e) || e.Code != "aborted" || attempt >= 5 {
			return nil, err
		}
	}
}

// EndSession ends the session; an unfinished transaction is undone.
func (s *Session) EndSession(ctx context.Context) { _ = s.AbortTransaction(ctx) }

// ChangeStreamOptions are the driver's: FullDocument ("updateLookup", "whenAvailable", "required"),
// FullDocumentBeforeChange ("whenAvailable", "required": kept when the collection has changeStreamPreAndPostImages),
// ResumeAfter or StartAfter (a resume token), StartAtOperationTime, MaxAwaitTime (10 s by default, 20 s at most),
// BatchSize.
type ChangeStreamOptions struct {
	FullDocument             string
	FullDocumentBeforeChange string
	ResumeAfter              any
	StartAfter               any
	StartAtOperationTime     *time.Time
	MaxAwaitTime             time.Duration
	BatchSize                int32
}

// ChangeStream is a change stream, as the driver's: for cs.Next(ctx) { var ev ished.M; cs.Decode(&ev) }; then
// cs.Err(). TryNext, ResumeToken, Close. Opened at once: the changes made from then on are seen. A collection
// dropped or renamed ends its stream ("invalidate").
type ChangeStream struct {
	call     caller
	path     string
	body     M
	wait     time.Duration
	position any
	at       *time.Time
	token    any
	buffer   []M
	closed   bool
	err      error
	// Current is the change Next or TryNext gave.
	Current M
}

func newChangeStream(ctx context.Context, call caller, path string, pipeline any, opts []ChangeStreamOptions) (*ChangeStream, error) {
	if pipeline == nil {
		pipeline = []any{}
	}
	cs := &ChangeStream{call: call, path: path, body: M{"pipeline": toWire(pipeline)}, wait: 10 * time.Second}
	if len(opts) > 0 {
		o := opts[0]
		if o.FullDocument != "" {
			cs.body["fullDocument"] = o.FullDocument
		}
		if o.FullDocumentBeforeChange != "" {
			cs.body["fullDocumentBeforeChange"] = o.FullDocumentBeforeChange
		}
		if o.BatchSize > 0 {
			cs.body["batchSize"] = o.BatchSize
		}
		if o.MaxAwaitTime > 0 {
			cs.wait = o.MaxAwaitTime
		}
		cs.position = o.ResumeAfter
		if cs.position == nil {
			cs.position = o.StartAfter
		}
		cs.at = o.StartAtOperationTime
	}
	cs.token = cs.position
	if cs.position == nil && cs.at == nil {
		if err := cs.fetch(ctx, 0); err != nil {
			return nil, err
		}
	}
	return cs, nil
}

func (cs *ChangeStream) fetch(ctx context.Context, wait time.Duration) error {
	body := M{"maxAwaitTimeMS": wait.Milliseconds()}
	for k, v := range cs.body {
		body[k] = v
	}
	if cs.position != nil {
		body["resumeAfter"] = cs.position
	} else if cs.at != nil {
		body["startAtOperationTime"] = toWire(*cs.at)
	}
	r, _, err := cs.call(ctx, "POST", cs.path, body)
	if err != nil {
		return err
	}
	cs.at = nil
	cs.position = r["resumeToken"]
	changes, _ := r["changes"].([]any)
	for _, c := range changes {
		if m, ok := fromWire(c).(M); ok {
			cs.buffer = append(cs.buffer, m)
		}
	}
	if len(cs.buffer) == 0 {
		cs.token = cs.position
	}
	return nil
}

func (cs *ChangeStream) take() bool {
	cs.Current = cs.buffer[0]
	cs.buffer = cs.buffer[1:]
	if len(cs.buffer) > 0 {
		cs.token = cs.Current["_id"]
	} else {
		cs.token = cs.position
	}
	if cs.Current["operationType"] == "invalidate" {
		cs.closed = true
	}
	return true
}

// Next waits for the next change; false when the stream is closed or fails (Err).
func (cs *ChangeStream) Next(ctx context.Context) bool {
	for len(cs.buffer) == 0 {
		if cs.closed || cs.err != nil {
			return false
		}
		if err := cs.fetch(ctx, cs.wait); err != nil {
			cs.err = err
			return false
		}
	}
	return cs.take()
}

// TryNext gives a change if one comes within a second (or MaxAwaitTime, if shorter).
func (cs *ChangeStream) TryNext(ctx context.Context) bool {
	if len(cs.buffer) == 0 && !cs.closed && cs.err == nil {
		if err := cs.fetch(ctx, min(cs.wait, time.Second)); err != nil {
			cs.err = err
			return false
		}
	}
	if len(cs.buffer) == 0 {
		return false
	}
	return cs.take()
}

// Decode puts the current change into v (*ished.M, or a struct through JSON).
func (cs *ChangeStream) Decode(v any) error {
	if p, ok := v.(*M); ok {
		*p = cs.Current
		return nil
	}
	b, err := json.Marshal(cs.Current)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// ResumeToken is the token to resume after the last change given (ResumeAfter).
func (cs *ChangeStream) ResumeToken() any { return cs.token }

// Err is the error that ended Next or TryNext.
func (cs *ChangeStream) Err() error { return cs.err }

// Close stops the stream.
func (cs *ChangeStream) Close(_ context.Context) error {
	cs.closed = true
	cs.buffer = nil
	return nil
}

// IndexOptions are the driver's (options.Index()): Name, Unique, PartialFilterExpression, Weights, DefaultLanguage,
// WildcardProjection (a wildcard index's fields: M{"attrs": 1} only those, M{"secret": 0} all but those).
type IndexOptions struct {
	Name                    string
	Unique                  bool
	PartialFilterExpression any
	Weights                 any
	DefaultLanguage         string
	WildcardProjection      any
}

// IndexModel is an index as the driver's: Keys (a D, in order: D{{Key: "a", Value: 1}, {Key: "b", Value: -1}}, or
// D{{Key: "titre", Value: "text"}}) and its Options.
type IndexModel struct {
	Keys    any
	Options *IndexOptions
}

// IndexView is the collection's indexes, as the driver's coll.Indexes().
type IndexView struct{ c *Collection }

// Indexes is the collection's index view.
func (c *Collection) Indexes() IndexView { return IndexView{c: c} }

func indexSpec(m IndexModel) M {
	spec := M{"key": toWire(m.Keys)}
	if o := m.Options; o != nil {
		if o.Name != "" {
			spec["name"] = o.Name
		}
		if o.Unique {
			spec["unique"] = true
		}
		if o.PartialFilterExpression != nil {
			spec["partialFilterExpression"] = toWire(o.PartialFilterExpression)
		}
		if o.Weights != nil {
			spec["weights"] = toWire(o.Weights)
		}
		if o.DefaultLanguage != "" {
			spec["default_language"] = o.DefaultLanguage
		}
		if o.WildcardProjection != nil {
			spec["wildcardProjection"] = toWire(o.WildcardProjection)
		}
	}
	return spec
}

// CreateOne makes an index (compound, unique, partial, or text) and answers its name.
func (iv IndexView) CreateOne(ctx context.Context, model IndexModel) (string, error) {
	r, _, err := iv.c.call(ctx, "POST", iv.c.path+":createIndex", indexSpec(model))
	if err != nil {
		return "", err
	}
	name, _ := r["name"].(string)
	return name, nil
}

// CreateMany makes several indexes and answers their names.
func (iv IndexView) CreateMany(ctx context.Context, models []IndexModel) ([]string, error) {
	specs := make([]any, len(models))
	for i, m := range models {
		specs[i] = indexSpec(m)
	}
	r, _, err := iv.c.call(ctx, "POST", iv.c.path+":createIndexes", M{"indexes": specs})
	if err != nil {
		return nil, err
	}
	var names []string
	list, _ := r["names"].([]any)
	for _, n := range list {
		names = append(names, fmt.Sprint(n))
	}
	return names, nil
}

// List is a cursor of the indexes, as MongoDB gives them (v, key, name, unique…).
func (iv IndexView) List(ctx context.Context) (*Cursor, error) {
	r, _, err := iv.c.call(ctx, "POST", iv.c.path+":listIndexes", M{})
	if err != nil {
		return nil, err
	}
	var out []M
	list, _ := r["indexes"].([]any)
	for _, i := range list {
		if m, ok := fromWire(i).(M); ok {
			out = append(out, m)
		}
	}
	return &Cursor{docs: out}, nil
}

// DropOne drops the index named.
func (iv IndexView) DropOne(ctx context.Context, name string) error {
	_, _, err := iv.c.call(ctx, "POST", iv.c.path+":dropIndex", M{"index": name})
	return err
}

// DropAll drops every index but _id_.
func (iv IndexView) DropAll(ctx context.Context) error {
	_, _, err := iv.c.call(ctx, "POST", iv.c.path+":dropIndexes", M{})
	return err
}

// Watch is the collection's changes as they happen, as the driver's.
func (c *Collection) Watch(ctx context.Context, pipeline any, opts ...ChangeStreamOptions) (*ChangeStream, error) {
	return newChangeStream(ctx, c.call, c.path+":watch", pipeline, opts)
}

// Watch is the changes of every collection of the base, as the driver's.
func (d *Database) Watch(ctx context.Context, pipeline any, opts ...ChangeStreamOptions) (*ChangeStream, error) {
	return newChangeStream(ctx, d.call, d.path+":watch", pipeline, opts)
}

// Watch is the changes of the whole cluster, as the driver's.
func (c *Client) Watch(ctx context.Context, pipeline any, opts ...ChangeStreamOptions) (*ChangeStream, error) {
	return newChangeStream(ctx, c.call, c.v1+":watch", pipeline, opts)
}

// DatabaseSpecification is a base of the cluster, as the driver's ListDatabases gives it.
type DatabaseSpecification struct {
	Name       string
	SizeOnDisk int64
	Empty      bool
}

// ListDatabasesResult is the driver's: the bases and their total size.
type ListDatabasesResult struct {
	Databases []DatabaseSpecification
	TotalSize int64
}

func wholeNumber(v any) int64 {
	switch n := v.(type) {
	case json.Number:
		i, _ := n.Int64()
		return i
	case float64:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

// ListDatabases lists the cluster's bases (a key limited to some: those), as the driver's; the filter is not read.
func (c *Client) ListDatabases(ctx context.Context, _ any) (ListDatabasesResult, error) {
	answer, _, err := c.call(ctx, "GET", c.v1+"/bases", nil)
	if err != nil {
		return ListDatabasesResult{}, err
	}
	out := ListDatabasesResult{TotalSize: wholeNumber(answer["totalSize"]), Databases: []DatabaseSpecification{}}
	list, _ := answer["databases"].([]any)
	for _, d := range list {
		m, _ := d.(map[string]any)
		empty, _ := m["empty"].(bool)
		out.Databases = append(out.Databases, DatabaseSpecification{Name: fmt.Sprint(m["name"]), SizeOnDisk: wholeNumber(m["sizeOnDisk"]), Empty: empty})
	}
	return out, nil
}

// ListDatabaseNames names the cluster's bases, as the driver's.
func (c *Client) ListDatabaseNames(ctx context.Context, filter any) ([]string, error) {
	r, err := c.ListDatabases(ctx, filter)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(r.Databases))
	for _, d := range r.Databases {
		names = append(names, d.Name)
	}
	return names, nil
}

// Database is a database of the cluster, as MongoDB's client.Database(name).
func (c *Client) Database(name string) *Database {
	return &Database{call: c.call, raw: c.raw, path: c.v1 + "/bases/" + url.PathEscape(name)}
}

// Database holds collections.
type Database struct {
	call caller
	raw  rawCaller
	path string
}

// Collection is one collection of the database.
func (d *Database) Collection(name string) *Collection {
	return &Collection{call: d.call, path: d.path + "/collections/" + url.PathEscape(name)}
}

// ListCollectionNames names the database's collections.
func (d *Database) ListCollectionNames(ctx context.Context) ([]string, error) {
	_, list, err := d.call(ctx, "GET", d.path+"/collections", nil)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(list))
	for _, c := range list {
		if m, ok := c.(map[string]any); ok {
			names = append(names, fmt.Sprint(m["name"]))
		}
	}
	return names, nil
}

// Collection holds documents.
type Collection struct {
	call caller
	path string
}

// FindOptions: Sort (a D, for its order), Projection, Limit (0: all), Skip.
type FindOptions struct {
	Sort       D
	Projection M
	Limit      int64
	Skip       int64
}

// UpdateOptions: Upsert.
type UpdateOptions struct{ Upsert bool }

// InsertOneResult, InsertManyResult, UpdateResult, DeleteResult: as MongoDB's.
type (
	InsertOneResult  struct{ InsertedID string }
	InsertManyResult struct{ InsertedIDs []string }
	UpdateResult     struct {
		MatchedCount, ModifiedCount int64
		UpsertedID                  string
	}
	DeleteResult struct{ DeletedCount int64 }
	// BulkWriteResult: as the driver's.
	BulkWriteResult struct {
		InsertedCount, MatchedCount, ModifiedCount, DeletedCount, UpsertedCount int64
		UpsertedIDs                                                             map[int64]any
	}
)

// WriteModel is one operation of BulkWrite, made as the driver's: NewInsertOneModel().SetDocument(doc)…
type WriteModel interface{ model() M }

type InsertOneModel struct{ Document any }
type UpdateOneModel struct {
	Filter, Update any
	Upsert         bool
}
type UpdateManyModel struct{ Filter, Update any }
type ReplaceOneModel struct {
	Filter, Replacement any
	Upsert              bool
}
type DeleteOneModel struct{ Filter any }
type DeleteManyModel struct{ Filter any }

func NewInsertOneModel() *InsertOneModel                    { return &InsertOneModel{} }
func (m *InsertOneModel) SetDocument(d any) *InsertOneModel { m.Document = d; return m }
func (m *InsertOneModel) model() M                          { return M{"insertOne": M{"document": toWire(m.Document)}} }
func NewUpdateOneModel() *UpdateOneModel                    { return &UpdateOneModel{} }
func (m *UpdateOneModel) SetFilter(f any) *UpdateOneModel   { m.Filter = f; return m }
func (m *UpdateOneModel) SetUpdate(u any) *UpdateOneModel   { m.Update = u; return m }
func (m *UpdateOneModel) SetUpsert(u bool) *UpdateOneModel  { m.Upsert = u; return m }
func (m *UpdateOneModel) model() M {
	return M{"updateOne": M{"filter": filterOf(m.Filter), "update": toWire(m.Update), "upsert": m.Upsert}}
}
func NewUpdateManyModel() *UpdateManyModel                  { return &UpdateManyModel{} }
func (m *UpdateManyModel) SetFilter(f any) *UpdateManyModel { m.Filter = f; return m }
func (m *UpdateManyModel) SetUpdate(u any) *UpdateManyModel { m.Update = u; return m }
func (m *UpdateManyModel) model() M {
	return M{"updateMany": M{"filter": filterOf(m.Filter), "update": toWire(m.Update)}}
}
func NewReplaceOneModel() *ReplaceOneModel                       { return &ReplaceOneModel{} }
func (m *ReplaceOneModel) SetFilter(f any) *ReplaceOneModel      { m.Filter = f; return m }
func (m *ReplaceOneModel) SetReplacement(r any) *ReplaceOneModel { m.Replacement = r; return m }
func (m *ReplaceOneModel) SetUpsert(u bool) *ReplaceOneModel     { m.Upsert = u; return m }
func (m *ReplaceOneModel) model() M {
	return M{"replaceOne": M{"filter": filterOf(m.Filter), "replacement": toWire(m.Replacement), "upsert": m.Upsert}}
}
func NewDeleteOneModel() *DeleteOneModel                    { return &DeleteOneModel{} }
func (m *DeleteOneModel) SetFilter(f any) *DeleteOneModel   { m.Filter = f; return m }
func (m *DeleteOneModel) model() M                          { return M{"deleteOne": M{"filter": filterOf(m.Filter)}} }
func NewDeleteManyModel() *DeleteManyModel                  { return &DeleteManyModel{} }
func (m *DeleteManyModel) SetFilter(f any) *DeleteManyModel { m.Filter = f; return m }
func (m *DeleteManyModel) model() M                         { return M{"deleteMany": M{"filter": filterOf(m.Filter)}} }

// BulkWriteOptions: Ordered (true unless set false: then it goes on past a refusal).
type BulkWriteOptions struct{ Ordered *bool }

// Cursor holds the documents a Find read; All copies them, as MongoDB's cursor.All.
type Cursor struct{ docs []M }

// All puts the documents into results (a *[]M).
func (c *Cursor) All(_ context.Context, results *[]M) error {
	*results = c.docs
	return nil
}

func filterOf(filter any) any {
	if filter == nil {
		return M{}
	}
	return toWire(filter)
}

func num(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		n, _ := x.Int64()
		return n
	case float64:
		return int64(math.Round(x))
	}
	return 0
}

// Find reads the documents a filter finds, 1000 at a time.
func (c *Collection) Find(ctx context.Context, filter any, opts ...FindOptions) (*Cursor, error) {
	var o FindOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	var out []M
	token := ""
	for {
		want := int64(1000)
		if o.Limit > 0 {
			want = min(1000, o.Limit-int64(len(out)))
		}
		body := map[string]any{"filter": filterOf(filter), "limit": want}
		// The next page starts after the last one read (its token), never at an offset that moves with writes.
		if token != "" {
			body["pageToken"] = token
		} else {
			body["skip"] = o.Skip
		}
		if len(o.Sort) > 0 {
			body["sort"] = o.Sort
		}
		if len(o.Projection) > 0 {
			body["projection"] = toWire(o.Projection)
		}
		page, _, err := c.call(ctx, "POST", c.path+":find", body)
		if err != nil {
			return nil, err
		}
		docs, _ := page["documents"].([]any)
		for _, d := range docs {
			if m, ok := fromWire(d).(M); ok {
				out = append(out, m)
			}
		}
		token, _ = page["nextPageToken"].(string)
		if token == "" || (o.Limit > 0 && int64(len(out)) >= o.Limit) {
			return &Cursor{docs: out}, nil
		}
		if len(out) >= MaxDocuments {
			return nil, &Error{Code: "resource-exhausted", Message: fmt.Sprintf("more than %d documents: narrow the filter", MaxDocuments)}
		}
	}
}

// FindOne reads the first document a filter finds, or nil.
func (c *Collection) FindOne(ctx context.Context, filter any, opts ...FindOptions) (M, error) {
	o := FindOptions{}
	if len(opts) > 0 {
		o = opts[0]
	}
	o.Limit = 1
	cur, err := c.Find(ctx, filter, o)
	if err != nil || len(cur.docs) == 0 {
		return nil, err
	}
	return cur.docs[0], nil
}

// Aggregate runs a pipeline on the server and reads every result, 1000 at a time (as the driver's Aggregate).
func (c *Collection) Aggregate(ctx context.Context, pipeline any) (*Cursor, error) {
	var out []M
	for skip := 0; ; skip += 1000 {
		page, _, err := c.call(ctx, "POST", c.path+":aggregate", map[string]any{"pipeline": toWire(pipeline), "limit": 1000, "skip": skip})
		if err != nil {
			return nil, err
		}
		docs, _ := page["documents"].([]any)
		for _, d := range docs {
			if m, ok := fromWire(d).(M); ok {
				out = append(out, m)
			}
		}
		if more, _ := page["more"].(bool); !more {
			return &Cursor{docs: out}, nil
		}
		if len(out) >= MaxDocuments {
			return nil, &Error{Code: "resource-exhausted", Message: fmt.Sprintf("more than %d documents: add $limit", MaxDocuments)}
		}
	}
}

// BulkWrite runs the operations in order, as the driver's BulkWrite; a refusal is an *Error.
func (c *Collection) BulkWrite(ctx context.Context, models []WriteModel, opts ...BulkWriteOptions) (*BulkWriteResult, error) {
	ordered := true
	if len(opts) > 0 && opts[0].Ordered != nil {
		ordered = *opts[0].Ordered
	}
	ops := make([]any, len(models))
	for i, m := range models {
		ops[i] = m.model()
	}
	r, _, err := c.call(ctx, "POST", c.path+":bulkWrite", map[string]any{"operations": ops, "ordered": ordered})
	if err != nil {
		return nil, err
	}
	out := &BulkWriteResult{InsertedCount: num(r["insertedCount"]), MatchedCount: num(r["matchedCount"]), ModifiedCount: num(r["modifiedCount"]), DeletedCount: num(r["deletedCount"]), UpsertedCount: num(r["upsertedCount"]), UpsertedIDs: map[int64]any{}}
	var ids map[string]any
	switch v := r["upsertedIds"].(type) {
	case M:
		ids = v
	case map[string]any:
		ids = v
	}
	for k, v := range ids {
		if i, err := strconv.ParseInt(k, 10, 64); err == nil {
			out.UpsertedIDs[i] = v
		}
	}
	return out, nil
}

// EstimatedDocumentCount answers the collection's number of documents, from the statistics (at once on a big one).
func (c *Collection) EstimatedDocumentCount(ctx context.Context) (int64, error) {
	r, _, err := c.call(ctx, "POST", c.path+":estimatedDocumentCount", map[string]any{})
	if err != nil {
		return 0, err
	}
	return num(r["count"]), nil
}

// Distinct answers the different values of a field among the documents a filter finds (a list's items one by one).
func (c *Collection) Distinct(ctx context.Context, fieldName string, filter any) ([]any, error) {
	r, _, err := c.call(ctx, "POST", c.path+":distinct", map[string]any{"key": fieldName, "filter": filterOf(filter)})
	if err != nil {
		return nil, err
	}
	values, _ := r["values"].([]any)
	out := make([]any, 0, len(values))
	for _, v := range values {
		out = append(out, fromWire(v))
	}
	return out, nil
}

// CountDocuments counts the documents a filter finds.
func (c *Collection) CountDocuments(ctx context.Context, filter any) (int64, error) {
	r, _, err := c.call(ctx, "POST", c.path+":countDocuments", map[string]any{"filter": filterOf(filter)})
	if err != nil {
		return 0, err
	}
	return num(r["count"]), nil
}

// InsertOne adds a document; its "_id" (a string) is kept when given.
func (c *Collection) InsertOne(ctx context.Context, document any) (*InsertOneResult, error) {
	r, _, err := c.call(ctx, "POST", c.path+":insertOne", map[string]any{"document": toWire(document)})
	if err != nil {
		return nil, err
	}
	return &InsertOneResult{InsertedID: fmt.Sprint(r["insertedId"])}, nil
}

// InsertMany adds documents, 1000 per request.
func (c *Collection) InsertMany(ctx context.Context, documents []any) (*InsertManyResult, error) {
	res := &InsertManyResult{}
	for start := 0; start < len(documents); start += 1000 {
		end := min(start+1000, len(documents))
		r, _, err := c.call(ctx, "POST", c.path+":insertMany", map[string]any{"documents": toWire(documents[start:end])})
		if err != nil {
			return res, err
		}
		ids, _ := r["insertedIds"].(map[string]any)
		for i := 0; i < len(ids); i++ {
			res.InsertedIDs = append(res.InsertedIDs, fmt.Sprint(ids[strconv.Itoa(i)]))
		}
	}
	return res, nil
}

func (c *Collection) change(ctx context.Context, method string, filter any, key string, value any, upsert *bool) (*UpdateResult, error) {
	body := map[string]any{"filter": filterOf(filter), key: toWire(value)}
	if upsert != nil {
		body["upsert"] = *upsert
	}
	r, _, err := c.call(ctx, "POST", c.path+":"+method, body)
	if err != nil {
		return nil, err
	}
	res := &UpdateResult{MatchedCount: num(r["matchedCount"]), ModifiedCount: num(r["modifiedCount"])}
	if id, ok := r["upsertedId"].(string); ok {
		res.UpsertedID = id
	}
	return res, nil
}

// UpdateOne changes the first document a filter finds ($set, $unset, $inc, $currentDate; Upsert).
func (c *Collection) UpdateOne(ctx context.Context, filter, update any, opts ...UpdateOptions) (*UpdateResult, error) {
	upsert := len(opts) > 0 && opts[0].Upsert
	return c.change(ctx, "updateOne", filter, "update", update, &upsert)
}

// UpdateMany changes every document a filter finds.
func (c *Collection) UpdateMany(ctx context.Context, filter, update any) (*UpdateResult, error) {
	return c.change(ctx, "updateMany", filter, "update", update, nil)
}

// ReplaceOne replaces the first document a filter finds.
func (c *Collection) ReplaceOne(ctx context.Context, filter, replacement any, opts ...UpdateOptions) (*UpdateResult, error) {
	upsert := len(opts) > 0 && opts[0].Upsert
	return c.change(ctx, "replaceOne", filter, "replacement", replacement, &upsert)
}

func (c *Collection) remove(ctx context.Context, method string, filter any) (*DeleteResult, error) {
	r, _, err := c.call(ctx, "POST", c.path+":"+method, map[string]any{"filter": filterOf(filter)})
	if err != nil {
		return nil, err
	}
	return &DeleteResult{DeletedCount: num(r["deletedCount"])}, nil
}

// DeleteOne deletes the first document a filter finds.
func (c *Collection) DeleteOne(ctx context.Context, filter any) (*DeleteResult, error) {
	return c.remove(ctx, "deleteOne", filter)
}

// DeleteMany deletes every document a filter finds.
func (c *Collection) DeleteMany(ctx context.Context, filter any) (*DeleteResult, error) {
	return c.remove(ctx, "deleteMany", filter)
}

// ---- views and commands, as the driver's: CreateView, CreateCollection, Drop, RunCommand ----

// SingleResult is a command's reply, as the driver's: Decode it (into an *M, or a struct through JSON), or Err.
type SingleResult struct {
	doc M
	err error
}

// Decode puts the reply into v.
func (r *SingleResult) Decode(v any) error {
	if r.err != nil {
		return r.err
	}
	if p, ok := v.(*M); ok {
		*p = r.doc
		return nil
	}
	b, err := json.Marshal(r.doc)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// Err is the command's error (an *Error), or nil.
func (r *SingleResult) Err() error { return r.err }

// RunCommand runs a command of the database, as the driver's: create, collMod (a view's viewOn and pipeline), drop,
// explain (D{{"explain", D{{"find", "patients"}, {"filter", M{…}}}}, {"verbosity", "executionStats"}}).
func (d *Database) RunCommand(ctx context.Context, command any) *SingleResult {
	page, _, err := d.call(ctx, "POST", d.path+":runCommand", toWire(command))
	if err != nil {
		return &SingleResult{err: err}
	}
	doc, _ := fromWire(map[string]any(page)).(M)
	return &SingleResult{doc: doc}
}

// CreateView makes a view, as the driver's: read only, computed when read from viewOn through the pipeline.
func (d *Database) CreateView(ctx context.Context, viewName, viewOn string, pipeline any) error {
	_, _, err := d.call(ctx, "POST", d.path+"/collections/"+url.PathEscape(viewName)+":create", map[string]any{"viewOn": viewOn, "pipeline": toWire(pipeline)})
	return err
}

// CreateCollection makes an empty collection, as the driver's.
func (d *Database) CreateCollection(ctx context.Context, name string, opts ...CreateCollectionOptions) error {
	body := M{}
	if len(opts) > 0 {
		o := opts[0]
		if o.Validator != nil {
			body["validator"] = toWire(o.Validator)
		}
		if o.ValidationLevel != "" {
			body["validationLevel"] = o.ValidationLevel
		}
		if o.ValidationAction != "" {
			body["validationAction"] = o.ValidationAction
		}
	}
	_, _, err := d.call(ctx, "POST", d.path+"/collections/"+url.PathEscape(name)+":create", body)
	return err
}

// CreateCollectionOptions are the driver's (options.CreateCollection()): Validator (M{"$jsonSchema": …}),
// ValidationLevel ("strict", "moderate", "off") and ValidationAction ("error", "warn").
type CreateCollectionOptions struct {
	Validator        any
	ValidationLevel  string
	ValidationAction string
}

// CollectionSpecification is a collection or a view, as the driver's ListCollectionSpecifications gives them.
type CollectionSpecification struct {
	Name     string
	Type     string // "collection" or "view"
	ReadOnly bool
	Options  M // a view's viewOn and pipeline
}

// ListCollectionSpecifications lists the database's collections and views, as the driver's.
func (d *Database) ListCollectionSpecifications(ctx context.Context, _ any) ([]CollectionSpecification, error) {
	_, list, err := d.call(ctx, "GET", d.path+"/collections", nil)
	if err != nil {
		return nil, err
	}
	out := make([]CollectionSpecification, 0, len(list))
	for _, c := range list {
		m, _ := c.(map[string]any)
		spec := CollectionSpecification{Name: fmt.Sprint(m["name"]), Type: fmt.Sprint(m["type"]), Options: M{}}
		if spec.Type == "view" {
			spec.ReadOnly = true
			spec.Options = M{"viewOn": m["viewOn"], "pipeline": fromWire(m["pipeline"])}
		}
		out = append(out, spec)
	}
	return out, nil
}

// Drop drops the collection and its documents, or the view, as the driver's.
func (c *Collection) Drop(ctx context.Context) error {
	_, _, err := c.call(ctx, "POST", c.path+":drop", M{})
	return err
}
