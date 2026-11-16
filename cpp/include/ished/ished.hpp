// IshedDB for C++, written as MongoDB's C++ driver (mongocxx) is:
//   ished::client client{url, key};
//   auto clients = client["boutique"]["clients"];
//   for (auto& doc : clients.find({{"ville", "Dakar"}}, ished::options::find{}.sort({{"age", -1}}))) { … }
// Header-only, on libcurl and nlohmann/json (documents are nlohmann::json). The key never travels but over HTTPS (or
// to this machine). Dates: ished::date(time_point) and ished::to_time(value); integers beyond 2**53 and binary values
// are kept as IshedDB keeps them.
#pragma once

#include <curl/curl.h>

#include <chrono>
#include <cstdint>
#include <ctime>
#include <deque>
#include <functional>
#include <iomanip>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace ished {

using json = nlohmann::json;
using document = nlohmann::json;
inline constexpr std::size_t max_documents = 100000;

/// A refusal from IshedDB: its code (invalid-argument, not-found…) and message.
class error : public std::runtime_error {
 public:
  error(std::string code, std::string message, long status = 0)
      : std::runtime_error(code + ": " + message), code_(std::move(code)), message_(std::move(message)), status_(status) {}
  const std::string& code() const { return code_; }
  const std::string& message() const { return message_; }
  long status() const { return status_; }

 private:
  std::string code_, message_;
  long status_;
};

/// A date as IshedDB keeps it ({"~date": "…Z"}), from a time point.
inline json date(std::chrono::system_clock::time_point t) {
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
  std::time_t s = static_cast<std::time_t>(ms / 1000);
  std::tm tm{};
  gmtime_r(&s, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << (ms % 1000) << 'Z';
  return json{{"~date", out.str()}};
}

/// The time point of a date value ({"~date": …}).
inline std::chrono::system_clock::time_point to_time(const json& value) {
  const std::string text = value.at("~date").get<std::string>();
  std::tm tm{};
  std::istringstream in(text.substr(0, 19));
  in >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
  if (in.fail()) throw error("invalid-argument", "not a date: " + text);
  auto seconds = std::chrono::system_clock::from_time_t(timegm(&tm));
  int ms = text.size() >= 23 ? std::stoi(text.substr(20, 3)) : 0;
  return seconds + std::chrono::milliseconds(ms);
}

inline bool is_date(const json& value) { return value.is_object() && value.size() == 1 && value.contains("~date"); }

namespace detail {
inline constexpr std::int64_t safe_int = (std::int64_t{1} << 53) - 1;

inline std::string base64(const std::vector<std::uint8_t>& bytes) {
  static const char* t = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  std::size_t i = 0;
  for (; i + 2 < bytes.size(); i += 3) {
    std::uint32_t n = (bytes[i] << 16) | (bytes[i + 1] << 8) | bytes[i + 2];
    out += {t[(n >> 18) & 63], t[(n >> 12) & 63], t[(n >> 6) & 63], t[n & 63]};
  }
  if (i + 1 == bytes.size()) {
    std::uint32_t n = bytes[i] << 16;
    out += {t[(n >> 18) & 63], t[(n >> 12) & 63], '=', '='};
  } else if (i + 2 == bytes.size()) {
    std::uint32_t n = (bytes[i] << 16) | (bytes[i + 1] << 8);
    out += {t[(n >> 18) & 63], t[(n >> 12) & 63], t[(n >> 6) & 63], '='};
  }
  return out;
}

inline json to_wire(const json& v) {
  if (v.is_binary()) return json{{"~bytes", base64(v.get_binary())}};
  if (v.is_number_integer()) {
    auto n = v.get<std::int64_t>();
    if (n > safe_int || n < -safe_int) return json{{"~int", std::to_string(n)}};
    return v;
  }
  if (v.is_object()) {
    json out = json::object();
    for (auto& [k, x] : v.items()) out[k] = to_wire(x);
    return out;
  }
  if (v.is_array()) {
    json out = json::array();
    for (auto& x : v) out.push_back(to_wire(x));
    return out;
  }
  return v;
}

inline json from_wire(const json& v) {
  if (v.is_object()) {
    if (v.size() == 1 && v.contains("~int")) return std::stoll(v.at("~int").get<std::string>());
    json out = json::object();
    for (auto& [k, x] : v.items()) out[k] = from_wire(x);
    return out;
  }
  if (v.is_array()) {
    json out = json::array();
    for (auto& x : v) out.push_back(from_wire(x));
    return out;
  }
  return v;
}

inline std::size_t collect(char* data, std::size_t size, std::size_t n, void* out) {
  static_cast<std::string*>(out)->append(data, size * n);
  return size * n;
}

inline std::string escape(const std::string& name) {
  std::ostringstream out;
  for (unsigned char c : name) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') out << c;
    else out << '%' << std::uppercase << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
  }
  return out.str();
}

/// The connection: one curl handle per call, the key in the header, errors made into ished::error.
struct link {
  std::string root, key;
  long timeout = 30;

  json call(const std::string& method, const std::string& path, const json* body, const std::string* transaction = nullptr) const {
    static std::once_flag once;
    std::call_once(once, [] { curl_global_init(CURL_GLOBAL_DEFAULT); });
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> h(curl_easy_init(), curl_easy_cleanup);
    if (!h) throw error("unavailable", "curl could not start");
    std::string url = root + path, answer, payload;
    curl_slist* headers = curl_slist_append(nullptr, ("Authorization: Bearer " + key).c_str());
    if (transaction) headers = curl_slist_append(headers, ("X-Ished-Transaction: " + *transaction).c_str());
    curl_easy_setopt(h.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(h.get(), CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(h.get(), CURLOPT_TIMEOUT, timeout);
    curl_easy_setopt(h.get(), CURLOPT_WRITEFUNCTION, collect);
    curl_easy_setopt(h.get(), CURLOPT_WRITEDATA, &answer);
    if (body) {
      payload = body->dump();
      headers = curl_slist_append(headers, "Content-Type: application/json");
      curl_easy_setopt(h.get(), CURLOPT_POSTFIELDS, payload.c_str());
      curl_easy_setopt(h.get(), CURLOPT_POSTFIELDSIZE, static_cast<long>(payload.size()));
    }
    curl_easy_setopt(h.get(), CURLOPT_HTTPHEADER, headers);
    CURLcode rc = curl_easy_perform(h.get());
    curl_slist_free_all(headers);
    if (rc != CURLE_OK) throw error("unavailable", curl_easy_strerror(rc));
    long status = 0;
    curl_easy_getinfo(h.get(), CURLINFO_RESPONSE_CODE, &status);
    json parsed = answer.empty() ? json() : json::parse(answer);
    if (status >= 400) {
      json e = parsed.is_object() && parsed.contains("error") ? parsed["error"] : json::object();
      throw error(e.value("code", std::string("internal")), e.value("message", "HTTP " + std::to_string(status)), status);
    }
    return parsed;
  }

  /// The files' bytes (gridfs::bucket): an upload session's own address ([auth] false: the key never goes there), or a
  /// download with the key, written to [sink] as they come. Over HTTPS (or to this machine) only.
  json bytes(const std::string& method, const std::string& where, const std::string* body, const std::string& range, bool auth, std::ostream* sink) const {
    static std::once_flag once;
    std::call_once(once, [] { curl_global_init(CURL_GLOBAL_DEFAULT); });
    const std::string url = where.rfind("http://", 0) == 0 || where.rfind("https://", 0) == 0 ? where : root + where;
    const bool local = url.rfind("http://127.0.0.1", 0) == 0 || url.rfind("http://localhost", 0) == 0 || url.rfind("http://[::1]", 0) == 0;
    if (url.rfind("https://", 0) != 0 && !local) throw error("invalid-argument", "a file's address is reached over HTTPS only");
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> h(curl_easy_init(), curl_easy_cleanup);
    if (!h) throw error("unavailable", "curl could not start");
    struct out_t {
      CURL* handle;
      std::ostream* sink;
      std::string text;
    } out{h.get(), sink, {}};
    auto write = +[](char* data, std::size_t size, std::size_t n, void* user) -> std::size_t {
      auto* o = static_cast<out_t*>(user);
      long status = 0;
      curl_easy_getinfo(o->handle, CURLINFO_RESPONSE_CODE, &status);
      if (o->sink && status < 400) o->sink->write(data, static_cast<std::streamsize>(size * n));
      else o->text.append(data, size * n);
      return size * n;
    };
    curl_slist* headers = nullptr;
    if (auth) headers = curl_slist_append(headers, ("Authorization: Bearer " + key).c_str());
    if (!range.empty()) headers = curl_slist_append(headers, ("Content-Range: " + range).c_str());
    curl_easy_setopt(h.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(h.get(), CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(h.get(), CURLOPT_TIMEOUT, 6L * 3600);
    curl_easy_setopt(h.get(), CURLOPT_WRITEFUNCTION, write);
    curl_easy_setopt(h.get(), CURLOPT_WRITEDATA, &out);
    if (body) {
      headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
      curl_easy_setopt(h.get(), CURLOPT_POSTFIELDS, body->data());
      curl_easy_setopt(h.get(), CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body->size()));
    }
    curl_easy_setopt(h.get(), CURLOPT_HTTPHEADER, headers);
    const CURLcode rc = curl_easy_perform(h.get());
    curl_slist_free_all(headers);
    if (rc != CURLE_OK) throw error("unavailable", curl_easy_strerror(rc));
    long status = 0;
    curl_easy_getinfo(h.get(), CURLINFO_RESPONSE_CODE, &status);
    const json parsed = out.text.empty() ? json() : json::parse(out.text, nullptr, false);
    if (status >= 400) {
      const json e = parsed.is_object() && parsed.contains("error") ? parsed["error"] : json::object();
      throw error(e.value("code", std::string("internal")), e.value("message", "HTTP " + std::to_string(status)), status);
    }
    return parsed;
  }
};
}  // namespace detail

namespace options {
namespace gridfs {
/// As mongocxx::options::gridfs::bucket: bucket_name (by default "fs").
class bucket {
 public:
  bucket& bucket_name(std::string n) { name_ = std::move(n); return *this; }
  std::string name_ = "fs";
};
/// As mongocxx::options::gridfs::upload: metadata (and IshedDB's content_type).
class upload {
 public:
  upload& metadata(json m) { metadata_ = std::move(m); return *this; }
  upload& content_type(std::string t) { content_type_ = std::move(t); return *this; }
  std::optional<json> metadata_;
  std::string content_type_;
};
}  // namespace gridfs

/// As mongocxx::options::find: sort, projection, limit, skip.
class find {
 public:
  find& sort(json s) { sort_ = std::move(s); return *this; }
  find& projection(json p) { projection_ = std::move(p); return *this; }
  find& limit(std::int64_t n) { limit_ = n; return *this; }
  find& skip(std::int64_t n) { skip_ = n; return *this; }
  std::optional<json> sort_, projection_;
  std::int64_t limit_ = 0, skip_ = 0;
};

/// As mongocxx::options::update: upsert.
class update {
 public:
  update& upsert(bool u) { upsert_ = u; return *this; }
  bool upsert_ = false;
};
}  // namespace options

namespace result {
struct insert_one {
  std::string id;
  const std::string& inserted_id() const { return id; }
};
struct insert_many {
  std::vector<std::string> ids;
  std::size_t inserted_count() const { return ids.size(); }
  const std::vector<std::string>& inserted_ids() const { return ids; }
};
struct update {
  std::int64_t matched, modified;
  std::optional<std::string> upserted;
  std::int64_t matched_count() const { return matched; }
  std::int64_t modified_count() const { return modified; }
  const std::optional<std::string>& upserted_id() const { return upserted; }
};
struct delete_result {
  std::int64_t deleted;
  std::int64_t deleted_count() const { return deleted; }
};
struct bulk_write {
  json r;
  std::int64_t inserted_count() const { return r.at("insertedCount").get<std::int64_t>(); }
  std::int64_t matched_count() const { return r.at("matchedCount").get<std::int64_t>(); }
  std::int64_t modified_count() const { return r.at("modifiedCount").get<std::int64_t>(); }
  std::int64_t deleted_count() const { return r.at("deletedCount").get<std::int64_t>(); }
  std::int64_t upserted_count() const { return r.at("upsertedCount").get<std::int64_t>(); }
};
}  // namespace result

/// The operations of a bulk write, as mongocxx::model: insert_one{doc}, update_one{filter, update}.upsert(true)…
namespace model {
struct insert_one {
  json document;
};
struct update_one {
  json filter, update;
  bool upsert_ = false;
  update_one& upsert(bool u) { upsert_ = u; return *this; }
};
struct update_many {
  json filter, update;
};
struct replace_one {
  json filter, replacement;
  bool upsert_ = false;
  replace_one& upsert(bool u) { upsert_ = u; return *this; }
};
struct delete_one {
  json filter;
};
struct delete_many {
  json filter;
};
}  // namespace model

namespace options {
/// As mongocxx::options::bulk_write: ordered (true unless set false: then it goes on past a refusal).
class bulk_write {
 public:
  bulk_write& ordered(bool o) { ordered_ = o; return *this; }
  bool ordered_ = true;
};
/// As mongocxx::options::change_stream: full_document ("updateLookup"), resume_after or start_after (a resume token),
/// start_at_operation_time, max_await_time (10 s by default, 20 s at most), batch_size.
class change_stream {
 public:
  change_stream& full_document(std::string f) { full_document_ = std::move(f); return *this; }
  change_stream& full_document_before_change(std::string f) { full_document_before_change_ = std::move(f); return *this; }
  change_stream& resume_after(json token) { resume_after_ = std::move(token); return *this; }
  change_stream& start_after(json token) { resume_after_ = std::move(token); return *this; }
  change_stream& start_at_operation_time(std::chrono::system_clock::time_point t) { start_at_ = t; return *this; }
  change_stream& max_await_time(std::chrono::milliseconds ms) { max_await_ = ms; return *this; }
  change_stream& batch_size(std::int32_t n) { batch_size_ = n; return *this; }
  std::string full_document_;
  std::string full_document_before_change_;
  json resume_after_;
  std::optional<std::chrono::system_clock::time_point> start_at_;
  std::chrono::milliseconds max_await_{10000};
  std::int32_t batch_size_ = 0;
};
/// As mongocxx::options::index: unique, name, partial_filter_expression, weights, default_language, and a wildcard
/// index's wildcard_projection ({{"attrs", 1}} only those, {{"secret", 0}} all but those).
class index {
 public:
  index& unique(bool u) { spec_["unique"] = u; return *this; }
  index& name(std::string n) { spec_["name"] = std::move(n); return *this; }
  index& partial_filter_expression(json f) { spec_["partialFilterExpression"] = std::move(f); return *this; }
  index& weights(json w) { spec_["weights"] = std::move(w); return *this; }
  index& default_language(std::string l) { spec_["default_language"] = std::move(l); return *this; }
  index& wildcard_projection(json p) { spec_["wildcardProjection"] = std::move(p); return *this; }
  json spec_ = json::object();
};
/// As mongocxx::options::create_collection: the validator ({{"$jsonSchema", …}}), its level and its action.
class create_collection {
 public:
  create_collection& validator(json v) { spec_["validator"] = std::move(v); return *this; }
  create_collection& validation_level(std::string l) { spec_["validationLevel"] = std::move(l); return *this; }
  create_collection& validation_action(std::string a) { spec_["validationAction"] = std::move(a); return *this; }
  json spec_ = json::object();
};
/// As mongocxx::options::create_view: the view's pipeline.
class create_view {
 public:
  create_view& pipeline(json p) { pipeline_ = std::move(p); return *this; }
  json pipeline_ = json::array();
};
}  // namespace options

class client;
class database;
class collection;
namespace gridfs {
class bucket;
}

/// A change stream, as mongocxx::change_stream: for (const auto& change : stream) { … } gives the changes there,
/// waiting up to max_await_time for some, and ends when a wait brings none (loop again to go on);
/// get_resume_token(). Opened at once: the changes made from then on are seen. A collection dropped or renamed ends
/// it ("invalidate").
class change_stream {
 public:
  class iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = document;
    using difference_type = std::ptrdiff_t;
    using pointer = const document*;
    using reference = const document&;
    iterator() = default;
    explicit iterator(change_stream* s) : s_(s) {}
    reference operator*() const { return s_->current_; }
    pointer operator->() const { return &s_->current_; }
    iterator& operator++() {
      if (!s_->load()) s_ = nullptr;
      return *this;
    }
    void operator++(int) { ++*this; }
    bool operator==(const iterator& o) const { return s_ == o.s_; }
    bool operator!=(const iterator& o) const { return s_ != o.s_; }

   private:
    change_stream* s_ = nullptr;
  };

  change_stream(const change_stream&) = delete;
  change_stream& operator=(const change_stream&) = delete;
  change_stream(change_stream&&) = default;
  change_stream& operator=(change_stream&&) = default;

  iterator begin() { return load() ? iterator(this) : iterator(); }
  iterator end() { return iterator(); }

  /// The token to resume after the last change given (resume_after).
  std::optional<json> get_resume_token() const { return token_.is_null() ? std::nullopt : std::optional<json>(token_); }

 private:
  friend class collection;
  friend class database;
  friend class client;
  change_stream(std::shared_ptr<detail::link> link, std::string path, const json& pipeline, const options::change_stream& o)
      : link_(std::move(link)), path_(std::move(path)), wait_(o.max_await_.count()), position_(o.resume_after_) {
    body_ = json{{"pipeline", detail::to_wire(pipeline)}};
    if (!o.full_document_.empty()) body_["fullDocument"] = o.full_document_;
    if (!o.full_document_before_change_.empty()) body_["fullDocumentBeforeChange"] = o.full_document_before_change_;
    if (o.batch_size_ > 0) body_["batchSize"] = o.batch_size_;
    if (o.start_at_) at_ = date(*o.start_at_);
    token_ = position_;
    if (position_.is_null() && at_.is_null()) fetch(0);
  }

  void fetch(std::int64_t wait) {
    json request = body_;
    request["maxAwaitTimeMS"] = wait;
    if (!position_.is_null()) request["resumeAfter"] = position_;
    else if (!at_.is_null()) request["startAtOperationTime"] = at_;
    const json r = link_->call("POST", path_, &request);
    at_ = json();
    position_ = r.at("resumeToken");
    for (const auto& c : r.at("changes")) buffer_.push_back(detail::from_wire(c));
    if (buffer_.empty()) token_ = position_;
  }

  // The next change made current (given, so read: as mongocxx, a loop left with break goes on after it).
  bool load() {
    if (buffer_.empty() && !closed_) fetch(wait_);
    if (buffer_.empty()) return false;
    current_ = std::move(buffer_.front());
    buffer_.pop_front();
    token_ = buffer_.empty() ? position_ : current_.at("_id");
    if (current_.value("operationType", std::string()) == "invalidate") closed_ = true;
    return true;
  }

  std::shared_ptr<detail::link> link_;
  std::string path_;
  json body_;
  std::int64_t wait_;
  json position_, at_, token_;
  document current_;
  std::deque<document> buffer_;
  bool closed_ = false;
};

/// A session, as mongocxx::client_session: auto s = client.start_session();
/// s.with_transaction([&](ished::client_session* s) { coll.update_one(*s, filter, update); });
/// The server holds the transaction (60 seconds at most, 20 without a request); a conflict with another transaction
/// answers "aborted", and with_transaction runs the callback again (5 times at most).
class client_session {
 public:
  client_session(const client_session&) = delete;
  client_session& operator=(const client_session&) = delete;
  client_session& operator=(client_session&&) = delete;
  client_session(client_session&& o) noexcept : link_(std::move(o.link_)), v1_(std::move(o.v1_)), id_(std::exchange(o.id_, {})) {}
  /// An unfinished transaction is undone.
  ~client_session() { abort_quietly(); }

  bool in_transaction() const { return !id_.empty(); }

  void start_transaction() {
    if (!id_.empty()) throw error("failed-precondition", "a transaction is already started in this session");
    const json body = json::object();
    id_ = link_->call("POST", v1_ + ":beginTransaction", &body).at("transaction").get<std::string>();
  }

  void commit_transaction() {
    if (id_.empty()) throw error("failed-precondition", "no transaction started in this session");
    const json body{{"transaction", std::exchange(id_, {})}};
    link_->call("POST", v1_ + ":commit", &body);
  }

  void abort_transaction() {
    if (id_.empty() || !link_) return;
    const json body{{"transaction", std::exchange(id_, {})}};
    try {
      link_->call("POST", v1_ + ":rollback", &body);
    } catch (const error& e) {
      if (e.code() != "not-found") throw;  // not-found: already ended by the server (an error, or too long), nothing kept
    }
  }

  void with_transaction(const std::function<void(client_session*)>& callback) {
    for (int attempt = 1;; ++attempt) {
      start_transaction();
      try {
        callback(this);
        commit_transaction();
        return;
      } catch (const error& e) {
        abort_quietly();
        if (e.code() != "aborted" || attempt >= 5) throw;
      } catch (...) {
        abort_quietly();
        throw;
      }
    }
  }

  /// The transaction a request runs in (none: nullptr).
  const std::string* transaction() const { return id_.empty() ? nullptr : &id_; }

 private:
  friend class client;
  client_session(std::shared_ptr<detail::link> link, std::string v1) : link_(std::move(link)), v1_(std::move(v1)) {}
  void abort_quietly() noexcept {
    try {
      abort_transaction();
    } catch (...) {
      // the first error is the one given back
    }
  }

  std::shared_ptr<detail::link> link_;
  std::string v1_, id_;
};

class collection {
 public:
  collection(std::shared_ptr<detail::link> link, std::string path, const client_session* session = nullptr)
      : link_(std::move(link)), path_(std::move(path)), session_(session) {}

  /// As mongocxx's watch: the collection's changes as they happen.
  change_stream watch(const json& pipeline = json::array(), const options::change_stream& o = {}) const { return change_stream(link_, path_ + ":watch", pipeline, o); }

  /// As mongocxx's create_index: keys as an object ({{"titre", "text"}}) or, for several fields in their order (a
  /// JSON object sorts its keys), as pairs: json::array({{"a", 1}, {"b", -1}}). Answers { "name": … }.
  document create_index(const json& keys, const options::index& o = {}) const {
    json spec = o.spec_;
    spec["key"] = keys;
    return json{{"name", call("createIndex", spec).at("name")}};
  }
  /// The indexes, as MongoDB gives them (v, key, name, unique…).
  std::vector<document> list_indexes() const {
    std::vector<document> out;
    const json answer = call("listIndexes", json::object());  // kept: a loop over a temporary's member would dangle
    for (const auto& i : answer.at("indexes")) out.push_back(detail::from_wire(i));
    return out;
  }
  void drop_index(const std::string& name) const { call("dropIndex", json{{"index", name}}); }
  void drop_indexes() const { call("dropIndexes", json::object()); }
  /// As mongocxx's drop: the collection and its documents, or the view.
  void drop() const { call("drop", json::object()); }

  // The overloads of mongocxx, session first: the request runs in the session's transaction.
  std::vector<document> find(const client_session& s, const json& filter = json::object(), const options::find& o = {}) const { return in(s).find(filter, o); }
  std::optional<document> find_one(const client_session& s, const json& filter = json::object(), options::find o = {}) const { return in(s).find_one(filter, o); }
  std::int64_t count_documents(const client_session& s, const json& filter = json::object()) const { return in(s).count_documents(filter); }
  std::vector<document> aggregate(const client_session& s, const json& pipeline) const { return in(s).aggregate(pipeline); }
  json distinct(const client_session& s, const std::string& name, const json& filter = json::object()) const { return in(s).distinct(name, filter); }
  std::optional<result::insert_one> insert_one(const client_session& s, const json& doc) const { return in(s).insert_one(doc); }
  std::optional<result::insert_many> insert_many(const client_session& s, const std::vector<json>& docs) const { return in(s).insert_many(docs); }
  std::optional<result::update> update_one(const client_session& s, const json& filter, const json& update, const options::update& o = {}) const { return in(s).update_one(filter, update, o); }
  std::optional<result::update> update_many(const client_session& s, const json& filter, const json& update) const { return in(s).update_many(filter, update); }
  std::optional<result::update> replace_one(const client_session& s, const json& filter, const json& replacement, const options::update& o = {}) const { return in(s).replace_one(filter, replacement, o); }
  std::optional<result::delete_result> delete_one(const client_session& s, const json& filter = json::object()) const { return in(s).delete_one(filter); }
  std::optional<result::delete_result> delete_many(const client_session& s, const json& filter = json::object()) const { return in(s).delete_many(filter); }

  /// The documents a filter finds, read 1000 at a time.
  std::vector<document> find(const json& filter = json::object(), const options::find& o = {}) const {
    std::vector<document> out;
    std::string token;
    for (;;) {
      std::int64_t want = o.limit_ > 0 ? std::min<std::int64_t>(1000, o.limit_ - static_cast<std::int64_t>(out.size())) : 1000;
      json body{{"filter", detail::to_wire(filter.is_null() ? json::object() : filter)}, {"limit", want}};
      // The next page starts after the last one read (its token), never at an offset that moves with writes.
      if (!token.empty()) body["pageToken"] = token; else body["skip"] = o.skip_;
      if (o.sort_) body["sort"] = *o.sort_;
      if (o.projection_) body["projection"] = *o.projection_;
      json page = call("find", body);
      for (auto& d : page.at("documents")) out.push_back(detail::from_wire(d));
      token = page.contains("nextPageToken") ? page.at("nextPageToken").get<std::string>() : std::string();
      if (token.empty() || (o.limit_ > 0 && static_cast<std::int64_t>(out.size()) >= o.limit_)) return out;
      if (out.size() >= max_documents) throw error("resource-exhausted", "more than 100000 documents: narrow the filter");
    }
  }

  std::optional<document> find_one(const json& filter = json::object(), options::find o = {}) const {
    auto found = find(filter, o.limit(1));
    if (found.empty()) return std::nullopt;
    return found.front();
  }

  std::int64_t count_documents(const json& filter = json::object()) const {
    return call("countDocuments", json{{"filter", detail::to_wire(filter)}}).at("count").get<std::int64_t>();
  }

  /// As mongocxx's aggregate: the pipeline run by the server; every result, read 1000 at a time.
  std::vector<document> aggregate(const json& pipeline) const {
    std::vector<document> out;
    for (std::int64_t skip = 0;; skip += 1000) {
      const json page = call("aggregate", json{{"pipeline", detail::to_wire(pipeline)}, {"limit", 1000}, {"skip", skip}});
      for (const auto& d : page.at("documents")) out.push_back(detail::from_wire(d));
      if (!page.at("more").get<bool>()) return out;
      if (out.size() >= max_documents) throw error("resource-exhausted", "more than 100000 documents: add $limit");
    }
  }

  class bulk_write;
  /// As mongocxx's create_bulk_write: append the operations, then execute().
  bulk_write create_bulk_write(const options::bulk_write& o = {}) const;
  bulk_write create_bulk_write(const client_session& s, const options::bulk_write& o = {}) const;

  /// The collection's number of documents, from the statistics (at once on a big collection).
  std::int64_t estimated_document_count() const { return call("estimatedDocumentCount", json::object()).at("count").get<std::int64_t>(); }

  /// The different values of a field among the documents found (a list's items one by one), as a JSON array.
  json distinct(const std::string& name, const json& filter = json::object()) const {
    // The answer kept first: a range-for over a temporary's member would read it after its end (before C++23).
    const json answer = call("distinct", json{{"key", name}, {"filter", detail::to_wire(filter)}});
    json out = json::array();
    for (const auto& v : answer.at("values")) out.push_back(detail::from_wire(v));
    return out;
  }

  std::optional<result::insert_one> insert_one(const json& doc) const {
    return result::insert_one{call("insertOne", json{{"document", detail::to_wire(doc)}}).at("insertedId").get<std::string>()};
  }

  std::optional<result::insert_many> insert_many(const std::vector<json>& docs) const {
    result::insert_many r;
    for (std::size_t start = 0; start < docs.size(); start += 1000) {
      json chunk = json::array();
      for (std::size_t i = start; i < std::min(docs.size(), start + 1000); i++) chunk.push_back(detail::to_wire(docs[i]));
      json ids = call("insertMany", json{{"documents", chunk}}).at("insertedIds");
      for (std::size_t i = 0; i < chunk.size(); i++) r.ids.push_back(ids.at(std::to_string(i)).get<std::string>());
    }
    return r;
  }

  std::optional<result::update> update_one(const json& filter, const json& update, const options::update& o = {}) const {
    return change("updateOne", filter, "update", update, o.upsert_);
  }
  std::optional<result::update> update_many(const json& filter, const json& update) const {
    return change("updateMany", filter, "update", update, std::nullopt);
  }
  std::optional<result::update> replace_one(const json& filter, const json& replacement, const options::update& o = {}) const {
    return change("replaceOne", filter, "replacement", replacement, o.upsert_);
  }
  std::optional<result::delete_result> delete_one(const json& filter = json::object()) const {
    return result::delete_result{call("deleteOne", json{{"filter", detail::to_wire(filter)}}).at("deletedCount").get<std::int64_t>()};
  }
  std::optional<result::delete_result> delete_many(const json& filter = json::object()) const {
    return result::delete_result{call("deleteMany", json{{"filter", detail::to_wire(filter)}}).at("deletedCount").get<std::int64_t>()};
  }

 private:
  json call(const std::string& method, const json& body) const {
    return link_->call("POST", path_ + ":" + method, &body, session_ ? session_->transaction() : nullptr);
  }
  collection in(const client_session& s) const { return collection(link_, path_, &s); }

  std::optional<result::update> change(const std::string& method, const json& filter, const std::string& key, const json& value, std::optional<bool> upsert) const {
    json body{{"filter", detail::to_wire(filter)}, {key, detail::to_wire(value)}};
    if (upsert) body["upsert"] = *upsert;
    json r = call(method, body);
    std::optional<std::string> up;
    if (r.contains("upsertedId") && r["upsertedId"].is_string()) up = r["upsertedId"].get<std::string>();
    return result::update{r.at("matchedCount").get<std::int64_t>(), r.at("modifiedCount").get<std::int64_t>(), up};
  }

  std::shared_ptr<detail::link> link_;
  std::string path_;
  const client_session* session_;
};

class database {
 public:
  database(std::shared_ptr<detail::link> link, std::string path) : link_(std::move(link)), path_(std::move(path)) {}
  collection operator[](const std::string& name) const { return collection(link_, path_ + "/collections/" + detail::escape(name)); }
  collection get_collection(const std::string& name) const { return (*this)[name]; }
  /// As mongocxx's watch: the changes of every collection of the base.
  change_stream watch(const json& pipeline = json::array(), const options::change_stream& o = {}) const { return change_stream(link_, path_ + ":watch", pipeline, o); }

  /// As mongocxx's create_view(name, view_on, options::create_view().pipeline(…)): read only, computed when read.
  collection create_view(const std::string& name, const std::string& view_on, const options::create_view& o = {}) const {
    const json body{{"viewOn", view_on}, {"pipeline", detail::to_wire(o.pipeline_)}};
    link_->call("POST", path_ + "/collections/" + detail::escape(name) + ":create", &body);
    return (*this)[name];
  }
  /// As mongocxx's create_collection(name): an empty collection.
  collection create_collection(const std::string& name, const options::create_collection& o = {}) const {
    const json body = detail::to_wire(o.spec_);
    link_->call("POST", path_ + "/collections/" + detail::escape(name) + ":create", &body);
    return (*this)[name];
  }
  /// As mongocxx's run_command: create, collMod (a view's viewOn and pipeline), drop, explain.
  document run_command(const json& command) const {
    const json body = detail::to_wire(command);
    return detail::from_wire(link_->call("POST", path_ + ":runCommand", &body));
  }
  /// As mongocxx's list_collections: name, type ("collection" or "view"), options, info.
  std::vector<document> list_collections() const {
    std::vector<document> out;
    const json answer = link_->call("GET", path_ + "/collections", nullptr);
    for (const auto& c : answer) {
      const bool view = c.at("type") == "view";
      out.push_back(json{{"name", c.at("name")}, {"type", c.at("type")}, {"options", view ? json{{"viewOn", c.at("viewOn")}, {"pipeline", c.at("pipeline")}} : json::object()}, {"info", {{"readOnly", view}}}});
    }
    return out;
  }
  /// The names of the collections and views.
  std::vector<std::string> list_collection_names() const {
    std::vector<std::string> out;
    for (const auto& c : list_collections()) out.push_back(c.at("name").get<std::string>());
    return out;
  }
  /// As mongocxx's gridfs_bucket(options): the files of a bucket, as GridFS.
  gridfs::bucket gridfs_bucket(const options::gridfs::bucket& o = {}) const;

 private:
  std::shared_ptr<detail::link> link_;
  std::string path_;
};

/// IshedDB's files from C++, as mongocxx's gridfs::bucket: upload_from_stream(_with_id), open_upload_stream (an
/// uploader), download_to_stream, open_download_stream (a downloader), find, delete_file; and rename, drop, by name
/// (0 the first revision, -1 the last), signed_url (IshedDB's own). The bytes go in pieces of 8 MB (a resumable upload,
/// as Cloud Storage's) to the session's own address, and come back written to a stream as they come.
namespace gridfs {
/// As mongocxx's result::gridfs::upload: id().
class upload_result {
 public:
  explicit upload_result(std::string id) : id_(std::move(id)) {}
  const std::string& id() const { return id_; }

 private:
  std::string id_;
};

class uploader;
class downloader;

class bucket {
 public:
  bucket(std::shared_ptr<detail::link> link, const std::string& db_path, const options::gridfs::bucket& o) : link_(std::move(link)), name_(o.name_) {
    static const std::regex ok("^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,93}$");
    if (!std::regex_match(name_, ok)) throw error("invalid-argument", "bucket_name: 1 to 94 letters, digits, \"_\", \"-\" or \".\"");
    db_path_ = db_path;
    path_ = db_path + "/buckets/" + detail::escape(name_);
  }
  const std::string& bucket_name() const { return name_; }

  /// As mongocxx: the file's id (made by IshedDB).
  upload_result upload_from_stream(const std::string& filename, std::istream* source, const options::gridfs::upload& o = {}) const { return upload_result(send(nullptr, filename, source, o)); }
  /// As mongocxx: the file kept under the id given.
  void upload_from_stream_with_id(const std::string& id, const std::string& filename, std::istream* source, const options::gridfs::upload& o = {}) const { send(&id, filename, source, o); }
  inline uploader open_upload_stream(const std::string& filename, const options::gridfs::upload& o = {}) const;

  /// As mongocxx: the bytes written to [destination] as they come.
  void download_to_stream(const std::string& id, std::ostream* destination) const {
    document_of(id);
    link_->bytes("GET", file_path(id, "download"), nullptr, "", true, destination);
  }
  inline downloader open_download_stream(const std::string& id) const;
  void download_to_stream_by_name(const std::string& filename, std::ostream* destination, int revision = -1) const { download_to_stream(by_name(filename, revision), destination); }

  /// As mongocxx: the files' documents (the collection <bucket>.files).
  std::vector<document> find(const json& filter = json::object(), const options::find& o = {}) const { return files().find(filter, o); }
  /// As mongocxx's delete_file: the file's document and its bytes.
  void delete_file(const std::string& id) const {
    const json body = json::object();
    link_->call("POST", file_path(id, "delete"), &body);
  }
  void rename(const std::string& id, const std::string& new_filename) const {
    const json body{{"filename", new_filename}};
    link_->call("POST", file_path(id, "rename"), &body);
  }
  /// Every file of the bucket, and its <bucket>.files collection.
  void drop() const {
    const json body = json::object();
    link_->call("POST", path_ + ":drop", &body);
  }
  /// IshedDB's own: a signed address for a browser (no key in it), as Cloud Storage's signed URLs.
  std::string signed_url(const std::string& id, int expires_in = 3600) const {
    const json body{{"expiresIn", expires_in}, {"disposition", "attachment"}};
    return link_->call("POST", file_path(id, "signUrl"), &body).at("url").get<std::string>();
  }

  document document_of(const std::string& id) const {
    auto found = files().find(json{{"_id", id}}, options::find{}.limit(1));
    if (found.empty()) throw error("not-found", "no file " + id + " in the bucket " + name_, 404);
    return found.front();
  }

 private:
  collection files() const { return collection(link_, db_path_ + "/collections/" + detail::escape(name_ + ".files")); }
  std::string file_path(const std::string& id, const std::string& action) const { return path_ + "/files/" + detail::escape(id) + ":" + action; }
  std::string by_name(const std::string& filename, int revision) const {
    auto found = files().find(json{{"filename", filename}}, options::find{}.sort(json{{"uploadDate", revision >= 0 ? 1 : -1}}).skip(revision >= 0 ? revision : -revision - 1).limit(1));
    if (found.empty()) throw error("not-found", "no revision " + std::to_string(revision) + " of the file " + filename + " in the bucket " + name_, 404);
    return found.front().at("_id").get<std::string>();
  }
  static std::string piece(std::istream* in) {
    std::string out(8 * 1024 * 1024, '\0');
    in->read(out.data(), static_cast<std::streamsize>(out.size()));
    out.resize(static_cast<std::size_t>(in->gcount()));
    return out;
  }
  /// Sends [source] in pieces of 8 MB, the length told with the last one; answers the file's id.
  std::string send(const std::string* id, const std::string& filename, std::istream* source, const options::gridfs::upload& o) const {
    json body{{"filename", filename}, {"length", nullptr}};
    if (id) body["id"] = *id;
    if (o.metadata_) body["metadata"] = detail::to_wire(*o.metadata_);
    if (!o.content_type_.empty()) body["contentType"] = o.content_type_;
    std::string current = piece(source);
    if (current.empty()) body["length"] = 0;
    const json started = link_->call("POST", path_ + "/uploads", &body);
    const std::string file_id = started.at("id").get<std::string>();
    if (started.contains("file")) return file_id;
    const std::string session = started.at("uploadUrl").get<std::string>();
    std::size_t at = 0;
    while (true) {
      std::string following = piece(source);
      const bool last = following.empty();
      const std::string total = last ? std::to_string(at + current.size()) : "*";
      link_->bytes("PUT", session, &current, "bytes " + std::to_string(at) + "-" + std::to_string(at + current.size() - 1) + "/" + total, false, nullptr);
      at += current.size();
      if (last) return file_id;
      current = std::move(following);
    }
  }

  std::shared_ptr<detail::link> link_;
  std::string name_, db_path_, path_;
};

/// As mongocxx's gridfs::uploader: write(bytes, length), then close() (sent then): the file's id.
class uploader {
 public:
  uploader(const bucket& b, std::string filename, options::gridfs::upload o) : bucket_(b), filename_(std::move(filename)), options_(std::move(o)) {}
  void write(const std::uint8_t* bytes, std::size_t length) { buffer_.write(reinterpret_cast<const char*>(bytes), static_cast<std::streamsize>(length)); }
  upload_result close() { return bucket_.upload_from_stream(filename_, &buffer_, options_); }

 private:
  bucket bucket_;
  std::string filename_;
  options::gridfs::upload options_;
  std::stringstream buffer_;
};

/// As mongocxx's gridfs::downloader: read(buffer, length) (0 at the end), file_length(), files_document().
class downloader {
 public:
  downloader(const bucket& b, const std::string& id) : document_(b.document_of(id)) { b.download_to_stream(id, &bytes_); }
  std::size_t read(std::uint8_t* buffer, std::size_t length) {
    bytes_.read(reinterpret_cast<char*>(buffer), static_cast<std::streamsize>(length));
    return static_cast<std::size_t>(bytes_.gcount());
  }
  std::int64_t file_length() const { return document_.at("length").get<std::int64_t>(); }
  const document& files_document() const { return document_; }

 private:
  document document_;
  std::stringstream bytes_;
};

inline uploader bucket::open_upload_stream(const std::string& filename, const options::gridfs::upload& o) const { return uploader(*this, filename, o); }
inline downloader bucket::open_download_stream(const std::string& id) const { return downloader(*this, id); }
}  // namespace gridfs

inline gridfs::bucket database::gridfs_bucket(const options::gridfs::bucket& o) const { return gridfs::bucket(link_, path_, o); }

/// A client for one project (its key), in one of its clusters (empty: the project's first, "Cluster0").
class client {
 public:
  client(const std::string& url, const std::string& key, const std::string& cluster = "") {
    static const std::regex address(R"(^(https?)://(\[[^\]]+\]|[^/:]+))");
    std::smatch m;
    if (!std::regex_search(url, m, address)) throw error("invalid-argument", "a valid address is needed");
    std::string host = m[2];
    bool local = host == "127.0.0.1" || host == "localhost" || host == "[::1]";
    if (m[1] != "https" && !(m[1] == "http" && local)) throw error("invalid-argument", "IshedDB is reached over HTTPS (plain HTTP only to this machine)");
    if (key.rfind("ished_sk_", 0) != 0) throw error("invalid-argument", "a key ished_sk_… is needed");
    if (!cluster.empty() && !std::regex_match(cluster, std::regex("^[A-Za-z0-9][A-Za-z0-9-]{0,63}$"))) throw error("invalid-argument", "cluster: 1 to 64 letters, digits or \"-\"");
    std::string root = url;
    while (!root.empty() && root.back() == '/') root.pop_back();
    link_ = std::make_shared<detail::link>(detail::link{root, key});
    v1_ = cluster.empty() ? "/v1" : "/v1/clusters/" + cluster;
  }
  database operator[](const std::string& name) const { return database(link_, v1_ + "/bases/" + detail::escape(name)); }
  database database_named(const std::string& name) const { return (*this)[name]; }
  /// As mongocxx's watch: the changes of the whole cluster.
  change_stream watch(const json& pipeline = json::array(), const options::change_stream& o = {}) const { return change_stream(link_, v1_ + ":watch", pipeline, o); }
  /// A session, as mongocxx's: its transactions are held by the server.
  client_session start_session() const { return client_session(link_, v1_); }
  /// As mongocxx's list_databases: each base's name, sizeOnDisk and empty (a key limited to some: those).
  std::vector<document> list_databases() const {
    std::vector<document> out;
    const json answer = link_->call("GET", v1_ + "/bases", nullptr);
    for (const auto& d : answer.at("databases")) out.push_back(d);
    return out;
  }
  /// As mongocxx's list_database_names.
  std::vector<std::string> list_database_names() const {
    std::vector<std::string> out;
    for (const auto& d : list_databases()) out.push_back(d.at("name").get<std::string>());
    return out;
  }

 private:
  std::shared_ptr<detail::link> link_;
  std::string v1_;
};

/// A bulk write being built, as mongocxx::bulk_write.
class collection::bulk_write {
 public:
  bulk_write(const collection& c, bool ordered) : c_(c), ordered_(ordered) {}
  bulk_write& append(const model::insert_one& m) { return add({{"insertOne", {{"document", detail::to_wire(m.document)}}}}); }
  bulk_write& append(const model::update_one& m) { return add({{"updateOne", {{"filter", detail::to_wire(m.filter)}, {"update", detail::to_wire(m.update)}, {"upsert", m.upsert_}}}}); }
  bulk_write& append(const model::update_many& m) { return add({{"updateMany", {{"filter", detail::to_wire(m.filter)}, {"update", detail::to_wire(m.update)}}}}); }
  bulk_write& append(const model::replace_one& m) { return add({{"replaceOne", {{"filter", detail::to_wire(m.filter)}, {"replacement", detail::to_wire(m.replacement)}, {"upsert", m.upsert_}}}}); }
  bulk_write& append(const model::delete_one& m) { return add({{"deleteOne", {{"filter", detail::to_wire(m.filter)}}}}); }
  bulk_write& append(const model::delete_many& m) { return add({{"deleteMany", {{"filter", detail::to_wire(m.filter)}}}}); }
  std::optional<result::bulk_write> execute() const { return result::bulk_write{c_.call("bulkWrite", json{{"operations", ops_}, {"ordered", ordered_}})}; }

 private:
  bulk_write& add(json op) {
    ops_.push_back(std::move(op));
    return *this;
  }
  collection c_;
  bool ordered_;
  json ops_ = json::array();
};

inline collection::bulk_write collection::create_bulk_write(const options::bulk_write& o) const { return bulk_write(*this, o.ordered_); }
inline collection::bulk_write collection::create_bulk_write(const client_session& s, const options::bulk_write& o) const { return bulk_write(in(s), o.ordered_); }

}  // namespace ished
