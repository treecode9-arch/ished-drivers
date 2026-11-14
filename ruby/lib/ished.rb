# IshedDB for Ruby, written as the mongo gem is: client.use("clinique")[:patients].find(ville: "Dakar").sort(age: -1).to_a
# Only Ruby's standard library. The key never travels but over HTTPS (or to this machine). Values: Time <-> dates,
# Integer beyond 2**53 <-> big integers, binary Strings (ASCII-8BIT) <-> bytes.
require "json"
require "net/http"
require "time"
require "uri"

module Ished
  MAX_DOCUMENTS = 100_000
  SAFE_INT = (2**53) - 1

  class Error < StandardError
    attr_reader :code, :status
    def initialize(code, message, status = nil)
      super("#{code}: #{message}")
      @code = code
      @status = status
    end
  end

  InsertOneResult = Struct.new(:inserted_id)
  InsertManyResult = Struct.new(:inserted_ids)
  UpdateResult = Struct.new(:matched_count, :modified_count, :upserted_id)
  DeleteResult = Struct.new(:deleted_count)
  BulkWriteResult = Struct.new(:inserted_count, :matched_count, :modified_count, :deleted_count, :upserted_count, :upserted_ids)

  def self.to_wire(value)
    case value
    when Time, DateTime then { "~date" => value.to_time.utc.iso8601(3) }
    when Integer then value.abs <= SAFE_INT ? value : { "~int" => value.to_s }
    when String then value.encoding == Encoding::BINARY ? { "~bytes" => [value].pack("m0") } : value
    when Symbol then value.to_s
    when Hash then value.to_h { |k, v| [k.to_s, to_wire(v)] }
    when Array then value.map { |v| to_wire(v) }
    when Float, true, false, nil then value
    else raise Error.new("invalid-argument", "a value of class #{value.class} cannot be kept")
    end
  end

  def self.from_wire(value)
    case value
    when Array then value.map { |v| from_wire(v) }
    when Hash
      if value.size == 1
        k, v = value.first
        return Time.iso8601(v) if k == "~date"
        return Integer(v) if k == "~int"
        return v.unpack1("m0") if k == "~bytes"
      end
      value.to_h { |k, v| [k, from_wire(v)] }
    else value
    end
  end

  # As the mongo gem's view: .sort, .limit, .skip, .projection, then .to_a, .each or .first. Read 1000 at a time.
  class View
    include Enumerable

    def initialize(call, path, filter, options = {})
      @call = call
      @path = path
      @filter = filter || {}
      @options = options
    end

    def sort(spec) = with(sort: spec)
    def limit(n) = with(limit: n)
    def skip(n) = with(skip: n)
    def projection(spec) = with(projection: spec)

    def each(&block)
      to_a.each(&block)
    end

    def first
      limit(1).to_a.first
    end

    # As the mongo gem's view.explain(verbosity: …): the find's plan (:query_planner, :execution_stats or
    # :all_plans_execution, the default), in MongoDB's shape.
    def explain(verbosity: nil)
      find = { "find" => URI.decode_www_form_component(@path.split("/").last), "filter" => Ished.to_wire(@filter) }
      find["sort"] = Ished.to_wire(@options[:sort]) if @options[:sort]
      find["projection"] = Ished.to_wire(@options[:projection]) if @options[:projection]
      find["skip"] = @options[:skip] if @options[:skip].to_i.positive?
      find["limit"] = @options[:limit] if @options[:limit].to_i.positive?
      body = { "explain" => find }
      body["verbosity"] = verbosity.to_s.gsub(/_(\w)/) { Regexp.last_match(1).upcase } if verbosity
      @call.call(:post, "#{@path}:explain", body)
    end

    def to_a
      limit = @options[:limit].to_i
      out = []
      token = nil
      loop do
        want = limit.positive? ? [1000, limit - out.size].min : 1000
        body = { "filter" => Ished.to_wire(@filter), "limit" => want }
        # The next page starts after the last one read (its token), never at an offset that moves with writes.
        if token then body["pageToken"] = token else body["skip"] = @options[:skip].to_i end
        body["sort"] = Ished.to_wire(@options[:sort]) if @options[:sort]
        body["projection"] = Ished.to_wire(@options[:projection]) if @options[:projection]
        page = @call.call(:post, "#{@path}:find", body)
        out.concat(page["documents"].map { |d| Ished.from_wire(d) })
        token = page["nextPageToken"]
        return out if token.nil? || (limit.positive? && out.size >= limit)
        raise Error.new("resource-exhausted", "more than #{MAX_DOCUMENTS} documents: narrow the filter") if out.size >= MAX_DOCUMENTS
      end
    end

    private

    def with(change) = View.new(@call, @path, @filter, @options.merge(change))
  end

  class Collection
    def initialize(call, path, options = {})
      @call = call
      @path = path
      @options = options
    end

    # As the mongo gem's collection.create: an empty collection, or a view when made with view_on: and pipeline:
    # (client[:name, view_on: "source", pipeline: […]].create).
    # Its validator too, as the gem's: client[:name, validator: { "$jsonSchema" => … }, validation_level: "moderate",
    # validation_action: "warn"].create.
    def create
      body = {}
      body["viewOn"] = @options[:view_on].to_s if @options[:view_on]
      body["pipeline"] = Ished.to_wire(@options[:pipeline]) if @options[:pipeline]
      body["validator"] = Ished.to_wire(@options[:validator]) if @options[:validator]
      body["validationLevel"] = @options[:validation_level].to_s if @options[:validation_level]
      body["validationAction"] = @options[:validation_action].to_s if @options[:validation_action]
      @call.call(:post, "#{@path}:create", body)
      self
    end

    # As the gem's collection.drop: the collection and its documents, or the view.
    def drop = @call.call(:post, "#{@path}:drop", {})["ok"] == 1

    def find(filter = {}, options = {}) = View.new(@call, @path, filter, options)

    def count_documents(filter = {})
      @call.call(:post, "#{@path}:countDocuments", { "filter" => Ished.to_wire(filter) })["count"]
    end

    # As the mongo gem's aggregate: the pipeline run by the server; every result, read 1000 at a time.
    def aggregate(pipeline, _options = {})
      out = []
      skip = 0
      loop do
        page = @call.call(:post, "#{@path}:aggregate", { "pipeline" => Ished.to_wire(pipeline), "limit" => 1000, "skip" => skip })
        out.concat(page["documents"].map { |d| Ished.from_wire(d) })
        return out unless page["more"]
        raise Error.new("resource-exhausted", "more than #{MAX_DOCUMENTS} documents: add $limit") if out.size >= MAX_DOCUMENTS
        skip += 1000
      end
    end

    # As the mongo gem's bulk_write: [{ insert_one: doc }, { update_one: { filter:, update:, upsert: } }, { update_many: … },
    # { replace_one: { filter:, replacement: } }, { delete_one: { filter: } }, { delete_many: … }]; ordered: false goes on.
    def bulk_write(requests, options = {})
      ops = requests.map do |request|
        name, args = request.first
        camel = name.to_s.gsub(/_(\w)/) { Regexp.last_match(1).upcase }
        { camel => (camel == "insertOne" ? { "document" => args } : args.transform_keys(&:to_s)) }
      end
      r = @call.call(:post, "#{@path}:bulkWrite", { "operations" => Ished.to_wire(ops), "ordered" => options.fetch(:ordered, true) })
      BulkWriteResult.new(r["insertedCount"], r["matchedCount"], r["modifiedCount"], r["deletedCount"], r["upsertedCount"], r["upsertedIds"].transform_keys(&:to_i))
    end

    # The collection's number of documents, from the statistics (at once on a big collection).
    def estimated_document_count
      @call.call(:post, "#{@path}:estimatedDocumentCount", {})["count"]
    end

    # The different values of a field among the documents found (a list's items one by one).
    def distinct(field_name, filter = {})
      @call.call(:post, "#{@path}:distinct", { "key" => field_name.to_s, "filter" => Ished.to_wire(filter) })["values"].map { |v| Ished.from_wire(v) }
    end

    def insert_one(document)
      InsertOneResult.new(@call.call(:post, "#{@path}:insertOne", { "document" => Ished.to_wire(document) })["insertedId"])
    end

    def insert_many(documents)
      ids = []
      documents.each_slice(1000) do |slice|
        r = @call.call(:post, "#{@path}:insertMany", { "documents" => slice.map { |d| Ished.to_wire(d) } })
        ids.concat((0...r["insertedIds"].size).map { |i| r["insertedIds"][i.to_s] })
      end
      InsertManyResult.new(ids)
    end

    def update_one(filter, update, upsert: false) = change("updateOne", filter, "update", update, upsert)
    def update_many(filter, update) = change("updateMany", filter, "update", update, nil)
    def replace_one(filter, replacement, upsert: false) = change("replaceOne", filter, "replacement", replacement, upsert)

    def delete_one(filter = {})
      DeleteResult.new(@call.call(:post, "#{@path}:deleteOne", { "filter" => Ished.to_wire(filter) })["deletedCount"])
    end

    def delete_many(filter = {})
      DeleteResult.new(@call.call(:post, "#{@path}:deleteMany", { "filter" => Ished.to_wire(filter) })["deletedCount"])
    end

    private

    def change(method, filter, key, value, upsert)
      body = { "filter" => Ished.to_wire(filter), key => Ished.to_wire(value) }
      body["upsert"] = upsert unless upsert.nil?
      r = @call.call(:post, "#{@path}:#{method}", body)
      UpdateResult.new(r["matchedCount"], r["modifiedCount"], r["upsertedId"])
    end
  end

  # As the driver's session: client.start_session { |s| s.with_transaction { coll.update_one(f, u, session: s) } }.
  # The transaction is held by the server (60 seconds at most, 20 without a request); a conflict with another
  # transaction answers "aborted", and with_transaction runs the block again.
  class Session
    def initialize(call, v1)
      @call = call
      @v1 = v1
      @id = nil
    end

    def in_transaction? = !@id.nil?

    def start_transaction(_options = {})
      raise Error.new("failed-precondition", "a transaction is already started in this session") if @id

      @id = @call.call(:post, "#{@v1}:beginTransaction", {})["transaction"]
      nil
    end

    def bind(call)
      header = { "X-Ished-Transaction" => @id }
      ->(method, path, body) { call.call(method, path, body, header) }
    end

    def commit_transaction
      raise Error.new("failed-precondition", "no transaction started in this session") unless @id

      tx = @id
      @id = nil
      @call.call(:post, "#{@v1}:commit", { transaction: tx })
      nil
    end

    def abort_transaction
      return unless @id

      tx = @id
      @id = nil
      @call.call(:post, "#{@v1}:rollback", { transaction: tx })
      nil
    rescue Error => e
      raise unless e.code == "not-found" # already ended by the server (an error, or too long): nothing kept
    end

    def with_transaction(_options = {}, max_attempts: 5)
      attempt = 0
      begin
        attempt += 1
        start_transaction
        out = yield self
        commit_transaction
        out
      rescue Exception => e # rubocop:disable Lint/RescueException -- given back as it came
        begin
          abort_transaction
        rescue Error
          nil
        end
        retry if e.is_a?(Error) && e.code == "aborted" && attempt < max_attempts
        raise
      end
    end

    def end_session = abort_transaction
  end

  # session: as the driver's: the method (and the view it gives, read later) runs in the session's transaction.
  module InSession
    %i[find count_documents aggregate bulk_write estimated_document_count distinct insert_one insert_many
       update_one update_many replace_one delete_one delete_many].each do |name|
      define_method(name) do |*args, **kw, &block|
        session = kw.delete(:session) if kw[:session].is_a?(Session)
        last = args.last
        if session.nil? && last.is_a?(Hash) && last[:session].is_a?(Session)
          session = last[:session]
          args = [*args[0...-1], last.except(:session)]
        end
        return super(*args, **kw, &block) unless session&.in_transaction?

        Collection.new(session.bind(@call), @path).public_send(name, *args, **kw, &block)
      end
    end
  end
  Collection.prepend(InSession)

  # As the driver's change stream: stream = coll.watch(pipeline, full_document: "updateLookup"); stream.each { |change| … }
  # (blocks, waiting for changes), try_next, resume_token, close. Opened at once: the changes made from then on are
  # seen. Each request waits up to max_await_time_ms (10 s by default, 20 s at most) on the server. A collection
  # dropped or renamed ends its stream ("invalidate").
  class ChangeStream
    include Enumerable

    def initialize(call, path, pipeline, options)
      @call = call
      @path = path
      @body = { pipeline: Ished.to_wire(pipeline || []) }
      @body[:fullDocument] = options[:full_document] if options[:full_document]
      @body[:fullDocumentBeforeChange] = options[:full_document_before_change] if options[:full_document_before_change]
      @body[:batchSize] = options[:batch_size] if options[:batch_size]
      @wait = options[:max_await_time_ms] || 10_000
      @position = options[:resume_after] || options[:start_after]
      @at = options[:start_at_operation_time]
      @token = @position
      @buffer = []
      @closed = false
      fetch(0) if @position.nil? && @at.nil?
    end

    # The token to resume after the last change given (resume_after:).
    def resume_token = @token
    def closed? = @closed

    # Each change as it comes, waiting for it, until the stream is closed.
    def each
      return to_enum(:each) unless block_given?

      loop do
        fetch(@wait) while @buffer.empty? && !@closed
        break if @buffer.empty?

        yield take
      end
    end

    # The next change if one comes within a second (or max_await_time_ms, if shorter), else nil.
    def try_next
      fetch([@wait, 1000].min) if @buffer.empty? && !@closed
      @buffer.empty? ? nil : take
    end

    def close
      @closed = true
      @buffer = []
      nil
    end

    private

    def fetch(wait)
      body = @body.merge(maxAwaitTimeMS: wait)
      if @position then body[:resumeAfter] = @position
      elsif @at then body[:startAtOperationTime] = Ished.to_wire(@at)
      end
      r = @call.call(:post, @path, body)
      @at = nil
      @position = r["resumeToken"]
      @buffer.concat(r["changes"].map { |c| Ished.from_wire(c) })
      @token = r["resumeToken"] if @buffer.empty?
    end

    def take
      change = @buffer.shift
      @token = @buffer.empty? ? @position : change["_id"]
      @closed = true if change["operationType"] == "invalidate"
      change
    end
  end

  # As the driver's index view: collection.indexes.create_one({ a: 1, b: -1 }, unique: true); create_many, each
  # (Enumerable), drop_one, drop_all. A text index: create_one({ titre: "text" }, default_language: "french").
  class IndexView
    include Enumerable

    OPTIONS = { unique: :unique, name: :name, partial_filter_expression: :partialFilterExpression, weights: :weights,
                default_language: :default_language, expire_after: :expireAfterSeconds,
                wildcard_projection: :wildcardProjection }.freeze

    def initialize(call, path)
      @call = call
      @path = path
    end

    def create_one(keys, options = {}) = @call.call(:post, "#{@path}:createIndex", Ished.to_wire(spec(keys, options)))["name"]

    def create_many(models)
      specs = models.map { |m| spec(m[:key], m.reject { |k, _| k == :key }) }
      @call.call(:post, "#{@path}:createIndexes", Ished.to_wire({ indexes: specs }))["names"]
    end

    def each(&block) = @call.call(:post, "#{@path}:listIndexes", {})["indexes"].map { |i| Ished.from_wire(i) }.each(&block)

    def drop_one(name) = @call.call(:post, "#{@path}:dropIndex", { index: name }) && nil
    def drop_all = @call.call(:post, "#{@path}:dropIndexes", {}) && nil

    private

    def spec(keys, options)
      out = { key: keys.map { |k, v| [k.to_s, v] } }
      options.each do |k, v|
        name = OPTIONS[k.to_sym] or raise Error.new("invalid-argument", "index option #{k} is not offered")
        out[name] = v
      end
      out
    end
  end

  class Collection
    # As the driver's collection.indexes: create_one, create_many, each, drop_one, drop_all.
    def indexes = IndexView.new(@call, @path)

    # As the driver's collection.watch: the collection's changes as they happen.
    def watch(pipeline = [], options = {}) = ChangeStream.new(@call, "#{@path}:watch", pipeline, options)
  end

  class Database
    def initialize(call, path, raw = nil)
      @call = call
      @path = path
      @raw = raw
    end

    # As the gem's database.fs(bucket_name:): the files of a bucket, as GridFS (ished/grid.rb).
    def fs(bucket_name: "fs") = FSBucket.new(self, @call, @raw, @path, bucket_name: bucket_name)

    # As the driver's database.watch: the changes of every collection of the base.
    def watch(pipeline = [], options = {}) = ChangeStream.new(@call, "#{@path}:watch", pipeline, options)

    def collection(name, options = {}) = Collection.new(@call, "#{@path}/collections/#{URI.encode_www_form_component(name.to_s).gsub('+', '%20')}", options)
    alias [] collection

    def collection_names = @call.call(:get, "#{@path}/collections", nil).map { |c| c["name"] }

    # As the gem's database.list_collections: name, type ("collection" or "view"), options, info.
    def list_collections
      @call.call(:get, "#{@path}/collections", nil).map do |c|
        view = c["type"] == "view"
        { "name" => c["name"], "type" => c["type"], "options" => view ? { "viewOn" => c["viewOn"], "pipeline" => c["pipeline"] } : {}, "info" => { "readOnly" => view } }
      end
    end

    # As the gem's database.command: create, collMod (a view's viewOn and pipeline), drop, explain. Answers the
    # reply's document.
    def command(document) = Ished.from_wire(@call.call(:post, "#{@path}:runCommand", Ished.to_wire(document)))
  end

  class Client
    def initialize(call, v1, raw = nil)
      @call = call
      @v1 = v1
      @raw = raw
    end

    def use(name) = Database.new(@call, "#{@v1}/bases/#{URI.encode_www_form_component(name.to_s).gsub('+', '%20')}", @raw)
    alias database use
    alias db use

    # As the driver's client.watch: the changes of the whole cluster.
    def watch(pipeline = [], options = {}) = ChangeStream.new(@call, "#{@v1}:watch", pipeline, options)

    # As the driver's client.list_databases: each base's name, sizeOnDisk and empty (a key limited to some: those).
    def list_databases = @call.call(:get, "#{@v1}/bases", nil)["databases"]

    # As the driver's client.database_names.
    def database_names = list_databases.map { |d| d["name"] }
    alias list_database_names database_names

    def start_session
      session = Session.new(@call, @v1)
      return session unless block_given?

      begin
        yield session
      ensure
        session.end_session
      end
    end
  end

  # A client for one project (its key), in one of its clusters (by default the first, "Cluster0").
  def self.connect(url, key, cluster: nil, timeout: 30)
    uri = URI(url)
    local = %w[127.0.0.1 localhost ::1].include?(uri.host)
    raise Error.new("invalid-argument", "IshedDB is reached over HTTPS (plain HTTP only to this machine)") unless uri.scheme == "https" || (uri.scheme == "http" && local)
    raise Error.new("invalid-argument", "a key ished_sk_… is needed") unless key.is_a?(String) && key.start_with?("ished_sk_")
    raise Error.new("invalid-argument", 'cluster: 1 to 64 letters, digits or "-"') if cluster && cluster !~ /\A[A-Za-z0-9][A-Za-z0-9-]{0,63}\z/
    root = url.chomp("/")
    v1 = cluster ? "/v1/clusters/#{cluster}" : "/v1"
    call = lambda do |method, path, body, extra = {}|
      target = URI(root + path)
      request = method == :get ? Net::HTTP::Get.new(target) : Net::HTTP::Post.new(target)
      request["Authorization"] = "Bearer #{key}"
      extra.each { |name, value| request[name] = value }
      if body
        request["Content-Type"] = "application/json"
        request.body = JSON.generate(body)
      end
      response = Net::HTTP.start(target.host, target.port, use_ssl: target.scheme == "https", open_timeout: timeout, read_timeout: timeout) { |h| h.request(request) }
      text = response.body.to_s.force_encoding("UTF-8")
      unless response.is_a?(Net::HTTPSuccess)
        err = (JSON.parse(text)["error"] rescue nil) || {}
        raise Error.new(err["code"] || "internal", err["message"] || "HTTP #{response.code}", response.code.to_i)
      end
      text.empty? ? nil : JSON.parse(text)
    end
    # The files' bytes (ished/grid.rb): an upload session's own address (no key sent to it), or a download with the
    # key, its bytes given to the block as they come.
    raw = lambda do |method, where, data, extra, auth: true, &block|
      target = URI(where.start_with?("http://", "https://") ? where : root + where)
      raise Error.new("invalid-argument", "a file's address is reached over HTTPS only") unless target.scheme == "https" || (target.scheme == "http" && %w[127.0.0.1 localhost ::1].include?(target.host))

      request = method == :put ? Net::HTTP::Put.new(target) : Net::HTTP::Get.new(target)
      request["Authorization"] = "Bearer #{key}" if auth
      extra.each { |name, value| request[name] = value }
      request.body = data if data
      Net::HTTP.start(target.host, target.port, use_ssl: target.scheme == "https", open_timeout: timeout, read_timeout: timeout) do |h|
        h.request(request) do |response|
          unless response.is_a?(Net::HTTPSuccess)
            text = response.read_body.to_s.force_encoding("UTF-8")
            err = (JSON.parse(text)["error"] rescue nil) || {}
            raise Error.new(err["code"] || "internal", err["message"] || "HTTP #{response.code}", response.code.to_i)
          end
          block ? response.read_body { |chunk| block.call(chunk) } : response.read_body
        end
      end
    end
    Client.new(call, v1, raw)
  end

  # A path's segment, as the addresses want it.
  def self.segment(text) = URI.encode_www_form_component(text.to_s).gsub("+", "%20")
end

require_relative "ished/grid"
