# IshedDB's files from Ruby, as the mongo gem's Mongo::Grid::FSBucket (database.fs): upload_from_stream,
# open_upload_stream, download_to_stream(_by_name), open_download_stream(_by_name), find, delete, rename, drop — the
# same names and arguments. The bytes go to IshedDB in pieces (a resumable upload, as Cloud Storage's) and come back
# as a stream; a file's document is one of the collection <bucket>.files. IshedDB's own: signed_url for a browser.

require "stringio"

module Ished
  class FSBucket
    PIECE = 8 * 1024 * 1024 # one request (a multiple of IshedDB's 256 KB segments)

    attr_reader :bucket_name

    # A file being written, as the gem's upload stream: write, then close (or the block's end); its file_id.
    class UploadStream
      attr_reader :file_id

      def initialize(bucket, filename, options)
        @bucket = bucket
        @filename = filename
        @options = options
        @buffer = StringIO.new("".b)
        @closed = false
      end

      def write(data)
        raise Error.new("invalid-argument", "this upload stream is closed") if @closed

        @buffer.write(data.b)
        self
      end

      def close
        return @file_id if @closed

        @buffer.rewind
        @file_id = @bucket.send(:upload, @filename, @buffer, @options)
        @closed = true
        @file_id
      end

      def closed? = @closed
    end

    def initialize(database, call, raw, path, bucket_name: "fs")
      raise Error.new("invalid-argument", 'bucket_name: 1 to 94 letters, digits, "_", "-" or "."') unless bucket_name.to_s.match?(/\A[A-Za-z0-9_-][A-Za-z0-9_.-]{0,93}\z/)

      @database = database
      @call = call
      @raw = raw
      @bucket_name = bucket_name.to_s
      @path = "#{path}/buckets/#{Ished.segment(@bucket_name)}"
    end

    # ---- writing ----------------------------------------------------------------------------------------------------

    # As the gem's upload_from_stream(filename, io, metadata:, file_id:): the file's id.
    def upload_from_stream(filename, io, options = {}) = upload(filename, io.is_a?(String) ? StringIO.new(io.b) : io, options)

    # As the gem's open_upload_stream(filename) { |stream| stream.write(...) }: the stream (closed after the block).
    def open_upload_stream(filename, options = {})
      stream = UploadStream.new(self, filename, options)
      return stream unless block_given?

      yield stream
      stream.close
      stream
    end

    # ---- reading ----------------------------------------------------------------------------------------------------

    # As the gem's download_to_stream(id, io): the bytes written to io as they come.
    def download_to_stream(file_id, io)
      document(file_id)
      @raw.call(:get, "#{@path}/files/#{Ished.segment(file_id.to_s)}:download", nil, {}, auth: true) { |chunk| io.write(chunk) }
      io
    end

    # As the gem's open_download_stream(id) { |stream| stream.read }: a stream of the file's bytes.
    def open_download_stream(file_id)
      io = download_to_stream(file_id, StringIO.new("".b))
      io.rewind
      block_given? ? yield(io) : io
    end

    # As the gem: revision 0 the first one sent, -1 the last one (default).
    def download_to_stream_by_name(filename, io, revision: -1) = download_to_stream(by_name(filename, revision), io)

    def open_download_stream_by_name(filename, revision: -1, &block) = open_download_stream(by_name(filename, revision), &block)

    # ---- the files' documents -----------------------------------------------------------------------------------------

    # As the gem's find(selector): a view of the files' documents (the collection <bucket>.files).
    def find(selector = {}, options = {}) = @database.collection("#{@bucket_name}.files").find(selector, options)

    def find_one(selector = {}) = find(selector).limit(1).first

    # As the gem's delete(id): the file's document and its bytes.
    def delete(file_id) = @call.call(:post, "#{@path}/files/#{Ished.segment(file_id.to_s)}:delete", {}) && nil

    def rename(file_id, new_filename) = @call.call(:post, "#{@path}/files/#{Ished.segment(file_id.to_s)}:rename", { "filename" => new_filename }) && nil

    # As the gem's drop: every file of the bucket, and its <bucket>.files collection.
    def drop = @call.call(:post, "#{@path}:drop", {}) && nil

    # IshedDB's own: a signed address for a browser (no key in it), as Cloud Storage's signed URLs: url, expiresAt.
    def signed_url(file_id, expires_in: 3600, disposition: "attachment") = @call.call(:post, "#{@path}/files/#{Ished.segment(file_id.to_s)}:signUrl", { "expiresIn" => expires_in, "disposition" => disposition })

    private

    def document(file_id)
      find_one({ _id: file_id }) || raise(Error.new("not-found", "no file #{file_id} in the bucket #{@bucket_name}", 404))
    end

    def by_name(filename, revision)
      view = find({ filename: filename }).sort(uploadDate: revision >= 0 ? 1 : -1).skip(revision >= 0 ? revision : -revision - 1).limit(1)
      (view.first || raise(Error.new("not-found", "no revision #{revision} of the file #{filename} in the bucket #{@bucket_name}", 404)))["_id"]
    end

    # Sends io in pieces of 8 MB; the length told with the last one.
    def upload(filename, io, options)
      body = { "filename" => filename, "length" => nil }
      body["id"] = options[:file_id].to_s if options[:file_id]
      body["metadata"] = Ished.to_wire(options[:metadata]) if options[:metadata]
      body["contentType"] = options[:content_type] if options[:content_type]
      piece = io.read(PIECE) || "".b
      body["length"] = 0 if piece.empty?
      started = @call.call(:post, "#{@path}/uploads", body)
      return started["id"] if started.key?("file")

      at = 0
      loop do
        following = io.read(PIECE)
        last = following.nil? || following.empty?
        total = last ? (at + piece.bytesize).to_s : "*"
        @raw.call(:put, started["uploadUrl"], piece, { "Content-Range" => "bytes #{at}-#{at + piece.bytesize - 1}/#{total}", "Content-Type" => "application/octet-stream" }, auth: false)
        at += piece.bytesize
        return started["id"] if last

        piece = following
      end
    end
  end
end
