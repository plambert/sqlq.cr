require "db"
require "sqlite3"
require "string_scanner"

module Queue
  VERSION            = "0.1.0"
  DEFAULT_QUEUE_NAME = ENV["SQLQ_QUEUE_NAME"]? || "default_queue"
  DEFAULT_QUEUE_FILE = ENV["SQLQ_QUEUE_FILE"]? || "~/.cache/sqlq/default_queue"

  enum Command
    Help
    Add
    List
    Take
    Peek
    Run
    Delete
    Reset
    Count
    Dedup
    CopyTo
    BackupTo
    RestoreFrom
    MergeFrom
  end

  alias EntryTuple = {Int64, Time, String}

  class CLI
    property argv : Array(String)
    property dbfile : String
    property db : DB::Database
    property arguments : Array(String)
    setter id_format : String? = nil
    property cmd : Command
    property timezone : Time::Location
    property queue_name : String
    property? quiet : Bool

    def initialize(@argv = ARGV.dup)
      opts = @argv.dup
      @arguments = [] of String
      @quiet = false
      @cmd = Command::Help
      @timezone = Time::Location.local
      @queue_name = DEFAULT_QUEUE_NAME
      @dbfile = ""
      while opt = opts.shift?
        case opt
        when "--help", "-h"
          raise ArgumentError.new "no help available"
        when "--file", "-f", "--db"
          @dbfile = opts.shift? || raise ArgumentError.new "#{opt}: expected an argument"
        when "--localtime"
          @timezone = Time::Location.local
        when "--utc"
          @timezone = Time::Location::UTC
        when "--queue", "--queue-name", "-q"
          @queue_name = opts.shift? || raise ArgumentError.new "#{opt}: expected an argument"
        when "--quiet", "-Q"
          @quiet = true
        when .starts_with? '-'
          raise ArgumentError.new "#{opt}: unknown option"
        when "add"
          @cmd = Command::Add
          @arguments = opts.dup
          opts = [] of String
        when "list", "l"
          @cmd = Command::List
          @arguments = opts.dup
          opts = [] of String
        when "take", "get", "g", "t"
          @cmd = Command::Take
          @arguments = opts.dup
          opts = [] of String
        when "peek", "p"
          @cmd = Command::Peek
          @arguments = opts.dup
          opts = [] of String
        when "run", "r"
          @cmd = Command::Run
          @arguments = opts.dup
          opts = [] of String
        when "delete", "remove", "rm"
          @cmd = Command::Delete
          @arguments = opts.dup
          opts = [] of String
        when "reset"
          @cmd = Command::Reset
          @arguments = opts.dup
          opts = [] of String
        when "count", "c"
          @cmd = Command::Count
          @arguments = opts.dup
          opts = [] of String
        when "dedupe", "dedup"
          @cmd = Command::Dedup
          @arguments = opts.dup
          opts = [] of String
        when "copy-to"
          @cmd = Command::CopyTo
          @arguments = opts.dup
          opts = [] of String
        when "backup-to"
          @cmd = Command::BackupTo
          @arguments = opts.dup
          opts = [] of String
        when "restore-from"
          @cmd = Command::RestoreFrom
          @arguments = opts.dup
          opts = [] of String
        else
          if @dbfile == ""
            @dbfile = opt
          else
            raise ArgumentError.new "#{opt}: expected a command add, list, take, get, peek, or run"
          end
        end
      end
      validate_queue_name @queue_name
      @dbfile = Path[DEFAULT_QUEUE_FILE].expand(home: true).to_s if @dbfile == ""
      parent = Path[@dbfile].parent
      Dir.mkdir_p parent unless File.directory? parent
      @db = DB.open "sqlite3:#{@dbfile}?max_idle_pool_size=3&initial_pool_size=3&journal_mode=wal&busy_timeout=5000"
      create_table @queue_name
    end

    def run
      case @cmd
      in Command::Help
        raise ArgumentError.new "no help available"
      in Command::Add
        cmd_add
      in Command::List
        cmd_list
      in Command::Take
        cmd_take
      in Command::Peek
        cmd_peek
      in Command::Run
        cmd_run
      in Command::Delete
        cmd_delete
      in Command::Reset
        cmd_reset
      in Command::Count
        cmd_count
      in Command::Dedup
        cmd_dedup
      in Command::CopyTo
        cmd_copy_to
      in Command::BackupTo
        cmd_backup_to
      in Command::RestoreFrom
        cmd_restore_from
      in Command::MergeFrom
        cmd_merge_from
      end
    end

    def id_format
      @id_format ||= create_id_format
    end

    def create_id_format
      max = @db.query_one "SELECT MAX(id) FROM \"#{@queue_name}\"", as: Int64
      "%" + max.to_s.size.to_s + "d"
    end

    private def copy_queue(from : String, to : String, truncate : Bool = false) : Int64
      previous_count : Int64 = 0
      subsequent_count : Int64 = 0
      validate_queue_name from
      validate_queue_name to
      create_table to
      table_count = @db.query_one "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", from, as: Int64
      raise ArgumentError.new "#{from}: queue does not exist" if table_count == 0
      @db.transaction do |trans|
        previous_count = @db.query_one "SELECT COUNT(*) FROM \"#{to}\"", as: Int64
        trans.connection.exec "DELETE FROM \"#{to}\"" if truncate
        trans.connection.exec "INSERT OR IGNORE INTO \"#{to}\" SELECT * FROM \"#{from}\""
        trans.connection.exec "INSERT INTO \"#{to}\" (creation_time, entry) SELECT creation_time, entry FROM \"#{from}\" WHERE id NOT IN (SELECT id FROM \"#{to}\")" unless truncate
        subsequent_count = @db.query_one "SELECT COUNT(*) FROM \"#{to}\"", as: Int64
      end
      subsequent_count - previous_count
    end

    def cmd_copy_to
      destination = @arguments.shift? || raise ArgumentError.new "copy-to requires an argument with the new queue name"
      copy_queue from: @queue_name, to: destination
    end

    def cmd_backup_to
      destination = @arguments.shift? || raise ArgumentError.new "copy-to requires an argument with the new queue name"
      copy_queue from: @queue_name, to: destination, truncate: true
    end

    def cmd_restore_from
      source = @arguments.shift? || raise ArgumentError.new "copy-to requires an argument with the new queue name"
      copy_queue from: source, to: @queue_name, truncate: true
    end

    def cmd_merge_from
      source = @arguments.shift? || raise ArgumentError.new "copy-to requires an argument with the new queue name"
      copy_queue from: source, to: @queue_name
    end

    def cmd_dedup
      keep_oldest = true
      while opt = @arguments.shift?
        case opt
        when "--oldest"
          keep_oldest = true
        when "--newest"
          keep_oldest = false
        else
          raise ArgumentError.new "#{opt}: unknown option"
        end
      end
      entries = Hash(String, {Int64, String}).new # map the entry string to the id and timestamp
      ids_to_delete = Array(Int64).new
      @db.query "SELECT id, creation_time, entry FROM \"#{@queue_name}\" ORDER BY creation_time, id, entry" do |recordset|
        recordset.each do
          id = recordset.read(Int64)
          timestamp = recordset.read(String)
          entry = recordset.read(String)
          if existing = entries[entry]?
            if compare_time_and_id existing: existing, other: {id, timestamp}, older: keep_oldest
              ids_to_delete << id
            else
              ids_to_delete << existing[0]
              entries[entry] = {id, timestamp}
            end
          else
            entries[entry] = {id, timestamp}
          end
        end
      end
      if ids_to_delete.size > 0
        @db.exec "DELETE FROM \"#{@queue_name}\" WHERE id IN (#{ids_to_delete.map(&.to_s).join(", ")})"
        STDERR.puts "deleted #{ids_to_delete.size} duplicate entries" unless @quiet
      else
        STDERR.puts "no duplicate entries" unless @quiet
      end
    end

    private def compare_time_and_id(existing, other, older = true)
      if existing[1] < other[1]
        older
      elsif existing[1] == other[1] && existing[0] < other[0]
        older
      else
        !older
      end
    end

    def cmd_add
      unique = false
      new_entries = [] of String
      while arg = @arguments.shift?
        case arg
        when "--unique", "-u"
          unique = true
        when "--"
          new_entries += @arguments
          @arguments = [] of String
        when .starts_with? '-'
          raise ArgumentError.new "#{arg}: unknown argument"
        else
          new_entries << arg
        end
      end
      if new_entries.empty?
        STDERR.puts "nothing specified to add" unless @quiet
      else
        creation_time = Time.local(location: @timezone)
        new_entries.each do |entry|
          count = if unique
                    @db.query_one "SELECT COUNT(id) FROM \"#{@queue_name}\" WHERE entry=?", entry, as: Int64
                  else
                    0
                  end
          if count == 0
            @db.exec "INSERT INTO \"#{@queue_name}\" (entry, creation_time) VALUES (?, ?)", entry, creation_time
          end
        end
        if 1 == new_entries.size
          puts "1 entry added" unless @quiet
        else
          puts "#{new_entries.size} entries added" unless @quiet
        end
      end
    end

    def cmd_list
      limit : Int64? = @arguments[0]? && @arguments[0].to_i64?
      if limit
        sql = "SELECT id, creation_time, entry FROM \"#{@queue_name}\" ORDER BY creation_time, id LIMIT #{limit}"
      else
        sql = "SELECT id, creation_time, entry FROM \"#{@queue_name}\" ORDER BY creation_time, id"
      end
      entries = @db.query_all sql, as: EntryTuple.types
      # entries = Array(EntryTuple).new
      # @db.query sql do |resultset|
      #   entries << resultset.read(EntryTuple)
      # end
      print_entries entries
    end

    def cmd_take
      if deleted_entry = take?
        puts deleted_entry[2]
      else
        STDERR.puts "no entries" unless @quiet
        exit 1
      end
    end

    private def take? : EntryTuple?
      if deleted_entry = @db.query_one? "DELETE FROM \"#{@queue_name}\" RETURNING id, creation_time, entry ORDER BY creation_time, id LIMIT 1", as: EntryTuple.types
        deleted_entry
      else
        nil
      end
    end

    def cmd_peek
      limit = @arguments[0]?.try(&.to_i64?) || 1
      if entries = @db.query_all "SELECT id, creation_time, entry FROM \"#{@queue_name}\" ORDER BY creation_time, id LIMIT #{limit}", as: EntryTuple.types
        entries.each { |entry| puts entry[2] }
      else
        STDERR.puts "no entries" unless @quiet
        exit 1
      end
    end

    def cmd_run
      args = [] of String
      timeout = 1.minute
      retry_delay = 2.seconds
      quit_on_error = true
      error_queue : String? = nil
      while arg = @arguments.shift?
        case arg
        when "--timeout", "-t"
          timeout_arg = @arguments.shift? || raise ArgumentError.new "#{arg}: expected an argument"
          timeout = parse_relative_time(timeout_arg, negative: false)
        when "--ignore-error", "-E"
          quit_on_error = false
        when "--no-ignore-error", "-e"
          quit_on_error = true
        when "--error-queue"
          error_queue = @arguments.shift? || raise ArgumentError.new "#{arg}: expected an argument"
          validate_queue_name error_queue
        when "--no-error-queue"
          error_queue = nil
        else
          args << arg
        end
      end
      raise ArgumentError.new "run: expected a command and maybe arguments" if args.empty?
      command = args.shift
      insert_index = args.index(":") || args.size
      args << ":" if insert_index == args.size
      last_time = Time.monotonic
      create_table error_queue if error_queue
      while Time.monotonic - last_time <= timeout
        if entry = take?
          status = run_entry insert_index: insert_index, command: command, args: args, entry: entry
          if !status.success?
            if error_queue
              @db.exec "INSERT INTO \"#{error_queue}\" (id, creation_time, entry) VALUES (?, ?, ?)", entry[0], entry[1], entry[2]
            end
            if quit_on_error
              exit status.exit_code
            end
          end
          last_time = Time.monotonic
        else
          sleep retry_delay
        end
      end
    end

    private def run_entry(*, insert_index : Int32, command : String, args : Array(String), entry : EntryTuple)
      new_args = args.dup
      new_args[insert_index] = entry[2]
      Process.run command: command, args: new_args, shell: false, output: STDOUT, error: STDERR
    end

    def cmd_delete
      if @arguments.empty?
        STDERR.puts "no entries to delete were given" unless @quiet
        exit 1
      end
      to_delete = [] of Range(Int64, Int64) | Range(Int64, Nil) | Range(Nil, Int64) | Regex | Range(Time, Time) | Range(Nil, Time) | Range(Time, Nil)
      @arguments.each do |arg|
        case arg
        when %r{^\d+$}
          id = arg.to_i64
          to_delete << (id..id)
        when %r{^(\d+)(?:\.\.|:|-)(\d+)$}
          min = $1.to_i64
          max = $2.to_i64
          raise ArgumentError.new "start of range #{min} is more than end of range #{max}" if min > max
          to_delete << (min..max)
        when %r{^(\d+)\.\.\.(\d+)$}
          min = $1.to_i64
          max = $2.to_i64
          raise ArgumentError.new "start of exclusive range #{min} is more than end of range #{max}" if min >= max
          to_delete << (min..(max - 1))
        when %r{^-?(\d+)([sSmMhHdDwW])(?:\.\.|:)?$}
          count = $1.to_i64
          unit = $2.upcase
          span = case unit
                 when "S"
                   count.seconds
                 when "M"
                   count.minutes
                 when "H"
                   count.hours
                 when "D"
                   count.days
                 when "W"
                   count.weeks
                 else
                   raise ArgumentError.new "#{unit}: cannot parse time unit"
                 end
          to_delete << ((Time.local(location: @timezone) - span)..)
        when %r{^(\d\d\d\d-\d\d-\d\d)(?:\.\.|\s+TO\s+(\d\d\d\d-\d\d-\d\d))?$}
          min = Time.parse("%Y-%m-%d", $1, @timezone)
          if $2 && !$2.empty?
            max = Time.parse("%Y-%m-%d", $2, @timezone)
            to_delete << (min..max)
          else
            to_delete << (min..)
          end
        when %r{(?s)\A/(.*)/\z}
          to_delete << Regex.new $1
        else
          raise ArgumentError.new "#{arg}: cannot parse argument to delete"
        end
      end

      to_delete.each do |delete|
        where_clauses = [] of String
        where_args = [] of String | Int64 | Time | Nil
        case delete
        in Range(Int64, Int64), Range(Int64, Nil), Range(Nil, Int64)
          min = delete.begin
          if max = delete.end
            if delete.exclusive?
              where_clauses << "? <= id < ?"
              where_args << min
              where_args << max
            else
              where_clauses << "? <= id <= ?"
              where_args << min
              where_args << max
            end
          else
            where_clauses << "? <= id"
            where_args << min
          end
        in Range(Time, Time), Range(Time, Nil), Range(Nil, Time)
          min = delete.begin
          # min_str = min.to_s(Sqlite3::DATE_FORMAT_SUBSECOND)
          if max = delete.end
            # max_str = max.to_s(Sqlite3::DATE_FORMAT_SUBSECOND)
            if delete.exclusive?
              where_clauses << "? <= creation_time < ?"
              where_args << min
              where_args << max
            else
              where_clauses << "? <= creation_time <= ?"
              where_args << min
              where_args << max
            end
          else
            where_clauses << "? <= creation_time"
            where_args << min
          end
        in Regex
          where_clauses << "entry REGEXP ?"
          where_args << delete.to_s
        end

        sql = if ENV["SQLQ_DEBUG"]?
                "SELECT id, creation_time, entry FROM \"#{@queue_name}\" WHERE " + where_clauses.join(" AND ") + " ORDER BY creation_time, id"
              else
                "DELETE FROM \"#{@queue_name}\" WHERE " + where_clauses.join(" AND ") + " RETURNING id, creation_time, entry"
              end

        results = @db.query_all sql, args: where_args, as: EntryTuple.types
        if results.empty?
          STDERR.puts "no entries deleted" unless @quiet
        else
          print_entries results
          if 1 == results.size
            STDERR.puts "1 entry deleted" unless @quiet
          else
            STDERR.puts "#{results.size} entries deleted" unless @quiet
          end
        end
      end
    end

    def cmd_count
      count = @db.query_one "SELECT COUNT(*) FROM \"#{@queue_name}\"", as: Int64
      puts count
    end

    def cmd_reset
      count = @db.query_one "SELECT COUNT(*) FROM \"#{@queue_name}\"", as: Int64

      if count == 0
        STDERR.puts "#{@dbfile}: queue #{@queue_name} is empty" unless @quiet
        return
      end

      count_described = count == 1 ? "1 entry" : "all #{count} entries"

      if @arguments[0]? != "--yes" && @arguments[0]? != "-y"
        to_be_closed = [] of IO
        io_out = if STDOUT.tty?
                   STDOUT
                 elsif STDERR.tty?
                   STDERR
                 else
                   begin
                     to_be_closed << File.open "/dev/tty", "w"
                     to_be_closed[-1]
                   rescue e : File::NotFoundError | File::AccessDeniedError
                     STDERR
                   end
                 end
        io_in = begin
          to_be_closed << File.open "/dev/tty", "r"
          to_be_closed[-1]
        rescue e : File::NotFoundError | File::AccessDeniedError
          STDIN
        end

        agreed = if io_in.responds_to? :cooked
                   begin
                     io_in.cooked do
                       reset_prompt io_in, io_out, count_described
                     end
                   rescue e : IO::Error
                     reset_prompt io_in, io_out, count_described
                   end
                 else
                   reset_prompt io_in, io_out, count_described
                 end

        if agreed
          results = @db.query_all "DELETE FROM \"#{@queue_name}\" RETURNING id, creation_time, entry", as: EntryTuple.types

          print_entries results
        else
          STDERR.puts "Reset aborted" unless @quiet
        end
      end
    end

    def reset_prompt(input, output, count_described) : Bool
      reply = ""
      decision : Bool? = nil
      while decision.nil?
        output << "This will remove #{count_described} from queue #{@queue_name} in #{@dbfile}, are you sure? (y/N) "
        begin
          reply = input.gets(chomp: true) || ""
          # output << '\n'
          case reply[0]?
          when 'y', 'Y'
            decision = true
          when 'n', 'N', Nil
            decision = false
          end
        end
      end
      decision
    end

    private def parse_relative_time(str, *, negative = true, ignore_sign = false) : Time::Span?
      scan = StringScanner.new str
      count : Int64 = 1
      unit : Char = 's'
      if scan.scan '-'
        negative = true
      elsif scan.scan '+'
        negative = false
      end
      if scan.scan %r{\d+}
        count = scan[0].to_i64
      end
      if scan.scan %r{[smhdw]}i
        unit = scan[0].downcase[0]
      end
      span = case unit
             when 's'
               count.seconds
             when 'm'
               count.minutes
             when 'h'
               count.hours
             when 'd'
               count.days
             when 'w'
               count.weeks
             else
               raise ArgumentError.new "#{str}: could not parse time span unit"
             end
      span = span * -1 if negative && !ignore_sign
      span
    end

    private def parse_relative_time(str, *, positive : Bool, ignore_sign = false) : Time::Span?
      parse_relative_time(str, negative: !positive, ignore_sign: ignore_sign)
    end

    private def print_entry(entry : EntryTuple)
      printf "#{id_format} %s %s\n", entry[0], entry[1], entry[2]
    end

    private def print_entries(entries : Array(EntryTuple))
      entries.each { |entry| print_entry(entry) }
    end

    private def validate_queue_name(str) : Nil
      raise ArgumentError.new "#{str.inspect} is an invalid queue name" unless str =~ %r{\A[A-Za-z][A-Za-z0-9_]*\z}
    end

    private def create_table(table = @queue_name)
      @db.exec "CREATE TABLE IF NOT EXISTS \"#{table}\" (id INTEGER PRIMARY KEY, entry TEXT NOT NULL, creation_time TEXT NOT NULL)"
    end
  end
end

cli = Queue::CLI.new
cli.run
