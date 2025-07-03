require "db"
require "sqlite3"

module Queue
  VERSION            = "0.1.0"
  DEFAULT_QUEUE_NAME = "default_queue"
  DEFAULT_QUEUE_FILE = "~/.cache/sqlq/default_queue"

  enum Command
    Help
    Add
    List
    Take
    Peek
    Run
    Delete
    Reset
  end

  class CLI
    property argv : Array(String)
    property dbfile : String
    property db : DB::Database
    property arguments : Array(String)
    setter id_format : String? = nil
    property cmd : Command
    property timezone : Time::Location
    property queue_name : String

    def initialize(@argv = ARGV.dup)
      opts = @argv.dup
      @arguments = [] of String
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
        when .starts_with? '-'
          raise ArgumentError.new "#{opt}: unknown option"
        when "add"
          @cmd = Command::Add
          @arguments = opts.dup
          opts = [] of String
        when "list"
          @cmd = Command::List
          @arguments = opts.dup
          opts = [] of String
        when "take", "get"
          @cmd = Command::Take
          @arguments = opts.dup
          opts = [] of String
        when "peek"
          @cmd = Command::Peek
          @arguments = opts.dup
          opts = [] of String
        when "run"
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
        else
          if @dbfile == ""
            @dbfile = opt
          else
            raise ArgumentError.new "#{opt}: expected a command add, list, take, get, peek, or run"
          end
        end
      end
      @dbfile = Path[DEFAULT_QUEUE_FILE].expand(home: true).to_s if @dbfile == ""
      parent = Path[@dbfile].parent
      Dir.mkdir_p parent unless File.directory? parent
      @db = DB.open "sqlite3:#{@dbfile}?max_idle_pool_size=3&initial_pool_size=3&journal_mode=wal&busy_timeout=5000"
      @db.exec "CREATE TABLE IF NOT EXISTS \"#{@queue_name}\" (id INTEGER PRIMARY KEY, entry TEXT NOT NULL, creation_time TEXT NOT NULL)"
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
      end
    end

    def id_format
      @id_format ||= create_id_format
    end

    def create_id_format
      max = @db.query_one "SELECT MAX(id) FROM \"#{@queue_name}\"", as: Int64
      "%" + max.to_s.size.to_s + "d"
    end

    def cmd_add
      if @arguments.empty?
        STDERR.puts "nothing specified to add"
      else
        creation_time = Time.local(location: @timezone)
        @arguments.each do |entry|
          @db.exec "INSERT INTO \"#{@queue_name}\" (entry, creation_time) VALUES (?, ?)", entry, creation_time
        end
        if 1 == @arguments.size
          puts "1 entry added"
        else
          puts "#{@arguments.size} entries added"
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
      entries = @db.query_all sql, as: {Int64, Time, String}
      entries.each do |entry|
        printf "#{id_format} %s %s\n", entry[0], entry[1], entry[2]
      end
    end

    def cmd_take
      if deleted_entry = @db.query_one? "DELETE FROM \"#{@queue_name}\" RETURNING entry ORDER BY creation_time, id LIMIT 1", as: String
        puts deleted_entry
      else
        STDERR.puts "no entries"
        exit 1
      end
    end

    def cmd_peek
      if next_entry = @db.query_one? "SELECT entry FROM \"#{@queue_name}\" ORDER BY creation_time, id LIMIT 1", as: String
        puts next_entry
      else
        STDERR.puts "no entries"
        exit 1
      end
    end

    def cmd_run
      raise RuntimeError.new "not implemented yet"
    end

    def cmd_delete
      if @arguments.empty?
        STDERR.puts "no entries to delete were given"
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
        when %r{^([\-\+]?)(\d+)([sSmMhHdDwW])(?:\.\.|:)?$}
          sign = case $1
                 when "-", "+"
                   $1
                 else
                   "-"
                 end
          count = $2.to_i64
          unit = $3.upcase
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

        results = @db.query_all sql, args: where_args, as: {Int64, Time, String}
        if results.empty?
          puts "no entries deleted"
        else
          if 1 == results.size
            puts "1 entry deleted"
          else
            puts "#{results.size} entries deleted"
          end
          results.each do |entry|
            printf "#{id_format} %s %s\n", entry[0], entry[1], entry[2]
          end
        end
      end
    end

    def cmd_reset
    end
  end
end

cli = Queue::CLI.new
cli.run
