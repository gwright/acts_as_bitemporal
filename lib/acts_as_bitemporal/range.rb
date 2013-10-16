
module ActsAsBitemporal
  class Range #< ::Range

    # Covenience method for creating instances.
    #   ActsAsBitemporal::Range[]           # => [now = Time.zone.now, now)
    #   ActsAsBitemporal::Range[time_ish]   # => [timeish, timeish)
    #   ActsAsBitemporal::Range[range_ish]  # => [rangeish.start, rangeish.end)
    #   ActsAsBitemporal::Range[start, end] # => [start, end)
    def self.[](*args)
      case args.size
      when 0
        args.push(now = Time.zone.now, now)
      when 1
        if (self === args.first) or (::Range === args.first)
          args = [args.first.begin, args.first.end]
        else
          args.push(args.first)
        end
      when 2
        #nothing
     else
       raise ArgumentError
     end

     new(*args)
    end
    #     begin, end, first, last,
    #     ==, eql?, hash, ===, 
    #     include?, member?, 
    #     inspect, pretty_print, step, to_s, to_yaml
    #     each, exclude_end?

    attr :begin, :end
    # All ranges are [closed, open)
    def initialize(min, max=min)
      @begin, @end = coerce(min), coerce(max)
    end

    def db_begin
      if infinite_begin?
        NinfinityLiteral
      else
        self.begin
      end
    end

    def db_end
      if infinite_end?
        InfinityLiteral
      else
        self.end
      end
    end

    def infinite_begin?
      self.begin == Ninfinity
    end

    def infinite_end?
      self.end == Infinity
    end

    def coerce(other)
      if other == "infinity" or other == InfinityValue
        Infinity
      elsif other == "-infinity" or other == NinfinityValue
        Ninfinity
      elsif other.respond_to?(:in_time_zone)
        other.in_time_zone
      elsif other.respond_to?(:to_time_in_current_zone)
        other.to_time_in_current_zone
      elsif other.respond_to?(:to_time)
        other.to_time.in_time_zone
      elsif other.respond_to?(:<=>)
        other
      else
        fail ArgumentError, "unable to convert: #{other.class} to ActiveSupport::TimeWithZone instance"
      end
    end

    alias first begin
    alias last end

    def ==(other)
      self.begin == other.begin and self.end == other.end
    end

    def eql?(other)
      self == other
    end

    # XXX Not sure about this...
    def hash
      self.begin.hash ^ self.end.hash
    end

    # The order is important here so that DateTime::Infinity
    # values work for end points.
    def ===(instant)
      (instant >= self.begin) and (instant < self.end)
    end
    alias include? ===
    alias member? ===
    alias cover? ===

    def each
      raise "use each_day, each_month, or each_year"
    end

    def each_day
    end


    # Returns true if the range intersects range or value.
    #   Range[1,4].intersects?(1)             # => true
    #   Range[1,4].intersects?(4)             # => false
    #   Range[1,4].intersects?(Range[0,2])    # => true
    #   Range[1,4].intersects?(0...2)         # => true
    #   Range[1,4].intersects?(0...1)         # => false
    #   Range[1,4].intersects?(0,1)           # => false
    def intersects?(*args)
      other = coerce_range(*args)
      raise ArgumentError, "#{self.begin.class} expected, received #{other.begin.class}" unless other.begin.kind_of?(self.begin.class)

      if other.instant?
        self.begin <= other.begin and other.end < self.end
      else
        other.begin < self.end and self.begin < other.end
      end
    end

    # Returns true if the range does not intersect range or value.
    #   Range[1,4].intersects?(1)             # => false
    #   Range[1,4].intersects?(4)             # => true
    #   Range[1,4].intersects?(Range[0,2])    # => false
    #   Range[1,4].intersects?(0...2)         # => false
    #   Range[1,4].intersects?(0...1)         # => true
    #   Range[1,4].intersects?(0,1)           # => true
    def disjoint?(*args)
      not intersects?(*args)
    end

    # Returns true if the range covers the entire range or value.
    def covers?(*args)
      other = coerce_range(*args)

      if other.instant?
        self.begin <= other.begin and other.end < self.end
      else
        self.begin <= other.begin and other.end <= self.end
      end
    end

    def merge(*args)
      other = coerce_range(*args)
      raise ArgumentError, "ranges are disjoint" if disjoint?(other) and !meets?(other)
      self.class.new(self.class.min(self.begin, other.begin), self.class.max(self.end, other.end))
    end

    # Returns true if there is no gap between the range and the other range.
    #   Range[1,4].meets?(4, 5)   # => true
    #   Range[1,4].meets?(0, 1)   # => true
    #   Range[1,4].meets?(5, 7)   # => false
    #   Range[1,4].meets?(1, 4)   # => false
    def meets?(*args)
      other = coerce_range(*args)
      self.begin == other.end or other.begin == self.end
    end

    # Return range representing intersection of this range and other range.
    def intersection(*args)
      other = coerce_range(*args)
      sorted = [self, other].sort

      return nil if self.class.compare(sorted[0].end, sorted[1].begin) < 0

      ARange[sorted[1].begin, self.class.min(sorted[1].end, sorted[0].end)]
    end

    def self.compare(a,b)
      a = (a == -Float::INFINITY) ? Ninfinity : a
      b = (b == Float::INFINITY) ? Infinity : b

      result = if a.kind_of?(Date::Infinity) 
        -(b <=> a)
      else
        (a <=> b)
      end

      if result.nil?
        -compare(b,a)
      else
        result
      end
    end

    def self.min(a,b)
      if compare(a,b) < 0
        a
      else
        b
      end
    end

    def self.max(a,b)
      if compare(a,b) > 0
        a
      else
        b
      end
    end

    def difference(*other)
      xor(intersection(*other))
    end

    # Return array of ranges representing intervals that are in this range or the other, but not both.
    def xor(*args)
      other = coerce_range(*args)
      intersection = intersection(other)
      return [self, other].sort unless intersection

      merged = merge(other)

      [ ARange[merged.begin, intersection.begin], ARange[intersection.end, merged.end] ].reject { |r| r.instant? } 
    end

    alias :^ xor

    def coerce_range(range_or_start, end_instant=nil)
      if end_instant
        self.class.new(range_or_start, end_instant)
      elsif range_or_start.respond_to?(:begin)
        self.class.new(range_or_start.begin, range_or_start.end)
      else
        self.class.new(range_or_start, range_or_start)
      end
    end

    def instant?
      self.begin == self.end
    end

    # Partial ordering of ranges based on the start endpoint.
    # If comparison fails, try reverse to handle DateTime::Infinity
    def <=>(other)
      (self.begin <=> other.begin) || (other.begin <=> self.begin)
    end

    def inspect
      "#{inspect_time(db_begin)}...#{inspect_time(db_end)}"
    end

    def inspect_time(value)
      if value.kind_of?(String)
        value
      elsif value.nil?
        "null"
      elsif self.class[Time.zone.now - 12.hours, Time.zone.now + 12.hours].include?(value)
        value.strftime("%r")
      else
        value.strftime("%F")
      end
    end

    def to_a
      [self.begin, self.end]
    end
  end
end
