
module ActsAsBitemporal
  class Range < ::Range

    # Covenience method for creating instances.
    #   ActsAsBitemporal::Range[1,4]      # => [1,4)
    #   ActsAsBitemporal::Range.new(1,4)  # => [1,4)
    def self.[](start_instant, end_instant)
      new(start_instant, end_instant)
    end

    # All ranges are [closed, open)
    def initialize(first, second)
      super(first, second, true)
    end

    # Returns true if the range intersects range_or_event.
    #   Range[1,4].intersects?(1)             # => true
    #   Range[1,4].intersects?(4)             # => false
    #   Range[1,4].intersects?(Range[0,2])    # => true
    #   Range[1,4].intersects?(0...2)         # => true
    #   Range[1,4].intersects?(0...1)         # => false
    #   Range[1,4].intersects?(0,1)           # => false
    def intersects?(range_or_instant, end_instant=nil)
      if end_instant
        start_instant = range_or_instant
      elsif range_or_instant.respond_to?(:begin)
        start_instant = range_or_instant.begin
        end_instant = range_or_instant.end
      else
        instant = range_or_instant
      end

      if instant
        raise ArgumentError, "#{self.begin.class} expected, received #{instant.class}" unless instant.kind_of?(self.begin.class)
        self.begin <= instant and instant < self.end
      else
        start_instant < self.end and self.begin < end_instant
      end
    end

    def disjoint?(*args)
      not intersects?(*args)
    end

    # Returns true if the range covers the entire range_or_instant.
    def covers?(range_or_instant, end_instant=nil)
      if end_instant
        start_instant = range_or_instant
      elsif range_or_instant.respond_to?(:begin)
        start_instant = range_or_instant.begin
        end_instant = range_or_instant.end
      else
        instant = range_or_instant
      end

      if instant
        self.begin <= instant and instant < self.end
      else
        self.begin <= start_instant and end_instant <= self.end
      end
    end

    def merge(*args)
      other = coerce_range(*args)
      raise ArgumentError, "ranges are disjoint" if disjoint?(other) and !meets?(other)
      self.class.new([self.begin, other.begin].min, [self.end, other.end].max)
    end

    def meets?(*args)
      other = coerce_range(*args)
      self.begin == other.end or other.begin == self.end
    end

    # Return range representing intersection of this range and other range.
    def intersection(*args)
      other = coerce_range(*args)
      sorted = [self, other].sort

      return nil if sorted[0].end < sorted[1].begin

      ARange[sorted[1].begin, [sorted[1].end, sorted[0].end].min]
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
        range_or_start
      else
        self.class.new(range_or_start, range_or_start)
      end
    end

    def instant?
      self.begin == self.end
    end

    # Partial ordering of ranges based on the start endpoint.
    def <=>(other)
      self.begin <=> other.begin
    end
  end
end
