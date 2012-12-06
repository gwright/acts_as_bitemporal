
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
      self.class.new([self.begin, other.begin].min, [self.end, other.end].max)
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
        self.class.new(range_or_start.begin, range_or_start.end)
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
