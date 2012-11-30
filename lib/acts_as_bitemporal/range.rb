
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

    def merge(range_or_start, end_instant=nil)
      if end_instant
        other_start, other_end = range_or_start, end_instant
      else
        other_start, other_end = range_or_start.begin, range_or_start.end
      end
      raise ArgumentError, "ranges are disjoint" if disjoint?(other_start, other_end)
      self.class.new([self.begin, other_start].min, [self.end, other_end].max)
    end
  end
end
