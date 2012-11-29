
module ActsAsBitemporal
  class Range < ::Range

    # All ranges are [closed, open)
    def initialize(first, second)
      super(first, second, true)
    end

    def intersects?(range_or_event)
      case range_or_event
      when ::Range
        range_or_event.begin < self.end and self.begin < range_or_event.end
      when self.end.class
        self.begin <= range_or_event and range_or_event < self.end
      else
        raise ArgumentError, "Range or #{self.begin.class} expected, received #{range_or_event.class}"
      end
    end

    def disjoint?(range2)
      not intersects?(range2)
    end
  end
end
