
module ActsAsBitemporal
  # Represents a bitemporal zone specified by a valid time
  # range and a transaction time range.
  class Zone #< ::Range

    # Covenience method for creating instances.
    #   ActsAsBitemporal::Zone[*args]
    #   ActsAsBitemporal::Zone.new(*args)
    def self.[](*args)
      new(*args)
    end
    #     begin, end, first, last,
    #     ==, eql?, hash, ===, 
    #     include?, member?, 
    #     inspect, pretty_print, step, to_s, to_yaml
    #     each, exclude_end?

    attr :vt_range, :tt_range
    # All ranges are [closed, open)
    def initialize(vt_range=Time.zone.now, tt_range=vt_range)
      @vt_range, @tt_range = coerce_args(vt_range, tt_range)
    end

    def coerce_zone(*args)
      if args.size == 1 and self.class === args.first
        args.first
      else
        coerce_args(*args)
      end
    end

    def coerce_args(*args)
      case args.size
      when 0
        now = Time.zone.now
        args.push(now, now)
      when 1
        args.push(args.first)
      when 2
        # do nothing
      else
        raise ArgumentError
      end

      [Range[args.first], Range[args.last]]
    end

    def ==(other)
      self.vt_range == other.vt_range and
      self.tt_range == other.tt_range
    end

    def eql?(other)
      self == other
    end

    def covers?(other)
      (vt_range.covers?(other.vt_range)) and (tt_range.covers?(other.tt_range))
    end

    # Returns true if the zone intersects other zone
    def intersects?(*args)
      other = coerce_zone(*args)
      vt_range.intersects?(other.vt_range) and tt_range.intersects?(other.tt_range)
    end

    # Returns true if the zone does not intersect other zone.
    def disjoint?(*args)
      not intersects?(*args)
    end

    # Return range representing intersection of this range and other range.
    def intersection(*args)
      other = coerce_zone(*args)

      vt = vt_range.intersection(other.vt_range)
      tt = tt_range.intersection(other.tt_range)
      vt && tt && self.class.new( vt, tt )
    end

    alias ^ intersection

    # Does this zone represent a snapshot?
    #  -- valid time fixed
    #  -- transaction time fixed
    def snapshot?
      vt_range.instant? and tt_range.instant?
    end

    def snapshot(vt_instant, tt_instant)
      if vt_range.include?(vt_instant) and tt_range.include?(tt_instant)
        self.class.new(Range[vt_instant], Range[tt_instant])
      else
        raise ArgumentError, "timestamps outside bounds of zone"
      end
    end

    # Does this zone represent a historical timeline?
    #  -- valid time varies
    #  -- transaction time fixed
    def historical?
      tt_range.instant? and !vt_range.instant?
    end

    def historical(tt_instant)
      if tt_range.include?(tt_instant)
        self.class.new(vt_range, Range[tt_instant])
      else
        raise ArgumentError, "timestamp outside bounds of zone"
      end
    end

    # Does this zone represent a rollback timeline?
    #  -- valid time fixed
    #  -- transaction time varies
    def rollback?
      vt_range.instant?  and !tt_range.instant?
    end

    def rollback(vt_instant)
      if vt_range.include?(vt_instant)
        self.class.new(Range[vt_instant], tt_range)
      else
        raise ArgumentError, "timestamps outside bounds of zone"
      end
    end

    def inspect
      "#{vt_range.inspect}, #{tt_range.inspect}"
    end

    def to_a
      [vt_range.begin, vt_range.end, tt_range.begin, tt_range.end]
    end
  end
end
