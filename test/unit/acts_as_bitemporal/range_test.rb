# encodsng: utf-8

require 'test_helper'

class ActsAsBitemporalRangeTest < ActiveSupport::TestCase

  ARange = ActsAsBitemporal::Range

  RangeCases = [
      # expected, start, end, visual
      [false, 6, 7],     #    ---o -o
      [false, 5, 6],     #    ---o-o
      [false, 4, 5],     #    ---*o
      [true, 3, 4],      #    --=O
      [true, 2, 3],      #    -=*o
      [true, 1, 2],      #    =*-o
      [false, 0, 1],     #   -*--o
      [false, -1, 0],    #  -o---o
      [false, -1, -2],   # -o ---o
      [true, 1, 5],      #    -==*o
      [true, 1, 4],      #    ===O
      [true, 0, 3],      #   -==*o
      [true, 0, 4],      #   -===O
      [true, 1, 5],      #    ===*o
      [true, 0, 5],      #   -===*o
    ]

  def test_with_integer_range
    first_range = ARange.new(1,4)
    RangeCases.each do |expected, b, e|
      assert_equal expected, first_range.intersects?(b...e), "1...4 vs #{b}...#{e}"
      assert_equal !expected, first_range.disjoint?(b...e), "1...4 vs #{b}...#{e}"
    end
  end

  def test_with_integer_arange
    first_range = ARange.new(1,4)
    RangeCases.each do |expected, b, e|
      assert_equal expected, first_range.intersects?(ARange.new(b, e)), "1...4 vs #{b}...#{e}"
      assert_equal !expected, first_range.disjoint?(ARange.new(b, e)), "1...4 vs #{b}...#{e}"
    end
  end

  def test_with_date_arange
    base_date = Time.zone.now
    d1_range = ARange.new((base_date+1), (base_date+4))

    RangeCases.each do |expected, b, e|
      d2_range = ARange.new(base_date+b, base_date+e)
      assert_equal expected, d1_range.intersects?(d2_range), "#{d1_range} vs #{b}...#{e}"
      assert_equal !expected, d1_range.disjoint?(d2_range), "#{d1_range} vs #{b}...#{e}"
    end
  end

  EventRangeCases = [
      # expected, start, end, visual
      [false, 6],     #    ---o -o
      [false, 5],     #    ---o-o
      [false, 4],     #    ---*o
      [true, 3],      #    --=O
      [true, 2],      #    -=*o
      [true, 1],      #    =*-o
      [false, 0],     #   -*--o
      [false, -1],    #  -o---o
      [false, -1],    # -o ---o
    ]

  def test_with_integer_event
    first_range = ARange.new(1,4)
    EventRangeCases.each do |expected, event|
      assert_equal expected, first_range.intersects?(event), "#{first_range} vs #{event}"
      assert_equal !expected, first_range.disjoint?(event), "#{first_range} vs #{event}"
    end
  end

  def test_with_time_event
    base_date = Time.zone.now
    first_range = ARange.new(base_date + 1, base_date + 4)

    EventRangeCases.each do |expected, event|
      event = base_date + event
      assert_equal expected, first_range.intersects?(event), "#{first_range} vs #{event}"
      assert_equal !expected, first_range.disjoint?(event), "#{first_range} vs #{event}"
    end
  end

  def test_with_invalid_argument
    assert_raises ArgumentError do 
      ARange.new(1,4).intersects?(true)
    end
  end
end
