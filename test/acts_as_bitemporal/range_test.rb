# encodsng: utf-8

require 'test_helper'

class ActsAsBitemporal::RangeTest < ActiveSupport::TestCase

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

  def test_with_integer_range_two_args
    first_range = ARange.new(1,4)
    RangeCases.each do |expected, b, e|
      assert_equal expected, first_range.intersects?(b, e), "1...4 vs #{b}...#{e}"
      assert_equal !expected, first_range.disjoint?(b, e), "1...4 vs #{b}...#{e}"
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
      ARange.new(1,4).intersects?(Object.new)
    end

    assert_raises ArgumentError do 
      ARange.new(1,4).intersects?(Date.today, Date.today + 1)
    end
  end

  def test_convenience_construction
    assert_equal ARange.new(1,4), ARange[1,4]
  end

  CoverCases = [
    # Exepected   First  Second
    [true,        1, 2,  1, 2],
    [true,        1, 10, 1, 2],
    [true,        1, 10, 2, 4],
    [true,        1, 10, 8, 10],
    [false,       1, 10, 8, 15],
    [false,       1, 10, 0, 2],
  ]

  def test_cover
    CoverCases.each do |expected, a_start, a_end, b_start, b_end|
      assert_equal expected, ARange[a_start, a_end].covers?(b_start, b_end)
      assert_equal expected, ARange[a_start, a_end].covers?(ARange[b_start, b_end])
      assert_equal expected, ARange[a_start, a_end].covers?(b_start...b_end)
    end
  end

  XorCases = [
    # First    Second   Expected Result
    [ [10,20], [0,15],  [[0,10],  [15,20] ]],
    [ [10,20], [10,15], [[15,20]          ]],
    [ [10,20], [12,15], [[10,12], [15,20] ]],
    [ [10,20], [15,20], [[10,15]          ]],
    [ [10,20], [15,25], [[10,15], [20,25] ]],
    [ [10,20], [20,25], [[10,20], [20,25] ]],
  ]

  def test_xor
    XorCases.each do |first, second, result|
      assert_equal(result.map { |pair| ARange[*pair] }, ARange[*first].xor(*second))
    end
  end

  MeetsCases = [
    # First    Second   Expected Result
    [ [10,20], [0,15],  false],
    [ [10,20], [0,10],  true],
    [ [10,20], [10,15], false],
    [ [10,20], [10,20], false],
    [ [10,20], [15,20], false],
    [ [10,20], [15,25], false],
    [ [10,20], [20,25], true]
  ]

  def test_meets
    MeetsCases.each do |first, second, expected|
      assert_equal(expected, ARange[*first].meets?(*second))
    end
  end

  def test_infinity
    now = Time.zone.now
    range = ARange[now, 'infinity']
    assert range.include?(now)
    assert range.include?(now + 1.day)
    assert range.include?(now + 1_000.days)
    assert range.include?(now + 1_000_000.days)
    assert !range.include?(now - 1.day)
    assert !range.include?(now - 1_000.days)
  end

  def test_negative_infinity
    now = Time.zone.now
    range = ARange['-infinity', now]
    assert !range.include?(now)
    assert !range.include?(now + 1.day)
    assert !range.include?(now + 1_000.days)
    assert !range.include?(now + 350_000.days)
    assert range.include?(now - 1.day)
    assert range.include?(now - 1_000.days)
    assert range.include?(now - 350_000.days)
  end

end
