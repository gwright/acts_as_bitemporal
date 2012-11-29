# encodsng: utf-8

require 'test_helper'

class ActsAsBitemporalTest < ActiveSupport::TestCase

  def with_one_record(options={})
    entity_options = {:extra_columns => {:entity_id => :integer, :name => :string }}
    ActsAsBitemporalTestDatabase.with_model(options.reverse_merge(entity_options)) do |bt_model|
      key_attributes = {entity_id: 100, name: "First"}
      bt_model.new(key_attributes).save!
      yield bt_model
    end
  end

  def new_record_assertions(record, now_time=Time.zone.now, transaction_time=nil)
    # Test valid time 
    assert_equal ActsAsBitemporal::Forever, record.vtend_at,                    "should be valid forever"
    assert (record.vtstart_at...(record.vtend_at)).cover?(now_time),           "should be valid now"
    refute (record.vtstart_at...(record.vtend_at)).cover?(now_time - 24*60*60),"should not be valid yesterday"

    # Test valid time convenience methods
    assert record.vt_forever?
    assert record.vt_intersects?(now_time)

    # Test transaction time
    assert_equal record.ttend_at, ActsAsBitemporal::Forever
    assert (record.ttstart_at...(record.ttend_at)).cover?(now_time)

    # Test transaction time covenience methods
    assert record.tt_forever?
    assert record.tt_intersects?(now_time)
  end

  def test_new_and_save
    ActsAsBitemporalTestDatabase.with_model(:extra_columns => {:entity_id => :integer }) do |bt_model|
      record = bt_model.new(entity_id: 1000)
      record.save
      new_record_assertions(record, Time.zone.now)
    end
  end

  def test_create
    ActsAsBitemporalTestDatabase.with_model(:extra_columns => {:entity_id => :integer }) do |bt_model|
      record = bt_model.create!(entity_id: 1000)
      new_record_assertions(record, Time.zone.now)
    end
  end

  def test_bt_update_attributes
    with_one_record do |bt_model|
      first             = bt_model.first
      original_attrs    = first.attributes
      second            = first.bt_update_attributes({})

      assert_equal 3,   first.bt_versions.count

      update_record_assertions(bt_model, original_attrs, first.ttend_at)
    end
  end

  def test_save_as_update
    with_one_record do |bt_model|
      first = bt_model.first!
      original_attrs = first.attributes

      first.name = new_name = "Second"
      first.save

      update_record_assertions(bt_model, original_attrs, first.ttend_at, name: [original_attrs[:name], new_name])
    end
  end

  def update_record_assertions(model, original_attrs, transaction_time, diffs={})
    original = model.where(id: original_attrs['id']).first!
    current  = model.where(original_attrs.slice(*model.bt_scope_columns)).bt_current!

    # Previous version
    assert_equal original_attrs['vtstart_at'],  original.vtstart_at,       "no change to vtstart_at"
    assert_equal original_attrs['vtend_at'],    original.vtend_at,         "no change to vtend_at"
    assert_equal original_attrs['ttstart_at'],  original.ttstart_at,       "no change to ttstart_at"
    assert_equal transaction_time,              original.ttend_at,         "previous version removed by transaction"

    # Newest version
    assert_equal transaction_time, current.vtstart_at,     "newest version becomes valid at transaction time"
    assert                         current.vt_forever?,    "newest version valid forever"
    assert_equal transaction_time, current.ttstart_at
    assert                         current.tt_forever?

    # Convenience method
    assert       current.forever?

    # Revised previous record
    revised = current.bt_versions.tt_current.vt_intersect(original_attrs['vtstart_at']).first!
    assert_equal original_attrs['vtstart_at'],  revised.vtstart_at,       "original valid start"
    assert_equal transaction_time,              revised.vtend_at,         "transaction valid end"
    assert_equal transaction_time,              revised.ttstart_at,       "must start at transaction time"
    assert       revised.tt_forever?,                                     "forever valid end"
  end
end

