# encodsng: utf-8

require 'test_helper'

class ActsAsBitemporalTest < ActiveSupport::TestCase

  def with_one_record(options={})
    entity_options = {:extra_columns => {:entity_id => :integer, :name => :string }}
    ActsAsBitemporalTestDatabase.with_model(options.reverse_merge(entity_options)) do |bt_model|
      key_attributes = {entity_id: 100, name: "First"}
      bt_model.bt_new(key_attributes).save!
      yield bt_model
    end
  end

  def test_bt_new
    ActsAsBitemporalTestDatabase.with_model(:extra_columns => {:entity_id => :integer }) do |bt_model|
      @key_attributes = { entity_id: 1000 }
      @record = bt_model.bt_new( @key_attributes )
      @record.save

      # Test valid time 
      assert_equal ActsAsBitemporal::Forever, @record.vtend_at,                    "should be valid forever"
      assert (@record.vtstart_at...(@record.vtend_at)).cover?(Time.now),           "should be valid now"
      refute (@record.vtstart_at...(@record.vtend_at)).cover?(Time.now - 24*60*60),"should not be valid yesterday"

      # Test valid time convenience methods
      assert @record.vt_forever?
      assert @record.vt_cover?(Time.now)

      # Test transaction time
      assert_equal @record.ttend_at, ActsAsBitemporal::Forever
      assert (@record.ttstart_at...(@record.ttend_at)).cover?(Time.now)

      # Test transaction time covenience methods
      assert @record.tt_forever?
      assert @record.tt_cover?(Time.now)
    end
  end

  def test_bt_update_attributes
    with_one_record do |bt_model|
      first_before      = bt_model.first

      assert_equal first_before.ttend_at, ActsAsBitemporal::Forever, first_before.ttend_at.inspect
      assert first_before.tt_forever?, first_before.inspect

      first_before.bt_update_attributes({})

      assert_equal 3, bt_model.count

      second            = bt_model.current!

      transaction_time  = second.ttstart_at
      first_after       = bt_model.find(first_before.id)

      # Previous version
      assert_equal first_before.vtstart_at,  first_after.vtstart_at,       "no change to vtstart_at"
      assert_equal first_before.vtend_at,    first_after.vtend_at,         "no change to vtend_at"
      assert_equal first_before.ttstart_at,  first_after.ttstart_at,       "no change to ttstart_at"
      assert_equal transaction_time,         first_after.ttend_at,         "previous version removed by transaction"

      # Newest version
      assert_equal transaction_time, second.vtstart_at,     "newest version becomes valid at transaction time"

    end
  end
end

