# encodsng: utf-8

require 'test_helper'

class ActsAsBitemporalTest < ActiveSupport::TestCase
  ARange = ActsAsBitemporal::ARange
  Forever = ActsAsBitemporal::Forever
  EntityOptions = {:extra_columns => {:entity_id => :integer, :name => :string }}.freeze

  def test_new_with_no_conflicts
    base_date = Time.zone.today
    ActsAsBitemporalTestDatabase.with_model(EntityOptions) do |bt_model|
      instance = bt_model.create!(entity_id: 100, name: "one", vtstart_at: base_date, vtend_at: base_date + 1)
      assert_equal instance.vt_range, ARange[base_date, base_date + 1]
    end
  end

  def test_new_with_conflicts
    base_date = Time.zone.today
    ActsAsBitemporalTestDatabase.with_model(EntityOptions) do |bt_model|
      instance = bt_model.create!(entity_id: 100, name: "one", vtstart_at: base_date, vtend_at: base_date + 1)
      assert_raises(ActiveRecord::RecordInvalid) { 
        bt_model.create!(entity_id: 100, name: "two", vtstart_at: base_date, vtend_at: base_date + 1) 
      }
      instance2 = bt_model.create!(entity_id: 100, name: "two", vtstart_at: base_date + 1, vtend_at: base_date + 2)
      assert_equal instance2.vt_range, ARange[base_date + 1, base_date + 2]
      assert_equal 2, instance.bt_versions.count
    end
  end

  def test_vt_revise
    base_date = Time.zone.now
    ActsAsBitemporalTestDatabase.with_model(EntityOptions) do |bt_model|

      # Create two records with disjoint valid periods.
      instance = bt_model.create!(entity_id: 100, name: "one", vtstart_at: base_date, vtend_at: base_date + 1.day)
      instance2 = bt_model.create!(entity_id: 100, name: "two", vtstart_at: base_date + 1.day, vtend_at: base_date + 2.day)

      # Revise the second record to extends its valid period.
      revision = instance2.vt_revise(base_date + 1.day, base_date + 3.day)
      assert_equal 3, instance.bt_versions.count

      assert_equal ARange[base_date + 1.day, base_date + 3.day], revision.vt_range,                   "new record has updated valid period"
      assert_equal ARange[instance2.ttstart_at, revision.ttstart_at], instance2.tt_range,     "old record has revised ttend"
    end
  end

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
    assert_equal Forever, record.vtend_at,                                     "should be valid forever"
    assert (record.vtstart_at...(record.vtend_at)).cover?(now_time),           "should be valid now"
    refute (record.vtstart_at...(record.vtend_at)).cover?(now_time - 24*60*60),"should not be valid yesterday"

    # Test valid time convenience methods
    assert record.vt_forever?
    assert record.vt_intersects?(now_time)

    # Test transaction time
    assert_equal record.ttend_at, Forever
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
      first.bt_save

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

  def test_bt_delete_no_future_records

    before_time = Time.zone.now
    with_one_record do |bt_model|
      current_rec         = bt_model.first!
      current_rec_before  = current_rec.attributes
      transaction_time    = current_rec.bt_delete
      after_time          = Time.zone.now

      current_rec_after   = current_rec.reload

      warn current_rec_before.inspect
      assert_equal current_rec_before['vtstart_at'],  current_rec_after.vtstart_at,   "vtstart_at unchanged"
      assert_equal current_rec_before['vtend_at'],    current_rec_after.vtend_at,     "vtend_at unchanged"
      assert_equal current_rec_before['ttstart_at'],  current_rec_after.ttstart_at,   "ttstart_at unchanged"
      assert_equal transaction_time,                  current_rec_after.ttend_at,     "ttend_at changed to transaction time"

      versions = current_rec.bt_versions.reject { |r| r == current_rec }

      assert_equal 1, versions.count,     "only one other version"

      revised_record = versions.first

      assert_equal current_rec_before['vtstart_at'],  revised_record.vtstart_at
      assert_equal transaction_time,                  revised_record.vtend_at
      assert_equal transaction_time,                  revised_record.ttstart_at
      assert_equal Forever,                           revised_record.ttend_at

    end
  end

  def test_bt_delete_with_future_records
    skip("not implemented")
  end

end

