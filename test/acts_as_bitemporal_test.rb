# encodsng: utf-8

require 'test_helper'

class ActsAsBitemporalTest < ActiveSupport::TestCase
  ARange = ActsAsBitemporal::ARange
  Forever = ActsAsBitemporal::Forever
  T = ActsAsBitemporal::T
  EntityOptions = {:extra_columns => {:entity_id => :integer, :name => :string }}.freeze

  def entity_attributes(options={})
    {entity_id: 100, name: "one"}.merge(options)
  end

  def with_one_record(options={})
    entity_options = {:extra_columns => {:entity_id => :integer, :name => :string }}
    ActsAsBitemporalTestDatabase.with_model(options.reverse_merge(entity_options)) do |bt_model|
      bt_model.new(entity_attributes).save!
      yield bt_model
    end
  end

  def capture_transaction_bounds
    before = Time.zone.now
    result = yield
    after = Time.zone.now
    return result, ARange[before, after]
  end

  def empty_database(options={})
    ActsAsBitemporalTestDatabase.with_model(EntityOptions) do |bt_model|
      yield bt_model
    end
  end

  def test_current_time_create_with_no_conflicts
    empty_database do |bt_model|
      instance, bounds = capture_transaction_bounds do
        bt_model.create!(entity_attributes)
      end

      current_time_assertions(instance, bounds)
    end
  end
 
  def test_current_time_new_then_save_with_no_conflicts
    empty_database do |bt_model|
      instance, bounds = capture_transaction_bounds do
        bt_model.new(entity_attributes).tap { |r| r.save! }
      end

      current_time_assertions(instance, bounds)
    end
  end

  def current_time_assertions(record, transaction_bounds)
    assert transaction_bounds.covers?(record.vtstart_at),          "valid time started during create call"
    assert_equal Forever, record.vtend_at,                            "valid time ends at Forever"

    assert_equal record.vtstart_at, record.ttstart_at,              "transaction started at vtstart_at"
    assert_equal Forever, record.ttend_at,                            "transaction ends at Forever"
  end


  def test_current_time_create_with_current_time_conflict
    with_one_record do |bt_model|
      assert_raises(ActiveRecord::RecordInvalid) do
        bt_model.create!(entity_attributes(name: "two"))
      end
    end
  end

  def test_current_time_new_then_save_with_current_time_conflict
    with_one_record do |bt_model|
      assert_raises(ActiveRecord::RecordInvalid) do
        record = bt_model.new(entity_attributes(name: "two"))
        record.save!
      end
    end
  end

  def test_valid_time_create_with_no_scope_conflicts
    base_date = Time.zone.now
    empty_database do |bt_model|
      instance, bounds = capture_transaction_bounds do
        bt_model.create!(entity_attributes(vtstart_at: base_date, vtend_at: base_date + 1))
      end

      valid_time_assertions(instance, ARange[base_date, base_date + 1], bounds)
    end
  end

  def valid_time_assertions(record, vt_range, tt_bounds)
    assert_equal vt_range, record.vt_range,               "valid time as expected"
    assert       tt_bounds.covers?(record.ttstart_at),    "transaction start bounded"
    assert_equal Forever,  record.ttend_at,               "transaction ends at Forever"
  end

  def test_valid_time_new_then_save_with_no_scope_conflicts
    base_date = Time.zone.now
    empty_database do |bt_model|
      instance, bounds = capture_transaction_bounds do
        bt_model.new(entity_attributes(vtstart_at: base_date, vtend_at: base_date + 1)).tap { |r| r.save! }
      end

      valid_time_assertions(instance, ARange[base_date, base_date + 1], bounds)
    end
  end

  def test_valid_time_create_with_and_without_conflicts
    base_date = Time.zone.now
    empty_database do |bt_model|
      instance = bt_model.create!(entity_attributes(vtstart_at: base_date, vtend_at: base_date + 1))

      assert_raises(ActiveRecord::RecordInvalid) { 
        # Attempt to create a valid period conflict for the defined scope.
        bt_model.create!(entity_attributes(name: "two", vtstart_at: base_date, vtend_at: base_date + 1) )
      }

      # This record doesn't conflict with the existing valid time periods.
      instance2, bounds = capture_transaction_bounds do
        bt_model.create!(entity_attributes(name: "two", vtstart_at: base_date + 1, vtend_at: base_date + 2))
      end
      valid_time_assertions(instance2, ARange[base_date + 1, base_date + 2], bounds)

      assert_equal 2, instance.bt_versions.count,         "disjoint valid_time records"
    end
  end

  def test_valid_time_new_save_with_and_without_conflicts
    base_date = Time.zone.now
    empty_database do |bt_model|
      instance = bt_model.create!(entity_attributes(vtstart_at: base_date, vtend_at: base_date + 1))

      assert_raises(ActiveRecord::RecordInvalid) { 
        # Attempt to create a valid period conflict for the defined scope.
        record = bt_model.new(entity_attributes(name: "two", vtstart_at: base_date, vtend_at: base_date + 1) )
        record.save!
      }

      # This record doesn't conflict with the existing valid time periods.
      instance2, bounds = capture_transaction_bounds do
        bt_model.new(entity_attributes(name: "two", vtstart_at: base_date + 1, vtend_at: base_date + 2)).tap do |r| 
          r.save!
        end
      end
      valid_time_assertions(instance2, ARange[base_date + 1, base_date + 2], bounds)

      assert_equal 2, instance.bt_versions.count,         "disjoint valid_time records"
    end
  end

  def test_save_on_modified_records
    empty_database do |bt_model|
      base_date = Time.zone.now

      first            = bt_model.create!(entity_id: 2000, name: "One", vtstart_at: base_date, vtend_at: base_date + 2)
      first.vtstart_at = base_date + 3
      first.vtend_at   = base_date + 4

      refute            first.save,               "save can't be used to update records"
      assert_raises(ActiveRecord::RecordInvalid,  "save can't be used to update records") do
        first.save!
      end

      second = bt_model.create!(entity_id: 3000, name: "Two")
      second.name = "Two A"

      refute            second.save,               "save can't be used to update records"
    end
  end

  def test_bt_save_with_temporal_changes
    empty_database do |bt_model|
      record = bt_model.create!(entity_id: 1000, name: "One")

      oldstart, oldend = record.vtstart_at, record.vtend_at

      record.vtstart_at   = new_start = Time.zone.now - 1.day
      record.vtend_at     = new_end   = Time.zone.now + 1.day
      record.name         = "Two"
      revised             = record.bt_save
      revision            = revised.first

      assert_kind_of(bt_model, revision, "bt_save succeeded")
      assert_not_equal(record, revision, "revision returned")

      assert_equal ARange[record.ttstart_at, revision.ttstart_at], record.tt_range,   "old record transaction is closed"
      assert_equal ARange[oldstart, new_end], revision.vt_range,                       "new record retains previous period"
      assert_equal "Two", revision.name
    end
  end

  def test_bt_revise_with_non_temporal_changes
    base_date = Time.zone.now
    empty_database do |bt_model|

      # Create two records with disjoint valid periods.
      instance = bt_model.create!(entity_attributes(vtstart_at: base_date, vtend_at: base_date + 1.day))
      instance2 = bt_model.create!(entity_attributes(name: "two", vtstart_at: base_date + 1.day, vtend_at: base_date + 2.day))

      # Revise the second record to extend its valid period and change its name.
      changed = instance2.bt_revise(vtstart_at: base_date + 1.day, vtend_at: base_date + 3.day, name: 'two.a')

      revision_query = instance2.bt_versions.tt_forever.vt_intersect(base_date + 1.day, base_date + 3.day)

      assert_equal 1, revision_query.count

      revision = revision_query.first

      assert_equal 3, instance.bt_versions.count

      assert_equal 'two', instance2.name
      assert_equal 'two.a', revision.name

      assert_equal ARange[(base_date + 1.day), (base_date + 2.day)], revision.vt_range,       "new record only updates previously valid period"
      assert_equal ARange[instance2.ttstart_at, revision.ttstart_at], instance2.tt_range,     "old record has revised ttend"
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

  def test_non_temporal_new_and_save
    ActsAsBitemporalTestDatabase.with_model(:extra_columns => {:entity_id => :integer }) do |bt_model|
      record = bt_model.new(entity_id: 1000)
      record.save
      new_record_assertions(record, Time.zone.now)
    end
  end


  def test_bt_update_attributes
    with_one_record do |bt_model|
      first             = bt_model.first
      original_attrs    = first.attributes
      second            = first.bt_update_attributes(:name =>  "two")

      assert_equal 2,   first.bt_versions.count

      update_record_assertions(bt_model, original_attrs, first.ttend_at, {name: [nil, "two"]}, "update_attributes: ")
    end
  end

  def test_bt_update_attributes_with_superfluous_change
    with_one_record do |bt_model|
      first             = bt_model.first
      original_attrs    = first.attributes
      second            = first.bt_update_attributes(:name =>  "one")
      assert_equal 1,   first.bt_versions.count
      assert_nil        second
    end
  end

  def test_save_as_update
    with_one_record do |bt_model|
      first = bt_model.first!
      original_attrs = first.attributes

      first.name = new_name = "Second"
      revision = first.bt_save

      assert_equal first.ttend_at, revision.ttstart_at,  "transaction times match"

      update_record_assertions(bt_model, original_attrs, first.ttend_at, {name: [original_attrs[:name], new_name]}, "save_as_update: ")
    end
  end

  def update_record_assertions(model, original_attrs, transaction_time, diffs={}, prefix="")
    original = model.where(id: original_attrs['id']).first!
    current  = model.where(original_attrs.slice(*model.bt_scope_columns)).bt_current!

    # Previous version
    assert_equal original_attrs['vtstart_at'],  original.vtstart_at,       "#{prefix}no change to vtstart_at"
    assert_equal original_attrs['vtend_at'],    original.vtend_at,         "#{prefix}no change to vtend_at"
    assert_equal original_attrs['ttstart_at'],  original.ttstart_at,       "#{prefix}no change to ttstart_at"
    assert_equal transaction_time,              original.ttend_at,         "#{prefix}previous version removed by transaction"

    # Newest version
    assert_equal original_attrs['vtstart_at'], current.vtstart_at,    "#{prefix}no changes to valid start"
    assert_equal original_attrs['vtend_at'],   current.vtend_at ,     "#{prefix}no changes to valid end" 
    assert_equal transaction_time, current.ttstart_at,                "#{prefix}transaction times match"
    assert                         current.tt_forever?,               "#{prefix}transaction valid until changed"

    diffs.each do |k,(ov, nv)|
      assert_equal nv, current[k.to_s]
    end

    # Convenience method
    assert       current.forever?

    # Revised previous record
    #revised = current.bt_versions.tt_current.vt_intersect(original_attrs['vtstart_at']).first!
    #assert_equal original_attrs['vtstart_at'],  revised.vtstart_at,       "original valid start"
    #assert_equal transaction_time,              revised.vtend_at,         "transaction valid end"
    #assert_equal transaction_time,              revised.ttstart_at,       "must start at transaction time"
    #assert       revised.tt_forever?,                                     "forever valid end"
  end

  def test_bt_delete_no_future_records

    before_time = Time.zone.now
    with_one_record do |bt_model|
      current_rec         = bt_model.first!
      current_rec_before  = current_rec.attributes
      transaction_time    = current_rec.bt_delete
      after_time          = Time.zone.now

      current_rec_after   = current_rec.reload

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

  DeleteCases = [
    # StartList         Del         ResultList
    [ [[2,4]],             [2,4],      [            ]],
    [ [[2,4], [6,8]],      [2,4],      [ [6,8]      ]],
    [ [[2,4], [6,8]],      [3,7],      [ [2,3], [7,8]]],
    [ [[2,4],[4,6],[6,8]], [3,7],      [ [2,3], [7,8]]],
    [ [[2,nil]],           [3,nil],    [ [2,3]]],
    [ [[2,nil]],           [3,5],      [ [2,3], [5,nil]]]
  ]
  
  def test_delete_cases
    DeleteCases.each_with_index do |(start_list,  del_range, result_list), index|
      empty_database do |bt_model|
        base_date = Time.zone.now

        # Fill databsae
        start_list.each_with_index do |(start_offset, end_offset), index|
          bt_model.create!(entity_id: 100, vtstart_at: base_date + start_offset, vtend_at: end_offset ? base_date + end_offset : Forever)
        end

        base_record = bt_model.first
        versions    = base_record.bt_versions.tt_forever.order(:vtstart_at)

        assert_equal start_list.count, versions.count

        # apply change
        deleted = base_record.bt_delete3( (base_date + del_range.first), del_range.last ? (base_date + del_range.last) : Forever )

        assert deleted

        assert_equal result_list.count, versions.count, "#{index}, failed: #{versions.inspect}"

        # verify results
        versions.each_with_index do |result_record, index|
          assert_equal (base_date + result_list[index].first), result_record.vtstart_at, "record #{index}, vtstart_at correct"

          expected_end = result_list[index].last ? (base_date + result_list[index].last) : Forever
          assert_equal expected_end, result_record.vtend_at, "record #{index}, vtend_at correct"
        end
      end
    end
  end

  Update2Cases = [
    # StartList            Upd         Updated                   Unchanged
    [ [[2,4]],             [2,4],      [ [2,4]                ], []              ],
    [ [[2,4], [6,8]],      [2,4],      [ [2,4]                ], [[6,8]          ]],
    [ [[2,4], [6,8]],      [3,7],      [ [3,4], [6,7]         ], [[2,3], [7,8]   ]],
    [ [[2,4],[4,6],[6,8]], [3,7],      [ [3,4], [4,6], [6,7]  ], [[2,3], [7,8]   ]],
    [ [[2,nil]],           [3,nil],    [ [3,nil]              ], [[2,3]          ]],
    [ [[2,nil]],           [3,5],      [ [3,5]                ], [[2,3], [5, nil]]]
  ]
  
  def test_update2_cases
    Update2Cases.each_with_index do |(start_list,  upd_range, upd_list, unchanged_list), index|
      empty_database do |bt_model|
        base_date = Time.zone.now

        # Fill databsae
        start_list.each_with_index do |(start_offset, end_offset), index|
          bt_model.create!(
            entity_id: 100, 
            vtstart_at: base_date + start_offset.days, 
            vtend_at: end_offset ? base_date + end_offset.days : Forever
          )
        end

        base_record = bt_model.first
        versions    = base_record.bt_versions.tt_forever.order(:vtstart_at)
        assert_equal start_list.count, versions.count

        # apply change
        updated = base_record.bt_revise(
          entity_id: 200,
          vtstart_at: (base_date + upd_range.first.days), 
          vtend_at:   upd_range.last ? (base_date + upd_range.last.days) : Forever
        )

        refute updated.empty?

        assert_equal (upd_list.count + unchanged_list.count), versions.count, "#{index}, visible:\n#{versions.to_a.map(&:inspect).join("\n")}\nupdated:\n#{updated.to_a.map(&:inspect).join("\n")}\nerrors: #{updated.to_a.map { |u| u.errors.full_messages.inspect}}}"

        # verify updated record exist
        upd_list.each do |start_offset, end_offset|
          start_date = (base_date + start_offset.days)
          end_date   = end_offset ? (base_date + end_offset.days) : Forever
          expected = bt_model.tt_forever.where(vtstart_at: start_date, vtend_at: end_date).first

          assert expected, "record still in database"
        end
      end
    end
  end

  def test_attributes_writer
    empty_database  do |bt_model|
      alpha = bt_model.create(name: "Alpha", entity_id: 100)
      alpha.attributes = {name: "Beta"}

      assert_equal 'Beta', alpha.name

      beta = bt_model.create(name: "Beta", entity_id: 200)
      changes = {id: 200, entity_id: 300, vtstart_at: 5, vtend_at: 6, ttstart_at: 7, ttend_at: 8}

      beta.bt_attributes = changes
      assert beta.changes.empty?, "bt_attributes should ignore non-versioned attributes"

    end

  end
        

end
