# encodsng: utf-8

require 'test_helper'

class ActsAsBitemporalTest < ActiveSupport::TestCase
  ARange = ActsAsBitemporal::ARange
  InfinityValue = ActsAsBitemporal::InfinityValue
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

  def with_records(records, options={})
    empty_database(options) do |bt_model|
      records.each do |r|
        bt_model.new(r).save!
      end
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
    assert_equal InfinityValue, record.vtend_at,                         "valid time ends at Infinity"

    assert_equal record.vtstart_at, record.ttstart_at,             "transaction started at vtstart_at"
    assert_equal InfinityValue, record.ttend_at,                         "transaction ends at Infinity"
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
    assert_equal InfinityValue,  record.ttend_at,               "transaction ends at Infinity"
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
      assert_raises(ActiveRecord::RecordNotSaved,  "save can't be used to update records") do
        first.save!
      end

      second = bt_model.create!(entity_id: 3000, name: "Two")
      second.name = "Two A"

      refute            second.save,               "save can't be used to update records"
    end
  end

  def test_bt_commit_with_temporal_changes
    empty_database do |bt_model|
      record = bt_model.create!(entity_id: 1000, name: "One")

      oldstart, oldend = record.vtstart_at, record.vtend_at

      record.vtstart_at   = new_start = Time.zone.now - 1.day
      record.vtend_at     = new_end   = Time.zone.now + 1.day
      record.name         = "Two"
      revised             = record.bt_commit
      revision            = revised.first

      assert_kind_of(bt_model, revision, "bt_commit succeeded")
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
    assert_equal InfinityValue, record.vtend_at,                                     "should be valid forever"
    assert ARange[record.vtstart_at, record.vtend_at].cover?(now_time),           "should be valid now"
    refute ARange[record.vtstart_at, record.vtend_at].cover?(now_time - 24*60*60),"should not be valid yesterday"

    # Test valid time convenience methods
    assert record.vt_forever?
    assert record.vt_intersects?(now_time)

    # Test transaction time
    assert_equal record.ttend_at, InfinityValue
    assert ARange[record.ttstart_at, record.ttend_at].cover?(now_time)

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
      second            = first.bt_revise(:name =>  "two")

      assert_equal 2,   first.bt_versions.count

      update_record_assertions(bt_model, original_attrs, first.ttend_at, {name: [nil, "two"]}, "update_attributes: ")
    end
  end

  def test_bt_commit_with_superfluous_change
    with_one_record do |bt_model|
      first             = bt_model.first
      original_attrs    = first.attributes

      first.name = "one"
      second            = first.bt_commit

      assert_equal 1,   first.bt_versions.count
      assert_empty      second
    end
  end

  def test_save_as_update
    with_one_record do |bt_model|
      first = bt_model.first!
      original_attrs = first.attributes

      first.name = new_name = "Second"
      revision = first.bt_commit.first

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
    assert       current.bt_forever?

    # Revised previous record
    # XXX
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

      current_rec.bt_delete
      transaction_time    = current_rec.ttend_at
      after_time          = Time.zone.now

      current_rec_after   = current_rec.reload

      assert_equal current_rec_before['vtstart_at'],  current_rec_after.vtstart_at,   "vtstart_at unchanged"
      assert_equal current_rec_before['vtend_at'],    current_rec_after.vtend_at,     "vtend_at unchanged"
      assert_equal current_rec_before['ttstart_at'],  current_rec_after.ttstart_at,   "ttstart_at unchanged"
      assert_equal transaction_time,                  current_rec_after.ttend_at,     "ttend_at changed to transaction time"

      versions = current_rec.bt_versions
      assert_equal 1, versions.count,     "only one version"

    end
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
          bt_model.create!(entity_id: 100, vtstart_at: base_date + start_offset, vtend_at: end_offset ? base_date + end_offset : InfinityValue)
        end

        base_record = bt_model.first
        versions    = base_record.bt_versions.tt_forever.order(:vtstart_at)

        assert_equal start_list.count, versions.count

        # apply change
        deleted = base_record.bt_delete( (base_date + del_range.first), del_range.last ? (base_date + del_range.last) : InfinityValue )

        assert deleted

        assert_equal result_list.count, versions.count, "#{index}, failed: #{versions.inspect}"

        # verify results
        versions.each_with_index do |result_record, index|
          assert_equal (base_date + result_list[index].first), result_record.vtstart_at, "record #{index}, vtstart_at correct"

          expected_end = result_list[index].last ? (base_date + result_list[index].last) : InfinityValue
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
            name: "one",
            vtstart_at: base_date + start_offset.days,
            vtend_at: end_offset ? base_date + end_offset.days : InfinityValue
          )
        end

        base_record = bt_model.first
        versions    = base_record.bt_versions.tt_forever.order(:vtstart_at)
        assert_equal start_list.count, versions.count

        # apply change
        updated = base_record.bt_revise(
          vtstart_at: (base_date + upd_range.first.days),
          vtend_at: upd_range.last ? (base_date + upd_range.last.days) : InfinityValue,
          name: "two"
        )

        assert_equal upd_list.size, updated.size, "update case #{index}: should have expected changes"

        assert_equal (upd_list.count + unchanged_list.count), versions.count, "#{index}, visible:\n#{versions.to_a.map(&:inspect).join("\n")}\nupdated:\n#{updated.to_a.map(&:inspect).join("\n")}\nerrors: #{updated.to_a.map { |u| u.errors.full_messages.inspect}}}"

        # verify updated record exist
        upd_list.each do |start_offset, end_offset|
          start_date = (base_date + start_offset.days)
          end_date   = end_offset ? (base_date + end_offset.days) : InfinityValue
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
      changes = {id: 200, entity_id: 300}

      beta.bt_attributes = changes
      assert beta.changes.empty?, "bt_attributes should ignore non-versioned attributes"
    end

  end

  Timestamp1 = Time.zone.now

  Sample = [
    { entity_id: 1, name: 'first', vtstart_at: '2010-01-01', vtend_at: '2010-02-01', ttstart_at: '2010-01-01', ttend_at: '2010-02-01'},
    { entity_id: 1, name: 'second', vtstart_at: '2010-02-01', vtend_at: '2010-03-01', ttstart_at: '2010-02-01', ttend_at: Timestamp1 },
    { entity_id: 1, name: 'third', vtstart_at: '2010-03-01', ttstart_at: Timestamp1 },
    { entity_id: 2, name: 'first Alternate', vtstart_at: '2010-01-01'},
  ]
  SampleMap = <<MAP
0: AA  
0: ABB 
0:  BCC
0:   CC
1: AAAA
1: AAAA
MAP

  def test_bt_current
    with_records Sample do |bt_model|
      assert_equal SampleMap, bt_model.unscoped.bt_ascii

      entity1 = bt_model.where(entity_id: 1).order(:ttstart_at)

      assert_equal 3, entity1.size

      assert_equal 1, entity1.bt_current.size
      assert_equal 'third', entity1.bt_current.first.name

      assert_equal 1, entity1.bt_current.size
      assert_equal 'second', entity1.bt_current('2010-02-01').first.name, "bt_current with instant argument"

    end
  end

  def test_bt_revise_with_no_overlaps
    with_records Sample do |bt_model|

      entity2 = bt_model.where(entity_id: 2).first

      # Create a new version that pre-dates the existing version.
      new_start = entity2.vtstart_at = entity2.vtstart_at - 15.days
      new_end = entity2.vtend_at = entity2.vtstart_at + 5.days
      entity2.bt_revise

      entity2_versions = entity2.bt_versions
      assert_equal 2, entity2_versions.size, 'should have two versions'

      versions  = entity2_versions.vt_intersect(new_start + 1.day)
      assert_equal 1, versions.size, 'new version should have new vt range'

      new_version = versions.first
      assert new_version, 'should only be one version in the new range'
      assert_equal new_start, new_version.vtstart_at, 'should have expected vtstart'
      assert_equal new_end, new_version.vtend_at, 'should have expected vtend'

      # Create another version that is even older and has change to non-temporal column'
      start3 = new_version.vtstart_at = new_start - 15.days
      end3 = new_version.vtend_at = start3 + 5.days
      name3 = new_version.name = 'Updated Entity 2'
      result = new_version.bt_revise

      assert_equal start3, result.first.vtstart_at, 'has revised vtstart_at'
      assert_equal end3, result.first.vtend_at, 'has revised vtend_at'
      assert_equal name3, result.first.name, 'has revised name'

      newest_versions  = entity2.bt_versions
      assert_equal 3, newest_versions.size, 'should have three versions now'

      version3 = newest_versions.vt_intersect(start3 + 1.day)
      assert_equal 1, version3.size,   'only one version in oldest vt range'
      assert_equal 'Updated Entity 2', version3.first.name, 'should have expected name'

    end
  end

  def test_bt_delete
    with_records Sample do |bt_model|
      first = bt_model.where(name: 'third').first
      first_start = first.vtstart_at
      first_end = first.vtend_at

      start2 = Time.zone.parse('2010-04-01')
      end2 = Time.zone.parse('2010-05-01')
      result = first.bt_delete(start2, end2)

      assert_equal 1, result.size

      thirds = bt_model.where(name: 'third')
      assert_equal 3, thirds.size
      thirds.each { |r|
        warn "#{r.id} #{r.vtstart_at} #{r.vtend_at} #{r.name}"
      }

      assert thirds.where(vtstart_at: first_start, vtend_at: first_end).exists?
      assert thirds.where(vtstart_at: first_start, vtend_at: start2).exists?
      assert thirds.where(vtstart_at: end2, vtend_at: first_end).exists?

    end
  end
end

