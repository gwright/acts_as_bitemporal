# encoding: utf-8
require "acts_as_bitemporal/version"
require "acts_as_bitemporal/range"
require 'active_support/time'
require 'active_record'

# ActsAsBitemporal provides a framework for recording multiple versions
# of an Active Record model. The module is appropriate for tracking
# information that changes over time while maintaining a history of all
# the changes.
#
# The framework expects the model to be
# composed of the following attributes:
#
# id:                   the active record unique identifier
# scope attributes:     one or more attributes that uniquely identify a
#                       model instance.
# temporal attributes:  four timestamp columns that define the temporal
#                       scope of the model instance.
# value attributes:     attributes that define the values that characterize
#                       each version of the record.
#
# A bitemporal model describes an entity with attributes that change
# over time. The scope attributes form a composite key that identifies
# that identifies the entity. The id column provides a unique key to
# identify a particular version of an entity and the temporal columns
# define the temporal scope of the version.
#
# The temporal scope of a version is defined by two timestamp ranges,
# valid time, and transaction time.  The transaction time range specifies
# when the record was added to the database and when the record was logically
# removed from the database. In normal operation, records are never physically
# deleted from the database. The valid time range specifies time period during
# which the value attributes are to be associated with the entity.
# Changes to an entity's attributes over time are represented as a
# succession of records all with the same scope attributes but with different
# valid time ranges and different value attributes.
#
#
module ActsAsBitemporal
  extend ActiveSupport::Concern  # XXX probably not needed

  # Columns to be managed by ActsAsBitemporal
  TemporalColumnNames = %w{vtstart_at vtend_at ttstart_at ttend_at}

  # Alias to clarify we aren't using Ruby's Range
  ARange = ActsAsBitemporal::Range

  adapter_name = (ENV['DB'] || ActiveRecord::Base.connection.adapter_name) rescue 'postgresql'

  if adapter_name =~/postgres/i
    # The timestamp used to signify an indefinite end of a time period.
    Infinity  = DateTime::Infinity.new #Time.utc(9999,12,31).in_time_zone
    InfinityLiteral = 'infinity'
    InfinityValue = Float::INFINITY

    # The timestamp used to signify an indefinite start of a time period.
    Ninfinity = -Infinity #Time.utc(1000,12,31).in_time_zone
    NinfinityLiteral = '-infinity'
    NinfinityValue = -InfinityValue
  else
    # The timestamp used to signify an indefinite end of a time period.
    Infinity  = Time.utc(9999,12,31).in_time_zone
    InfinityLiteral = Infinity
    InfinityValue = Infinity

    # The timestamp used to signify an indefinite start of a time period.
    Ninfinity = Time.utc(1000,12,31).in_time_zone
    NinfinityLiteral = Ninfinity
    NinfinityValue = Ninfinity
  end

  # A Range that represents all time.
  AllTime         = ARange[Ninfinity, Infinity]

  def inspect
    "id: #{id}, vt:#{vt_range.inspect}, tt:#{tt_range.inspect}, scope: #{attributes.slice(*self.class.bt_scope_columns).map { |k,v| "#{k}: #{v}"}.join(' ')}"
  end

  # Returns versions of this record with bitemporal scope that intersects
  # with the specified bitemporal scope.
  #
  #    bt_history                     # => returns all versions of this record that are active
  #    bt_history(Time.zone.now)      # => returns all versions of this record that are active and valid now
  #
  #    bt_history(Time.zone.now...(Time.zone.now + 30.days))
  #      # => returns all versions of this record that are active and valid within the next 30 days
  #
  #    bt_history((Time.zone.now...Time.zone.now + 30.days), "2011-01-01")
  #      # => returns all versions of this record that were active on January 1st, 2011 and are valid within the next 30 days
  def bt_history(vtparams=AllTime, ttparams=nil)
    if ttparams
      bt_versions.vt_intersect(vtparams).tt_intersect(ttparams).order(:vtstart_at)
    else
      bt_versions.vt_intersect(vtparams).tt_forever.order(:vtstart_at)
    end
  end

  # Return relation that evalutes to all versions (identical key attributes)
  # of the current record.
  def bt_versions
    self.class.where(bt_scope_conditions)
  end

  # Returns valid time period represented as an ActsAsBitemporal::Range.
  def vt_range
    ARange.new(vtstart_at, vtend_at)
  end

  # Returns transaction time period represented as an ActsAsBitemporal::Range.
  def tt_range
    ARange.new(ttstart_at, ttend_at)
  end

  # Returns true if the transaction period intersects with the instant
  # or period specified by the arguments.
  #
  #     tt_intersects?(Time.zone.now)
  #     tt_intersects?(Time.zone.now, Time.zone.now + 60)
  #     tt_intersects?(ARange.new(Time.zone.now, Time.zone.now + 60))
  def tt_intersects?(*args)
    tt_range.intersects?(*args)
  end

  # Returns true if the valid time period intersects with the instant
  # or period specified by the arguments.
  #
  #     vt_intersects?(Time.zone.now)
  #     vt_intersects?(Time.zone.now, Time.zone.now + 60)
  #     vt_intersects?(ARange.new(Time.zone.now, Time.zone.now + 60))
  def vt_intersects?(*args)
    vt_range.intersects?(*args)
  end

  # Returns true if the record is active (i.e. transaction period is open).
  def tt_forever?
    ttend_at == InfinityValue
  end
  alias active? tt_forever?

  # Returns true if the record is inactive (i.e. transaction period is closed).
  def inactive?
    not active?
  end

  # Returns true if the valid period is open ended.
  def vt_forever?
    vtend_at == InfinityValue
  end

  # Returns true if the transaction and valid periods are both open ended.
  def bt_forever?
    vt_forever? and tt_forever?
  end

  # Returns true if the transaction period covers the current time.
  def tt_current?
    tt_intersects?(Time.zone.now)
  end

  # Returns true if the valid period covers the current time.
  def vt_current?
    vt_intersects?(Time.zone.now)
  end

  # Returns true if the valid period and transaction period covers the current time.
  def bt_current?
    now = Time.zone.now
    vt_intersects?(now) and tt_intersects?(now)
  end

  def bt_ensure_timestamps
    transaction_time = ttstart_at || Time.zone.now

    self.ttstart_at ||= transaction_time
    self.ttend_at ||= InfinityLiteral

    self.vtstart_at ||= transaction_time
    self.vtend_at ||= InfinityLiteral
  end

  # Bitemporal Equality Tests
  #
  # same object                               equal?
  # same ActiveRecord id                      ==
  # same scope, values, timestamp             bt_equal?
  # same scope, values, vtrange               bt_same_snapshot?
  # same scope, values                        bt_same_value?
  # same scope                                bt_same_scope?

  # Returns true if the scope attributes of this record are equal (==) to
  # the scope attributes of other record. This test ignores differences
  # between versioned attributes, temporal attributes, and the primary id
  # column.
  def bt_same_scope?(other)
    bt_scope_attributes == other.bt_scope_attributes
  end

  # Returns true if the scope and versioned attributes are equal (==) to
  # the attributes of other record. This test ignores differences
  # between temporal attributes and the primary id column.
  def bt_same_value?(other)
    bt_value_attributes == other.bt_value_attributes
  end

  # Returns true if the two records represent an identical snapshot. That is,
  # the scope, versioned, and valid time attributes are equal (==) to the
  # attributes of other record. This test ignores differences between
  # transaction time attributes and the primary id column.
  def bt_same_snapshot?(other)
    bt_snapshot_attributes == other.bt_snapshot_attributes
  end

  # Commit the record as a new version for this scope.
  #
  # If the record is a new record, it is inserted into the table as long as
  # its valid time range doesn't conflict with any existing records.
  #
  # If the record is a modification of an existing record, the changes
  # are applied as an update to all records that are covered by the valid
  # time range.
  #
  # If commit_time is provided it is used as the ttstart_at time for a
  # new record.  It is ignored for updates.
  def bt_commit(commit_time=nil)
    if new_record?
      self.ttstart_at = commit_time
      self.save ? [self] : []
    else
      bt_revise
    end
  end

  # Logically delete (finalize) the versions of this record that match the
  # specified temporal scope.
  #
  #   bt_delete                      # [current range, now]
  #   bt_delete(vt_range)            # [vt_range, now]
  #   bt_delete(vt_range, tt_range)  # [vt_range, tt_range]
  #   bt_delete(start, end)          # [start...end, now]
  #   bt_delete(start, end, time)    # [start...end, time]
  #
  # If no block is given, returns array of records that were finalized.
  #
  # If a block is given, the block is called once for each record that
  # is finalized and the return values from these calls is returned as an array.
  # The block is passed the finalized record, the valid time range that
  # is being finalized and the commit time for the transaction.
  #
  #   bt_delete { |record, vt_range, commit_time| .. }
  def bt_delete(*args)
    delete_vt_range, commit_time = bt_coerce_slice(*args)
    ActiveRecord::Base.transaction do
      bt_history(delete_vt_range).lock(true).map do |overlap|
        overlap.bt_finalize(commit_time)

        overlap.vt_range.difference(delete_vt_range).each do |segment|
          bt_new_version(vtstart_at: segment.db_begin, vtend_at: segment.db_end).bt_commit(commit_time)
        end

        (block_given? && yield(overlap, delete_vt_range, commit_time)) || overlap
      end
    end.tap do
      # Clean up in memory version...a bit. May be misleading if entire range wasn't removed.
      self.ttend_at = commit_time if vt_range.intersects?(delete_vt_range)
    end
  end

  # Create a new (unsaved) version of the record. Updated attributes can
  # be specified. Only versioned and temporal attributes can be modified. All
  # other attributes are replicated from the existing record.
  def bt_new_version(attributes={})
    self.class.new(bt_value_attributes) do |rec|
      rec.bt_attributes = attributes
      rec.vtstart_at ||= vtstart_at
      rec.vtend_at   ||= vtend_at
    end
  end

  # Mark the current record as finalized by updating the ttend_at timestamp.
  # The record is only updated if the in-memory copy is unchanged and active.
  #
  # Returns true if the database record was updated and false if the update
  # failed because the record had already been finalized.
  def bt_finalize(commit_time=Time.zone.now)
    if !changed? and active?
      not self.class.where(id: id, ttend_at: InfinityLiteral).update_all(ttend_at: commit_time).zero?
    else
      raise ArgumentError, "invalid finalization of modified or finalized record"
    end
  end

  # Revise this record by finalizing the current version and saving the new version.
  # An array of new records are returned. When only non-temporal attributes are
  # revised, the array will contain just a single record.
  #
  #     bt_revise(attr1: 'new value')
  # XXX Should detect fragmented period and coalece in revision.
  #
  # When the proposed revision has a vt range that overlaps one or more existing
  # records, those records are also finalized and revised but their own vt periods
  # are retained. bt_revise preserves the existing valid time periods -- it will
  # not create a new record with a valid time range that covers a previously
  # invalid time.
  def bt_revise(attrs={})
    raise ArgumentError, "invalid revision of non-current record" if inactive?

    revision = bt_new_version(attrs)

    return [] if !changed? and revision.bt_same_snapshot?(self)

    result = bt_delete(revision.vtstart_at, revision.vtend_at) do |overlapped, vtrange, transaction_time|
      intersection = overlapped.vt_range.intersection(vtrange)
      revision.bt_new_version(vtstart_at: intersection.db_begin, vtend_at: intersection.db_end).bt_commit(transaction_time).first
    end

    if result.empty?
      # The revised record doesn't intersect with any existing records (including its previous version).
      revision.bt_commit
      result << revision
    end

    result
  end

  # Returns hash of the four temporal attributes.
  def bt_temporal_attributes
    attributes.slice(*TemporalColumnNames)
  end

  # Returns attribute hash including just the scoped attributes.
  def bt_scope_attributes
    attributes.slice(*self.class.bt_scope_columns)
  end

  # Returns attribute hash including just the versioned attributes (i.e., neither scoped nor temporal).
  def bt_versioned_attributes
    attributes.slice(*self.class.bt_versioned_columns)
  end

  # Returns attribute hash excluding the primary key and the four temporal attributes.
  def bt_value_attributes
    attributes.slice(*(self.class.bt_scope_columns + self.class.bt_versioned_columns))
  end

  # Returns attribute hash excluding the primary key and the transaction time attributes.
  def bt_snapshot_attributes
    attributes.tap { |a| a.delete('id'); a.delete('ttstart_at'); a.delete('ttend_at') }
  end

  # Returns attribute hash merged with other hash. Temporal attributes are excluded.
  #   bt_attributes_merge(column: "new value")   # => Hash
  def bt_attributes_merge(updates)
    updates = updates.stringify_keys

    bt_value_attributes.merge( updates.slice(*self.class.bt_versioned_columns) )
  end

  def bt_attributes=(changes)
    self.attributes = changes.stringify_keys.slice(*self.class.bt_nonkey_columns)
  end

  private

  # Arel expresstion to select records with same key attributes as this record.
  def bt_scope_conditions
    table = self.class.arel_table
    self.class.bt_scope_columns.map do |key_attr|
      table[key_attr].eq(self[key_attr])
    end.inject do |memo, condition|
      memo.and(condition)
    end
  end

  # Does this record temporally intersect with an existing version of this record?
  def bt_scope_constraint_violation?
    bt_history(*bt_coerce_slice(vtstart_at, vtend_at, ttstart_at)).exists?
  end

  # The new record can not have a valid time period that overlaps
  # with any existing record for the same entity.
  def bt_scope_constraint
    if bt_scope_constraint_violation?
      if $DEBUG
        errors[:base] << "overlaps existing valid record: #{bt_versions.vt_intersect(vtstart_at, vtend_at).tt_intersect(ttstart_at).to_a.inspect}"
      else
        errors[:base] << "overlaps existing valid record"
      end
      false
    else
      true
    end
  end

  def bt_guard_save
    if !new_record? and !bt_safe?
      errors[:base] << "invalid use of save on temporal records"
      false
    else
      true
    end
  end

  def bt_after_commit
    self.bt_safe = false
  end

  # Coerce arguments to a standard format for a slice of valid time records
  # represented by a valid time range and a transaction time instant.
  #
  #   bt_coerce_slice                      # [vt_current, now]
  #   bt_coerce_slice(vt_range)            # [vt_range, now]
  #   bt_coerce_slice(vt_range, tt_range)  # [vt_range, tt_range]
  #   bt_coerce_slice(start, end)          # [start...end, now]
  #   bt_coerce_slice(start, end, time)    # [start...end, time]
  def bt_coerce_slice(*args)
    case args.size
    when 0
      [vt_range, Time.zone.now]
    when 1
      [ARange[*args], Time.zone.now]
    when 2
      case args.first
      when Range
        args
      else
        [ARange[*args], Time.zone.now]
      end
    when 3
      [ARange[args.at(0),args.at(1)], args.at(2)]
    else
      raise ArgumentError
    end
  end

  # Used internally to prevent accidental use of AR methods that don't ensure bitemporal semantics.
  def bt_safe?
    @bt_safe
  end

  module ClassMethods

    def bt_nonkey_columns
      bt_versioned_columns + TemporalColumnNames
    end

    def sifter_bt_constraint(vtstart, vtend, ttstart, ttend)
      squeel do
        (ttstart_at == nil) |
          ((vtstart_at < vtend) & (vtend_at > vtstart) &
           (ttstart_at < ttend) & (ttend_at > ttstart))
      end
    end

    # Generate arel expression that evaluates to true if the period specified by
    # _start_column_ and _end_column_ intersects with the instant or period. All
    # periods are considered half-open: [closed, open).
    #   arel_intersect(:vtstart_at, :vtend_at, Time.zone.now)
    #   arel_intersect(:ttstart_at, :ttend_at, Time.zone.parse("2014-01-01"), Time.zone.parse("2015-01-01"))
    def arel_intersect(start_column, end_column, start_or_instant_or_range=nil, range_end=nil)
      table = self.arel_table
      if range_end
        table[start_column].lt(range_end).and(table[end_column].gt(start_or_instant_or_range))
      elsif Range === start_or_instant_or_range
        table[start_column].lt(start_or_instant_or_range.db_end).and(table[end_column].gt(start_or_instant_or_range.db_begin))
      else
        start_or_instant_or_range ||= InfinityLiteral
        table[start_column].lteq(start_or_instant_or_range).and(table[end_column].gt(start_or_instant_or_range))
      end
    end

    # Generate arel expression for intersection with valid time period.
    def arel_vt_intersect(instant, range_end)
      arel_intersect(:vtstart_at, :vtend_at, instant, range_end)
    end

    # Generate arel expression for intersection with transaction time period.
    def arel_tt_intersect(instant, range_end)
      arel_intersect(:ttstart_at, :ttend_at, instant, range_end)
    end

    # Generate arel expression for intersection with valid and transaction time periods.
    def arel_bt_intersect(*args)
      arel_vt_intersect(args.at(0), args.at(1)).and(arel_tt_intersect(args.at(2), args.at(3)))
    end

    # AR relation where condition for valid time intersection. Selects all record
    # that have a valid period that intersects the instant or period provided.
    #   vt_intersect                # => selects records valid now.
    #   vt_intersect("2013-01-01")  # => selects records valid on January 1, 2013 at 00:00:00.
    #   vt_intersect("2013-01-01", "2013-01-02")  # => selects records valid on January 1, 2013 between 00:00:00 and 24:00:00.
    def vt_intersect(instant=Time.zone.now, range_end=nil)
      where(arel_vt_intersect(instant, range_end))
    end

    # AR relation where condition for transaction time intersection. Selects all record
    # that have a transaction period that intersects the instant or period provided.
    #   tt_intersect                # => selects records active now.
    #   tt_intersect("2013-01-01")  # => selects records active on January 1, 2013 at 00:00:00.
    #   tt_intersect("2013-01-01", "2013-01-02")  # => selects records active on January 1, 2013 between 00:00:00 and 24:00:00.
    def tt_intersect(instant=Time.zone.now, range_end=nil)
      where(arel_tt_intersect(instant, range_end))
    end

    # AR relation where condition for bitemporal time intersection. Selects all record
    # that have valid time and transaction time periods that intersects the instant or period provided.
    #   bt_intersect                # => selects records valid and active now.
    #   tt_intersect("2013-01-01")  # => selects records active on January 1, 2013 at 00:00:00.
    #   bt_intersect("2013-01-01", "2013-01-02")  # => selects records valid on Jan 1 at midnight but not known until Jan 2 at midnight.
    #   bt_intersect("2013-01-01", "2013-01-02", "2013-02-01", InfinityLiteral)
    #       # => selects records valid on Jan 1st but not known until after Feb 1 at midnight.
    #   bt_intersect(bt_record)     # => selects records valid and active while bt_record is also valid and active.
    def bt_intersect(*args)
      where(arel_bt_intersect(*bt_temporal(*args)))
    end

    # Coerce temporal arguments into a 4-tuple: valid_start, valid_end, transaction_start, transaction_end
    #   bt_temporal                           # => now, now, now, now
    #   tt_temporal(t1)                       # => t1, t1, now, now
    #   bt_temporal(t1, t2)                   # => t1, t1, t2, t2
    #   bt_temporal(t1, t2, t3, t4)           # => t1, t2, t3, t4
    #   bt_temporal(r1)                       # => r1.begin, r1.end, now, now
    #   bt_temporal(t1, r2)                   # => t1, t1, r2.begin, r2.end
    #   bt_temporal(r1, t2)                   # => r1.begin, r1.end, t2, t2
    def bt_temporal(*args)
      case args.count
      when 0
        instant = Time.zone.now
        bt_temporal(instant, instant)
      when 1
        case instant = args.first
        when ActsAsBitemporal
          return instant.bt_temporal_attributes.values
        else
          return bt_temporal(instant, Time.zone.now)
        end
      when 2
        return [*bt_temporal_limits(args.at(0)), *bt_temporal_limits(args.at(1))]
      when 4
        return args
      end
    end

    # Coerce an instance or a range into start and end points.
    def bt_temporal_limits(instant_or_range)
      case instant_or_range
      when ::Range, ARange
        return instant_or_range.begin, instant_or_range.end
      else
        return instant_or_range, instant_or_range
      end
    end

    # Selects records valid right now (active or inactive). The result can be
    # considered an audit trail of the record showing all the changes that
    # have been recorded in the table along the transaction time axis.
    def vt_current
      vt_intersect()
    end

    # Selects records active right now (valid or not). The result can be
    # considered a history of the real world record showing changes that
    # have been recorded in the table along the valid time axis.
    def tt_current
      tt_intersect()
    end

    # Selects records valid and active right now.
    def bt_current(instant=Time.zone.now)
      vt_intersect(instant).tt_intersect(instant)
    end

    def bt_current!
      bt_current.first!
    end

    def vt_forever
      where(:vtend_at => InfinityLiteral)
    end

    def tt_forever
      where(:ttend_at => InfinityLiteral)
    end

    Tokens = ('A'..'Z').to_a.join
    def bt_ascii(detail=false)
      final = ""

      result = order(bt_scope_columns)
      records = result.group_by { |r| r.bt_scope_attributes }
      vt_ticks = records.map { |scope, list| list.map { |x| [x.vtstart_at, x.vtend_at]} }.flatten.uniq.sort { |a,b| Range.compare(a,b) }

      row = 0
      records.each_with_index do |(scope, list), index|
      tt_ticks = list.map { |x| [x.ttstart_at, x.ttend_at]}.flatten.uniq.sort { |a,b| Range.compare(a,b) }
      picture = Array.new(tt_ticks.size) { " " * vt_ticks.size }

      list.sort { |a,b| Range.compare(a.ttstart_at, b.ttstart_at) }.each_with_index do |record, version|
        vstart = vt_ticks.index(record.vtstart_at)
        vend   = vt_ticks.index(record.vtend_at)
        tstart = tt_ticks.index(record.ttstart_at)
        tend   = tt_ticks.index(record.ttend_at)
        #warn "start,end,tstart,tend,len = #{[row, vstart, vend, tstart, tend,len = (vend - vstart + 1), version.to_s * len].inspect}"

        (tstart..tend).each do |tindex|
          span = Tokens[version] * (vend - vstart + 1)

          picture[tindex][vstart..vend] = span
        end
      end
      final << picture.each_with_index.map { |row, rindex| "%d%s: %s" % [index, detail ? tt_ticks[rindex] : "", row] }.join("\n")
      final << "\n"
      end
      final
    end

    # Verify bitemporal key constraints
    def bt_scope_constraint_table
      n = Name.arel_table
      n1 = n.alias('n1')
      n2 = n.alias('n2')
      subquery = n.from(n2).project(n2[:entity_id].count).
        where(n1[:entity_id].eq(n2[:entity_id])).
        where(n1[:vtstart_at].lt(n2[:vtend_at])).
        where(n2[:vtstart_at].lt(n1[:vtend_at])).
        where(n1[:ttend_at].eq( InfinityLiteral)).
        where(n2[:ttend_at].eq( InfinityLiteral))
      subquery2 = n.from(n1).project(n1[:entity_id], n1[:vtstart_at], n1[:vtend_at], n1[:ttend_at]).where(Arel::SqlLiteral.new("(#{subquery.to_sql})").gt(1))
      ActiveRecord::Base.connection.execute("select #{subquery2.exists.not.to_sql}").values == "t"
    end


  end

  module AssociationMethods
    def bt_build(attrs={})
      transaction_time = Time.zone.now
      build(attrs.reverse_merge(
        vtstart_at: nil, vtend_at: InfinityLiteral,
        ttstart_at: nil, ttend_at: InfinityLiteral
      ))
    end
  end

  module TableDefinitionHelper
    def bt_timestamps(options={})
      column(:vtstart_at, :timestamp, options)
      column(:vtend_at, :timestamp, options)
      column(:ttstart_at, :timestamp, options)
      column(:ttend_at, :timestamp, options)
    end
  end

  class ActiveRecord::ConnectionAdapters::TableDefinition
    include TableDefinitionHelper
  end

end

class << ActiveRecord::Base

  # Enable bitemporal controls on this table. By default, bitemporal
  # constraints will be scoped by any foreign keys in the table,
  # which are detected by looking for column names ending in '_id'.
  #
  # The scope can be changed from the default with the following
  # options:
  #
  #     :scope => [:col1, :col2]  # sets scope to named columns
  #     :for => Model             # sets scope to foreign_key for Model
  #
  # A model configured with acts_as_bitemporal will have the following
  # additional class methods:
  #
  # bt_value_columns        # the columns considered when versioning the model
  # bt_scope_columns        # the columns that uniquely identify the model scope
  # bt_versioned_columns    # the value columns with bt_scope_columns excluded
  #
  # The 'id' and 'type' columns are ignored by acts_as_bitemporal.
  #
  # The normal ActiveRecord timestamp columns should not be defined on
  # an acts_as_bitemporal table. Instead the bitemporal timestamps should
  # be defined and are # maintained by acts_as_bitemporal:
  #
  #   vtstart_at
  #   vtend_at
  #   ttstart_at
  #   ttend_at
  #
  # The table definition helper, bt_timestamps, is provided to easily add
  # these timestamps.
  #
  # include ActsAsBitemporal instance methods
  # define scope
  #     default (_id)
  #     for
  #     scope
  def acts_as_bitemporal(*args)
    options = args.extract_options!
    bt_exclude_columns = %w{id type}    # AR maintains these columns

    include ActsAsBitemporal

    class_attribute :bt_scope_columns
    class_attribute :bt_versioned_columns
    class_attribute :bt_value_columns

    if bt_belongs_to = options.delete(:for)
      self.bt_scope_columns = [bt_belongs_to.to_s.foreign_key] # Entity => entity_id
    end

    if self.bt_scope_columns = options.delete(:scope)
      self.bt_scope_columns = Array(bt_scope_columns).map(&:to_s)
    else
      self.bt_scope_columns = self.column_names.grep /_id\z/
    end

    self.bt_value_columns = self.column_names - ActsAsBitemporal::TemporalColumnNames - bt_exclude_columns
    self.bt_versioned_columns = self.bt_value_columns - bt_scope_columns

    attr_accessor :bt_safe

    before_validation :bt_ensure_timestamps
    validate          :bt_scope_constraint, :on => :create
    before_save       :bt_guard_save
    after_commit      :bt_after_commit

  end

  def has_many_bitemporal(collection, options={})

    if !respond_to?(:bt_attributes)
      class_attribute :bt_attributes
      self.bt_attributes = {}
    end

    collection = collection.to_s.pluralize
    singular_sym = collection.singularize.to_sym
    plural_sym = collection.to_sym

    info = bt_attributes[collection.to_sym] = {
      :type => :collection,
      :class_name => options.delete(:class_name) || collection.classify,
    }

    options = { extend: ActsAsBitemporal::AssociationMethods }.merge( class_name: info[:class_name] )
    has_many plural_sym, options

    define_method("bt_#{collection}") do |*args|
      send(plural_sym).bt_intersect(*args)
    end
  end

  # Define an associated record that is versioned bitemporaly.
  #
  #     has_one_bitemporal :name
  def has_one_bitemporal(attribute, options={})

    if !respond_to?(:bt_attributes)
      class_attribute :bt_attributes
      self.bt_attributes = {}
    end

    singular      = attribute.to_s.singularize
    singular_sym  = singular.to_sym
    plural_sym    = singular.pluralize.to_sym
    through = options.delete(:through)
    class_name = options.delete(:class_name)
    assignments = through.to_s.pluralize.to_sym

    info = bt_attributes[singular_sym] = {
      :type => :scalar,
      :class_name => class_name || singular.classify,
      :through => through,
      :expose => options.delete(:expose) || [],
      :through_class_name => through.to_s.classify,
    }

    if through
      has_one_bitemporal assignments
      has_many plural_sym, through: assignments

      define_method("bt_#{plural_sym}".to_sym) do |*args|
        info[:class] ||= info[:class_name].constantize
        info[:through_class] ||= info[:through_class_name].constantize

        send(plural_sym).merge(info[:through_class].bt_intersect(*args))
      end

      define_method("bt_#{singular_sym}") { |*args| send("bt_#{plural_sym}", *args).first }
      define_method("bt_#{singular_sym}!") { |*args| send("bt_#{plural_sym}", *args).first! }

    else

      attr_list = info[:expose]
      options = { extend: ActsAsBitemporal::AssociationMethods }.merge( class_name: info[:class_name] )

      has_many plural_sym, options

      after_method = "bt_after_create_#{singular}"
      after_create after_method.to_sym

      define_method(after_method) do
        info[:class] ||= info[:class_name].constantize
        if !attr_list.empty?
          attributes = Hash[ attr_list.map { |a| [a, send("bta_#{singular_sym}_#{a}")] } ]
          # things         << (thing              ||        Thing.new( :thing_attr0 => bta_thing_attr0, :thing_attr1 => bta_thing_attr1)
          raise ActiveRecord::Rollback unless send(plural_sym).push((send(singular_sym) || info[:class].new(attributes)))
        end
      end

      define_method("bt_#{plural_sym}") { |*args| send(plural_sym).bt_intersect(*args) }
      define_method("bt_#{singular_sym}") { |*args| send("bt_#{plural_sym}", *args).first }
      define_method("bt_#{singular_sym}!") { |*args| send("bt_#{plural_sym}", *args).first! }

      attr_accessor singular_sym
      attr_list.each do |attr|
        setter = "bta_#{singular_sym}_#{attr}"
        attr_accessor setter
        define_method("#{attr}") { send(setter) }
        define_method("#{attr}=") {|value| send("#{setter}=", value)}
      end
    end
  end
end
