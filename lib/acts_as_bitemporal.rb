# encoding: utf-8
require "acts_as_bitemporal/version"
require "acts_as_bitemporal/version"
require "acts_as_bitemporal/range"
require 'active_support'
require 'active_support/time'
require 'active_record'

module ActsAsBitemporal
  extend ActiveSupport::Concern

  # Columns to be managed by ActsAsBitemporal
  TemporalColumnNames = %w{vtstart_at vtend_at ttstart_at ttend_at}
  # Alias to clarify we aren't using Ruby's Range
  ARange = ActsAsBitemporal::Range    

  # The timestamp used to signify an indefinite end of a time period.
  Forever         = Time.utc(9999,12,31).in_time_zone
  NegativeForever = Time.utc(1000,12,31).in_time_zone
  AllTime         = ARange[NegativeForever, Forever]

  T = ->(t) { t ? t.strftime("%c %N %z") : "Forever" }
  def inspect

    "id: #{id}, vt:#{T[vtstart_at]}..#{T[vtend_at]}, tt:#{T[ttstart_at]}..#{T[ttend_at]}, scope: #{self[self.class.bt_scope_columns.first]}"
  end

  # Returns versions of this record satisfying various bitemporal constraints.
  def bt_history(vtparams, ttparams=nil)
    if ttparams
      bt_versions.vt_intersect(vtparams).tt_intersect(ttparams).order(:vtstart_at)
    else
      bt_versions.vt_intersect(vtparams).tt_forever.order(:vtstart_at)
    end
  end

  # Coerce arguments to a standard format for a slice of valid time records
  # represented by a valid time range and a transaction time instant.
  #
  #   bt_coerce_slice                      # [AllTime, now]
  #   bt_coerce_slice(vt_range)            # [vt_range, now]
  #   bt_coerce_slice(vt_range, tt_range)  # [vt_range, tt_range]
  #   bt_coerce_slice(start, end)          # [start...end, now]
  #   bt_coerce_slice(start, end, time)    # [start...end, time]
  def bt_coerce_slice(*args)
    case args.size
    when 0
      [AllTime, Time.zone.now]
    when 1
      [ARange[*args], Time.zone.now]
    when 2
      case args.first
      when Range
        [args.first, args.last]
      else
        [ARange[*args], Time.zone.now]
      end
    when 3
      [ARange[args.at(0),args.at(1)], args.at(2)]
    else
      raise ArgumentError
    end
  end
  
  def bt_scope_constraint_violation?
    bt_history(*bt_coerce_slice(vtstart_at, vtend_at, ttstart_at)).exists?
  end

  # The new record can not have a valid time period that overlaps 
  # with any existing record for the same entity.
  def bt_scope_constraint
    if !new_record? and !bt_safe?
      errors[:base] << "invalid use of save on temporal records"
    elsif bt_scope_constraint_violation?
      if $DEBUG
        errors[:base] << "overlaps existing valid record: #{bt_versions.vt_intersect(vtstart_at, vtend_at).tt_intersect(ttstart_at).to_a.inspect}"
      else
        errors[:base] << "overlaps existing valid record"
      end
    end
  end

  def bt_after_commit
    self.bt_safe = false
  end

  # Return relation that evalutes to all versions (identical key attributes)
  # of the current record.
  def bt_versions
    self.class.where(bt_scope_conditions)
  end

  # Arel expresstion to select records with same key attributes as this record.
  def bt_scope_conditions
    table = self.class.arel_table
    self.class.bt_scope_columns.map do |key_attr| 
      table[key_attr].eq(self[key_attr]) 
    end.inject do |memo, condition| 
      memo.and(condition)
    end
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
  #  tt_intersects?(Time.zone.now)
  #  tt_intersects?(Time.zone.now, Time.zone.now + 60)
  #  tt_intersects?(ARange.new(Time.zone.now, Time.zone.now + 60))
  def tt_intersects?(*args)
    tt_range.intersects?(*args)
  end

  # Returns true if the valid time period intersects with the instant
  # or period specified by the arguments.
  #  vt_intersects?(Time.zone.now)
  #  vt_intersects?(Time.zone.now, Time.zone.now + 60)
  #  vt_intersects?(ARange.new(Time.zone.now, Time.zone.now + 60))
  def vt_intersects?(instant_or_range, end_of_range=nil)
    instant_or_range = ARange.new(instant_or_range, end_of_range) if end_of_range
    vt_range.intersects?(instant_or_range)
  end

  # Returns true if the transaction period is open ended.
  def tt_forever?
    ttend_at == Forever
  end

  # Returns true if the valid period is open ended.
  def vt_forever?
    vtend_at == Forever
  end

  # Returns true if the transaction and valid periods are both open ended.
  def forever?
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

  def complete_bt_timestamps
    transaction_time = Time.zone.now
    self.vtstart_at ||= transaction_time
    self.ttstart_at ||= transaction_time
    self.vtend_at ||= Forever
    self.ttend_at ||= Forever
  end

  # Returns true if the non-temporal attributes of this object are equal to the
  # non-temporal attributes of other record.  This is test for value equality.
  def bt_equal?(other)
    bt_nontemporal_attributes == other.bt_nontemporal_attributes
  end

  # Pushes changes to the database respecting bitemporal semantics.
  def bt_save(*args)
    if new_record?
      save(*args)
    elsif vtstart_at_changed? or vtend_at_changed?
      bt_revise(bt_nonkey_attributes)
    else
      bt_update_attributes( Hash[changes.map { |k,(oldv, newv)| [k,newv] }] )
    end
  end

  # Rewrite records within specified vtrange.
  #
  #  bt_delete
  #
  #  Returns array of records that were finalized.
  def bt_delete(*args)
    vt_range, commit_time = bt_coerce_slice(*args)
    ActiveRecord::Base.transaction do
      bt_history(vt_range).lock(true).map do |overlap|
        overlap.bt_finalize(commit_time)

        overlap.vt_range.difference(vt_range).each do |segment|
          bt_dup(segment.begin, segment.end).bt_commit(commit_time)
        end

        (block_given? && yield(overlap, vt_range, commit_time)) || overlap
      end
    end.tap do
      self.ttend_at = commit_time 
    end
  end

  # Duplicate the existing record but configure with new valid time range.
  def bt_dup(vt_start=vtstart_at, vt_end=vtend_at, attributes={})
    self.class.new(bt_nontemporal_attributes) do |rec|
      rec.vtstart_at = vt_start
      rec.vtend_at = vt_end
      rec.bt_attributes = attributes
    end
  end

  def bt_commit(commit_time=nil)
    if new_record?
      self.ttstart_at = commit_time
      self.save!
      self
    elsif vtstart_at_changed? or vtend_at_changed?
      bt_revise
    elsif changed?
      bt_update_attributes( Hash[changes.map { |k,(oldv, newv)| [k,newv] }] )
    end
  end

  def bt_finalize(commit_time=Time.zone.now)
    update_column(:ttend_at, commit_time)
  end

  def bt_update_attributes(changes)
    return unless tt_forever?

    updated = bt_dup
    updated.bt_attributes = changes
    return unless changed? or bt_nontemporal_attributes != updated.bt_nontemporal_attributes

    bt_delete(vtstart_at, vtend_at) do |overlapped, vtrange, commit|
      updated.bt_commit(commit)
    end.last
  end

  def bt_revise(attrs={})
    attrs = attrs.stringify_keys
    revision = bt_dup(attrs['vtstart_at'], attrs['vtend_at'], attrs)

    raise ArgumentError, "invalid revision of non-current record" unless tt_forever? or ttend_at.nil?
    bt_delete(revision.vtstart_at, revision.vtend_at) do |overlapped, vtrange, transaction_time|
      intersection = overlapped.vt_range.intersection(revision.vtstart_at, revision.vtend_at)
      revision.bt_dup(intersection.begin, intersection.end).bt_commit(transaction_time)
    end
  end

  # Returns hash of the four temporal attributes.
  def bt_temporal_attributes
    attributes.slice(*TemporalColumnNames)
  end

  # Returns attribute hash excluding the four temporal attributes.
  def bt_nontemporal_attributes
    attributes.slice(*(self.class.bt_scope_columns + self.class.bt_versioned_columns))
  end

  # Returns attribute hash including just the scoped attributes.
  def bt_scope_attributes
    attributes.slice(*self.class.bt_scope_columns)
  end

  # Returns attribute hash including just the versioned attributes (i.e., neither scoped nor temporal).
  def bt_versioned_attributes
    attributes.slice(*self.class.bt_versioned_columns)
  end

  # Returns attribute hash including excluding the primary keys.
  def bt_nonkey_attributes
    attributes.slice(*(self.class.bt_versioned_columns + TemporalColumnNames))
  end

  # Returns attribute hash merged with other hash. Temporal attributes are excluded.
  #   bt_attributes_merge(column: "new value")   # => Hash
  def bt_attributes_merge(updates)
    updates = updates.stringify_keys

    bt_nontemporal_attributes.merge( updates.slice(*self.class.bt_versioned_columns) )
  end

  def bt_attributes=(changes)
    self.attributes = changes.stringify_keys.slice(*self.class.bt_versioned_columns)
  end

  private 

  # Used internally to prevent accidental use of AR methods that don't ensure bitemporal semantics.
  def bt_safe?
    @bt_safe
  end

  module ClassMethods
    # Generate arel expression that evaluates to true if the period specified by
    # _start_column_ and _end_column_ intersects with the instant or period. All
    # periods are considered half-open: [closed, open).
    #   arel_intersect(:vtstart_at, :vtend_at, Time.zone.now)
    #   arel_intersect(:ttstart_at, :ttend_at, Time.zone.parse("2014-01-01"), Time.zone.parse("2015-01-01"))
    def arel_intersect(start_column, end_column, start_or_instant_or_range, range_end=nil)
      table = self.arel_table
      if range_end
        table[start_column].lt(range_end).and(table[end_column].gt(start_or_instant_or_range))
      elsif Range === start_or_instant_or_range
        table[start_column].lt(start_or_instant_or_range.end).and(table[end_column].gt(start_or_instant_or_range.begin))
      else
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
    # that have a valid and transaction periods that intersects the instant or period provided.
    #   bt_intersect                # => selects records valid and active now.
    #   tt_intersect("2013-01-01")  # => selects records active on January 1, 2013 at 00:00:00.
    #   bt_intersect("2013-01-01", "2013-01-02")  # => selects records valid on Jan 1 at midnight but not known until Jan 2 at midnight.
    #   bt_intersect("2013-01-01", "2013-01-02", "2013-02-01", Forever)  
    #       # => selects records valid on Jan 1st but not known until after Feb 1 at midnight.
    #   bt_intersect(bt_record)     # => selects records valid and active while bt_record is also valid and active.
    def bt_intersect(*args)
      case args.count
      when 0
        bt_current
      when 1
        case instant = args.first
        when ActsAsBitemporal
          where(arel_bt_intersect( *args.first.bt_temporal_attributes.values ))
        else
          vt_intersect(instant).tt_intersect(instant)
        end
      when 2
        vt_intersect(args.at(0)).tt_intersect(args_at(1))
      when 4
        where(arel_bt_intersect(*args))
      end
    end

    # Selects records valid right now (active or inactive).
    def vt_current
      vt_intersect()
    end

    # Selects records active right now (valid or not).
    def tt_current
      tt_intersect()
    end

    # Selects records valid and active right now.
    def bt_current
      now = Time.zone.now
      vt_intersect(now).tt_intersect(now)
    end

    def bt_current!
      bt_current.first!
    end

    def vt_forever
      where(:vtend_at => Forever)
    end

    def tt_forever
      where(:ttend_at => Forever)
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
        where(n1[:ttend_at].eq( Forever)).
        where(n2[:ttend_at].eq( Forever))
      subquery2 = n.from(n1).project(n1[:entity_id], n1[:vtstart_at], n1[:vtend_at], n1[:ttend_at]).where(Arel::SqlLiteral.new("(#{subquery.to_sql})").gt(1))
      ActiveRecord::Base.connection.execute("select #{subquery2.exists.not.to_sql}").values == "t"
    end


  end

  module AssociationMethods
    def bt_build(attrs={})
      transaction_time = Time.zone.now
      build(attrs.reverse_merge(
        vtstart_at: nil, vtend_at: Forever, 
        ttstart_at: nil, ttend_at: Forever
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
  def acts_as_bitemporal(*args)
    options = args.extract_options!
    bt_exclude_columns = %w{id type}    # AR maintains these columns

    include ActsAsBitemporal

    class_attribute :bt_scope_columns
    class_attribute :bt_versioned_columns

    if bt_belongs_to = options.delete(:for)
      self.bt_scope_columns = [bt_belongs_to.foreign_key]     # Entity => entity_id
    elsif self.bt_scope_columns = options.delete(:scope)
      self.bt_scope_columns = Array(bt_scope_columns).map(&:to_s)
    else
      self.bt_scope_columns = self.column_names.grep /_id/
    end

    self.bt_versioned_columns = self.column_names - bt_scope_columns - ActsAsBitemporal::TemporalColumnNames - bt_exclude_columns

    attr_accessor :bt_safe

    after_commit      :bt_after_commit
    before_validation :complete_bt_timestamps
    validate          :bt_scope_constraint

  end
end
