# encoding: utf-8
require "acts_as_bitemporal/version"
require "acts_as_bitemporal/version"
require "acts_as_bitemporal/range"
require 'active_support'
require 'active_support/time'
require 'active_record'

module ActsAsBitemporal

  # Columns to be managed by ActsAsBitemporal
  TemporalColumnNames = %w{vtstart_at vtend_at ttstart_at ttend_at}

  # The timestamp used to signify an indefinite end of a time period.
  Forever = Time.utc(9999,12,31).in_time_zone

  ARange = ActsAsBitemporal::Range    # Alias to clarify we aren't using Ruby's Range

  extend ActiveSupport::Concern

  
  def bt_scope_constraint_violation?
    bt_versions.vt_intersect(vtstart_at, vtend_at).tt_intersect(ttstart_at).exists?
  end

  # The new record can not have a valid time period that overlaps with any existing record for the same entity.
  def bt_scope_constraint
    if !new_record? and !bt_safe?
      errors[:base] << "invalid use of save on temporal records"
    elsif bt_scope_constraint_violation?
      errors[:base] << "overlaps existing valid record"
    end
  end

  def bt_after_commit
    self.bt_safe = false
  end

  # Return relation that evalutes to all images of the current record.
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
  def tt_intersects?(instant_or_range, end_of_range=nil)
    instant_or_range = ARange.new(instant_or_range, end_of_range) if end_of_range
    tt_range.intersects?(instant_or_range)
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

  def bt_equal?(other)
    bt_nontemporal_attributes == other.bt_nontemporal_attributes
  end

  # Pushes changes to the database respecting bitemporal semantics.
  def bt_save(*args)
    if new_record?
      save(*args)
    elsif vtstart_at_changed? or vtend_at_changed?
      vt_revise(vtstart_at: vtstart_at, vtend_at: vtend_at)
    else
      bt_update_attributes( Hash[changes.map { |k,(oldv, newv)| [k,newv] }] )
    end
  end

  # Remove records from commit_time to vtend_at for this record.
  # Use commit_time as the transaction time if given.
  def bt_delete(commit_time=nil)
    commit_time ||= Time.zone.now
    result = bt_delete3(commit_time, vtend_at, commit_time)
    reload
    result
  end

  # Remove records associated with the vt_range for this record.
  # Use commit_time as the transaction time if given.
  def bt_delete2(commit_time=nil)
    return bt_delete3(vtstart_at, vtend_at, commit_time)
  end

  def bt_delete3(start_at, end_at, commit_time=nil)
    ActiveRecord::Base.transaction do
      commit_time ||= Time.zone.now
      bt_versions.tt_forever.vt_intersect(start_at, end_at).order(:vtstart_at).each do |existing|
        existing.bt_finalize(commit_time)
        existing.vt_range.difference(start_at, end_at).each do |segment|
          bt_dup(segment.begin, segment.end).bt_commit(commit_time)
        end
      end
    end
    commit_time
  end

  # Duplicate the existing record but configure with new valid time range.
  def bt_dup(vt_start=vtstart_at, vt_end=vtend_at)
    self.class.new(bt_nontemporal_attributes) do |rec|
      rec.vtstart_at = vt_start
      rec.vtend_at = vt_end
    end
  end

  def bt_commit(commit_time=nil)
    if new_record?
      self.ttstart_at = commit_time
      self.save
    end
  end

  def bt_finalize(commit_time=Time.zone.now)
    update_column(:ttend_at, commit_time)
  end

  def bt_update_attributes(changes)
    return unless tt_forever?

    ActiveRecord::Base.transaction do
      commit_time = Time.zone.now
      revision = bt_dup.tap do |rec|
        rec.bt_attributes = changes
      end

      return unless changed? or bt_nontemporal_attributes != revision.bt_nontemporal_attributes

      bt_finalize(commit_time)
      revision.bt_commit(commit_time)
      revision
    end
  end

  def vt_revise(attrs)
    revised_attrs = attrs.dup
    newstart  = (revised_attrs.delete(:vtstart_at) || vtstart_at).try(:to_time)
    newend    = (revised_attrs.delete(:vtend_at) || vtend_at).try(:to_time)

    raise ArgumentError, "invalid revision of non-current record" unless tt_forever?

    transaction_time = Time.zone.now
    ActiveRecord::Base.transaction do
      overlapped = self.class.tt_forever.vt_intersect(newstart, newend).lock(true).to_a
      bt_finalize(transaction_time) if overlapped.count > 0
      overlapped.each do |rec|
        if rec.vtstart_at < newstart
          self.class.create!(rec.bt_nontemporal_attributes.merge(vtstart_at: rec.vtstart_at, vtend_at: newstart, ttstart_at: transaction_time))
        end

        if newend <= rec.vtend_at
          self.class.create!(rec.bt_nontemporal_attributes.merge(vtstart_at: newend, vtend_at: rec.vtend_at, ttstart_at: transaction_time))
        end

        rec.bt_finalize(transaction_time)
      end
      self.class.create!(bt_nontemporal_attributes.merge(revised_attrs).merge(vtstart_at: newstart, vtend_at: newend, ttstart_at: transaction_time))
    end
  end

  def vt_revise2(attrs)
    revised_attrs = attrs.dup
    newstart  = (revised_attrs.delete(:vtstart_at) || vtstart_at).try(:to_time)
    newend    = (revised_attrs.delete(:vtend_at) || vtend_at).try(:to_time)
    return self if vt_range.covers?(newstart, newend) and revised_attrs.empty?

    revised_attrs = bt_nontemporal_attributes.merge!(revised_attrs)

    ActiveRecord::Base.transaction do
      commit_time = Time.zone.now
      if bt_scope_constraint_violation?
        bt_finalize(commit_time)
        new_period = vt_range.merge(newstart, newend)
        self.class.create(revised_attrs.merge(vtstart_at: new_period.begin, vtend_at: new_period.end, ttstart_at: commit_time))
      else
        self.class.create(revised_attrs.merge(vtstart_at: newstart, vtend_at: newend))
      end
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
    def arel_intersect(start_column, end_column, start_or_instant, range_end=nil)
      table = self.arel_table
      if range_end
        table[start_column].lt(range_end).and(table[end_column].gt(start_or_instant))
      else
        table[start_column].lteq(start_or_instant).and(table[end_column].gt(start_or_instant))
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
