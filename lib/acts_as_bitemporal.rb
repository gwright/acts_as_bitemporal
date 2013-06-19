# encoding: utf-8
require "acts_as_bitemporal/version"
require "acts_as_bitemporal/range"
require 'active_support/time'
require 'active_record'

module ActsAsBitemporal
  extend ActiveSupport::Concern  # XXX probably not needed

  # Columns to be managed by ActsAsBitemporal
  TemporalColumnNames = %w{vtstart_at vtend_at ttstart_at ttend_at}

  # Alias to clarify we aren't using Ruby's Range
  ARange = ActsAsBitemporal::Range

  # The timestamp used to signify an indefinite end of a time period.
  Forever         = Time.utc(9999,12,31).in_time_zone

  # The timestamp used to signify an indefinite start of a time period.
  NegativeForever = Time.utc(1000,12,31).in_time_zone

  # A Range that represents all time.
  AllTime         = ARange[NegativeForever, Forever]

  # A lambda to format timestamps.
  T = ->(t) { t ? t.strftime("%c %N %z") : "Forever" }

  def inspect
    "id: #{id}, vt:#{T[vtstart_at]}..#{T[vtend_at]}, tt:#{T[ttstart_at]}..#{T[ttend_at]}, scope: #{self[self.class.bt_scope_columns.first]}"
  end

  # Returns versions of this record that have a bitemporal scope that intersects
  # with the specified bitemporal scope.
  #
  #    bt_history                     # => returns all versions of this record that are active
  #    bt_history(Time.zone.now)      # => returns all versions of this record that are active and valid now
  #
  #    bt_history(Time.zone.now, Time.zone.now + 30.days)
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
    ttend_at == Forever
  end
  alias active? tt_forever?

  # Returns true if the record is inactive (i.e. transaction period is closed).
  def inactive?
    not active?
  end

  # Returns true if the valid period is open ended.
  def vt_forever?
    vtend_at == Forever
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

  def complete_bt_timestamps
    transaction_time = ttstart_at || Time.zone.now

    self.ttstart_at ||= transaction_time
    self.ttend_at ||= Forever

    self.vtstart_at ||= transaction_time
    self.vtend_at ||= Forever
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
  #   bt_delete                      # [AllTime, now]
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
    vt_range, commit_time = bt_coerce_slice(*args)
    ActiveRecord::Base.transaction do
      bt_history(vt_range).lock(true).map do |overlap|
        overlap.bt_finalize(commit_time)

        overlap.vt_range.difference(vt_range).each do |segment|
          bt_new_version(vtstart_at: segment.begin, vtend_at: segment.end).bt_commit(commit_time)
        end

        (block_given? && yield(overlap, vt_range, commit_time)) || overlap
      end
    end.tap do
      self.ttend_at = commit_time
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
      not self.class.where(id: id, ttend_at: Forever).update_all(ttend_at: commit_time).zero?
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
  def bt_revise(attrs={})
    raise ArgumentError, "invalid revision of non-current record" unless tt_forever?

    revision = bt_new_version(attrs)

    return [] if !changed? and revision.bt_same_snapshot?(self)

    bt_delete(revision.vtstart_at, revision.vtend_at) do |overlapped, vtrange, transaction_time|
      intersection = overlapped.vt_range.intersection(revision.vtstart_at, revision.vtend_at)
      revision.bt_new_version(vtstart_at: intersection.begin, vtend_at: intersection.end).bt_commit(transaction_time).first
    end
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

  # Returns attribute hash excluding the four temporal attributes.
  def bt_value_attributes
    attributes.slice(*(self.class.bt_scope_columns + self.class.bt_versioned_columns))
  end

  # Returns attribute hash including excluding the primary keys.
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
        table[start_column].lt(start_or_instant_or_range.end).and(table[end_column].gt(start_or_instant_or_range.begin))
      else
        start_or_instant_or_range ||= Forever
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
        vt_intersect(args.at(0)).tt_intersect(args.at(1))
      when 4
        where(arel_bt_intersect(*args))
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
      where(:vtend_at => Forever)
    end

    def tt_forever
      where(:ttend_at => Forever)
    end

    Tokens = ('A'..'Z').to_a.join
    def bt_ascii(detail=false)
      final = ""

      result = order(bt_scope_columns)
      records = result.group_by { |r| r.bt_scope_attributes }
      vt_ticks = records.map { |scope, list| list.map { |x| [x.vtstart_at, x.vtend_at]} }.flatten.uniq.sort

      row = 0
      records.each_with_index do |(scope, list), index|
      tt_ticks = list.map { |x| [x.ttstart_at, x.ttend_at]}.flatten.uniq.sort
      picture = Array.new(tt_ticks.size) { " " * vt_ticks.size }

      list.sort_by { |r| r.ttstart_at }.each_with_index do |record, version|
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
  def acts_as_bitemporal(*args)
    options = args.extract_options!
    bt_exclude_columns = %w{id type}    # AR maintains these columns

    include ActsAsBitemporal

    class_attribute :bt_scope_columns
    class_attribute :bt_versioned_columns
    class_attribute :bt_value_columns

    if bt_belongs_to = options.delete(:for)
      self.bt_scope_columns = [bt_belongs_to.foreign_key]     # Entity => entity_id
    elsif self.bt_scope_columns = options.delete(:scope)
      self.bt_scope_columns = Array(bt_scope_columns).map(&:to_s)
    else
      self.bt_scope_columns = self.column_names.grep /_id\z/
    end

    self.bt_value_columns = self.column_names - ActsAsBitemporal::TemporalColumnNames - bt_exclude_columns
    self.bt_versioned_columns = self.bt_value_columns - bt_scope_columns

    attr_accessor :bt_safe

    after_commit      :bt_after_commit
    before_validation :complete_bt_timestamps
    validate          :bt_scope_constraint

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
      :class => collection.classify.constantize,
    }

    has_many collection.to_sym, extend: ActsAsBitemporal::AssociationMethods

    define_method("bt_#{collection}") do |*args|
      send(collection.to_sym).bt_intersect(*args)
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
    assignments = "#{singular_sym}_assignments".to_sym
    shared = options.delete(:shared)

    info = bt_attributes[singular_sym] = {
      :type => :scalar, 
      :class => singular.classify.constantize,
      :shared => shared,
      :expose => options.delete(:expose) || [],
      :assignment_class => shared && assignments.to_s.classify.constantize,
    }

    if shared
      has_one_bitemporal assignments
      has_many plural_sym, through: assignments

      define_method("bt_#{plural_sym}".to_sym) do |*args| 
        info[:class].joins(assignments).
          merge(info[:assignment_class].bt_intersect(*args)).
          merge(info[:assignment_class].where(record_id: self))
      end

      define_method("bt_#{singular_sym}") { |*args| send("bt_#{plural_sym}", *args).first }
      define_method("bt_#{singular_sym}!") { |*args| send("bt_#{plural_sym}", *args).first! }

    else

      attr_list = info[:expose]

      has_many plural_sym,  extend: ActsAsBitemporal::AssociationMethods

      after_method = "bt_after_create_#{singular}"
      after_create after_method.to_sym

      define_method(after_method) do
        attributes = Hash[ attr_list.map { |a| [a, send("bta_#{singular_sym}_#{a}")] } ]
        # things         << (thing              ||        Thing.new( :thing_attr0 => bta_thing_attr0, :thing_attr1 => bta_thing_attr1)
        send(plural_sym) << (send(singular_sym) || info[:class].new(attributes))
      end

      define_method("bt_#{plural_sym}") { |*args| send(plural_sym).bt_intersect(*args) }
      define_method("bt_#{singular_sym}") { |*args| send("bt_#{plural_sym}").first }
      define_method("bt_#{singular_sym}!") { |*args| send("bt_#{plural_sym}").first! }

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
