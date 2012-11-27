# encoding: utf-8
require "acts_as_bitemporal/version"
require 'active_support'
require 'active_record'

module ActsAsBitemporal

  # Columns to be managed by ActsAsBitemporal
  TemporalColumnNames = %w{vtstart_at vtend_at ttstart_at ttend_at}

  extend ActiveSupport::Concern

  included do
    before_validation :complete_bt_timestamps, :on => :create
    validate          :bitemporal_key_constraint
  end

  # The new record can not have a valid time period that overlaps with any existing record for the same entity.
  def bitemporal_key_constraint
    if bt_images.
      valid_during(vtstart_at, vtend_at).
      known_at(ttstart_at).exists?
      errors[:base] << "overlaps existing valid record"
    end
  end

  # Return relation that evalutes to all images of the current record.
  def bt_images
    self.class.where(bt_key_conditions)
  end

  # Arel expresstion to select records with same key attributes as this record.
  def bt_key_conditions
    table = self.class.arel_table
    self.class.bt_key_attrs.map do |key_attr| 
      table[key_attr].eq(self[key_attr]) 
    end.inject do |memo, condition| 
      memo.and(condition)
    end
  end

  # The timestamp used to signify an indefinite end of a time period.
  Forever = DateTime.parse("9999-12-31T23:59:59.999999+00:00")

  # Does the transaction time overlap 'as_of'?
  def tt_cover?(as_of)
    ttstart_at <= as_of and as_of < ttend_at
  end

  # Does the transaction time end at 'forever'?
  def tt_forever?
    ttend_at == Forever
  end

  # Does the valid time span overlap time as_of.
  def vt_cover?(as_of)
    vtstart_at <= as_of and as_of < vtend_at
  end

  # Does the valid time end at 'forever'?
  def vt_forever?
    vtend_at == Forever
  end


  def complete_bt_timestamps
    transaction_time = Time.now
    self.vtstart_at ||= transaction_time
    self.ttstart_at ||= transaction_time
  end

  # Replace current version with new version.
  def bt_update_attributes(new_attrs)
    commit_time = Time.now

    new_record = nil

    ActiveRecord::Base.transaction do
      if tt_forever? and vt_cover?(commit_time)
        # close the transaction span for existing record
        update_column(:ttend_at, commit_time)

        # New data splits the existing valid_time span.
        # Record new data for the remaining span.
        new_record = self.class.bt_new(bt_attribute_merge(new_attrs).merge(vtstart_at: commit_time, vtend_at: vtend_at, ttstart_at: commit_time, ttend_at: ActsAsBitemporal::Forever))
        new_record.save!

        # Record current fields for the preceeding span.
        self.class.bt_new(bt_attributes_without_timestamps.merge(vtstart_at: vtstart_at, vtend_at: commit_time, ttstart_at: commit_time, ttend_at: ActsAsBitemporal::Forever)).save!

      end

      # Adjust records scheduled in future that intersect valid period.
      bt_images.known_now.where(['vtstart_at > ?', commit_time]).each do |future_rec|
        self.class.bt_new(future_rec.bt_attributes_without_timestamps.merge(ttstart_at: commit_time, ttend_at: ActsAsBitemporal::Forever)).save!
        future_rec.update_column(:ttend_at, commit_time)
      end
    end

    new_record
  end

  # Return attributes excluding bitemporal timestamps.
  def bt_attributes_without_timestamps
    attributes.tap { |a| ActsAsBitemporal::TemporalColumnNames.each { |tcol| a.delete(tcol) } }
  end

  # Create a new attribute hash based on the current attributes.
  #   bitemporal timestamps are excluded
  #   key columns are ignored
  def bt_attribute_merge(new_attrs)
    attributes = bt_attributes_without_timestamps
    new_attrs = new_attrs.stringify_keys
    self.class.bt_versioned_attrs.each do |aname|
      attributes[aname] = new_attrs.fetch(aname, read_attribute(aname))
    end
    attributes
  end

  module ClassMethods
    def current
      now = Time.now
      vtstart_at(now).known_at(now)
    end

    def current!
      current.first!
    end

    def vtstart_at(time)
      table = self.arel_table
      where( table[:vtstart_at].lteq(time).and(table[:vtend_at].gt(time)) )
    end

    def known_at(time=Time.now)
      table = self.arel_table
      where( table[:ttstart_at].lteq(time).and(table[:ttend_at].gt(time)) )
    end

    def valid_during(valid_start, valid_end)
      table = self.arel_table
      where(table[:vtstart_at].lt(valid_end).and(table[:vtend_at].gt(valid_start)))
    end

    def known_now
      table = self.arel_table
      where( table[:ttend_at].eq(ActsAsBitemporal::Forever) )
    end

    # Create a new record that is valid starting now. The actual
    # timestamp is established when the record is saved.
    def bt_new(attrs={})
      attrs = attrs.reverse_merge(
        vtstart_at: nil, vtend_at: ActsAsBitemporal::Forever, 
        ttstart_at: nil, ttend_at: ActsAsBitemporal::Forever
      )
      new(attrs)
    end


    # Verify bitemporal key constraints
    def bitemporal_key_constraint_table
      n = Name.arel_table
      n1 = n.alias('n1')
      n2 = n.alias('n2')
      subquery = n.from(n2).project(n2[:entity_id].count).
        where(n1[:entity_id].eq(n2[:entity_id])).
        where(n1[:vtstart_at].lt(n2[:vtend_at])).
        where(n2[:vtstart_at].lt(n1[:vtend_at])).
        where(n1[:ttend_at].eq( ActsAsBitemporal::Forever)).
        where(n2[:ttend_at].eq( ActsAsBitemporal::Forever))
      subquery2 = n.from(n1).project(n1[:entity_id], n1[:vtstart_at], n1[:vtend_at], n1[:ttend_at]).where(Arel::SqlLiteral.new("(#{subquery.to_sql})").gt(1))
      ActiveRecord::Base.connection.execute("select #{subquery2.exists.not.to_sql}").values == "t"
    end

  end

  module AssociationMethods
    def bt_build(attrs={})
      transaction_time = Time.now
      build(attrs.reverse_merge(
        vtstart_at: nil, vtend_at: ActsAsBitemporal::Forever, 
        ttstart_at: nil, ttend_at: ActsAsBitemporal::Forever
      ))
    end
  end

  module TableDefinitionHelper
    def bt_timestamps(options={})
      column(:vtstart_at, :datetime, options)
      column(:vtend_at, :datetime, options)
      column(:ttstart_at, :datetime, options)
      column(:ttend_at, :datetime, options)
    end
  end

  class ActiveRecord::ConnectionAdapters::TableDefinition
    include TableDefinitionHelper
  end

end

class << ActiveRecord::Base
  def acts_as_bitemporal(*args)
    options = args.extract_options!

    include ActsAsBitemporal

    raise ArgumentError, "must specify :for option" unless bt_belongs_to = options[:for]

    class_attribute :bt_key_attrs
    class_attribute :bt_versioned_attrs
    self.bt_key_attrs = [bt_belongs_to.foreign_key]     # Entity => entity_id
    self.bt_versioned_attrs = self.column_names - ActsAsBitemporal::TemporalColumnNames - bt_key_attrs - ["id"]
  end
end
