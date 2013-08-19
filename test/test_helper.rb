require 'active_record'
require 'active_support/test_case'
require 'active_support/time'
require 'active_support/core_ext/hash'
require 'test/unit'
require 'shoulda-context'

Time.zone = "UTC"   # Force to UTC
ActiveRecord::Base.time_zone_aware_attributes = true

# this is to make absolutely sure we test this one, not the one
# installed on the system.
require File.expand_path('../../lib/acts_as_bitemporal', __FILE__)

require 'debugger' if RUBY_VERSION =~ /\A1.9/

# This file is based on the test code in the Ancestry gem: https://github.com/stefankroes/ancestry


class ActsAsBitemporalTestDatabase
  TestTable = 'bt_recs'

  def self.setup
    db_driver = ENV['DB'] || 'postgresql'
    ActiveRecord::Base.logger = ActiveSupport::BufferedLogger.new("log/#{db_driver}_test.log")
    ActiveRecord::Base.establish_connection YAML.load(File.open(File.join(File.dirname(__FILE__), 'database.yml')).read)[db_driver]
  end

  def self.with_model options = {}
    options           = options.dup
    extra_columns     = options.delete(:extra_columns) || []
    model_name        = options.delete(:model_name) || "BtRec"

    ActiveRecord::Base.connection.create_table TestTable do |table|
      table.bt_timestamps
      extra_columns.each do |name, type|
        table.send type, name
      end
    end
    
    begin
      model = Class.new(ActiveRecord::Base)
      const_set model_name, model

      model.table_name = TestTable
      #model.send :default_scope, default_scope_params if default_scope_params.present?

      model.acts_as_bitemporal options

      yield model
    ensure
      model.reset_column_information
      ActiveRecord::Base.connection.drop_table TestTable
      remove_const model_name
    end
  end
end

ActsAsBitemporalTestDatabase.setup

puts "\nRunning ActsAsBitemporal test suite:"
puts "  Ruby: #{RUBY_VERSION}"
puts "  ActiveRecord: #{ActiveRecord::VERSION::STRING}"
puts "  Database: #{ActiveRecord::Base.connection.adapter_name}\n\n"

