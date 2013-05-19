# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'acts_as_bitemporal/version'

Gem::Specification.new do |gem|
  gem.name          = "acts_as_bitemporal"
  gem.version       = ActsAsBitemporal::VERSION
  gem.authors       = ["Gary Wright"]
  gem.email         = ["gary.r.wright@mac.com"]
  gem.description   = %q{Bitemporal versioning of Active Record models}
  gem.summary       = %q{Bitemporal versioning of Active Record models}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_dependency 'activerecord'
  gem.add_dependency 'activesupport'
  gem.add_development_dependency 'sqlite3'
  gem.add_development_dependency 'pg'
  gem.add_development_dependency 'shoulda-context'
  gem.add_development_dependency 'autotest'
  gem.add_development_dependency 'ZenTest', '~> 4.9.0'
  gem.add_development_dependency 'autotest-standalone'
  gem.add_development_dependency 'autotest-fsevent'
  gem.add_development_dependency 'autotest-growl'
end
