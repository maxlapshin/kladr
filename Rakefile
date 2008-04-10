require 'rubygems'
require 'rake'
require 'rake/testtask'
require 'rake/gempackagetask'

spec = Gem::Specification.new do |s|
  s.name = 'kladr'
  s.version = '0.1'
  s.summary = 'Importer of russian classificator of addresses'
#  s.autorequire = 'attacheable'
  s.author  = "Max Lapshin"
  s.email   = "max@maxidoors.ru"
  s.description = ""
  s.rubyforge_project = "kladr"
  s.has_rdoc          = false
  s.files = FileList["**/**"].exclude(".git").to_a
  
end

Rake::GemPackageTask.new(spec) do |package|
  package.gem_spec = spec
end


task :default => [ :test ]

desc "Run all tests"
Rake::TestTask.new("test") { |t|
  t.libs << "test"
  t.pattern = 'test/*_test.rb'
  t.verbose = true
}
