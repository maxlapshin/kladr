$:.unshift("lib")
require 'rubygems'
require 'active_record'
require 'kladr'

ActiveRecord::Base.establish_connection(:adapter => "sqlite3", :dbfile => "kladr.sqlite3")

