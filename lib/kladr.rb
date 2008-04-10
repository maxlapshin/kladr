require 'dbf/dbf'
require 'iconv'
require 'active_support'
require 'active_record'
$KCODE = 'u'

class Kladr
  
  def self.exec_streets_schema
    ActiveRecord::Migration.create_table "streets" do |t|
      t.column "name", :string, :limit => 40
      t.column "street_code", :integer
      t.column "abbrev", :string, :limit => 10
    end
    ActiveRecord::Migration.add_index :streets, :street_code
    ActiveRecord::Migration.add_index :streets, :name
  end
  
  
  class Street < ActiveRecord::Base
    has_many :houses
  end   
  
  class House < ActiveRecord::Base
    belongs_to :street
  end
  
  def self.file_unpack(file)
    return if File.exists?(file)
    return unless File.exists?(file+".gz")
    `gzip -cd #{file}.gz > #{file}`
  end
  
  def self.recode(string)
    Iconv.iconv("UTF-8", "CP866", string).first
  end
  
  def self.street_import(file = File.dirname(__FILE__)+"/../BASE/STREET.DBF")
    start_time = Time.now
    file_unpack(file)
    table = Kladr::DBF::Table.new(file, :in_memory => false)
    table.columns.each {|c| c.name.replace(c.name.downcase) }
    
    exec_streets_schema rescue false
    
    table_columns = Street.columns.map(&:name)
    
    puts "Table created, importing #{table.record_count} records"
    count = 0
    0.upto(table.record_count-1) do |i|
      record = table.record(i) 
      next unless record
      city_code = record.attributes["code"][0,2].to_i
      street_code = record.attributes["code"][11, 4].to_i
      actuality_code = record.attributes["code"][15,2].to_i
      
      next unless city_code == 77 && actuality_code == 0
      attributes = {:street_code => street_code, :name => recode(record.attributes["name"]), :abbrev => recode(record.attributes["socr"])}.
        reject {|field, value| !table_columns.include?(field.to_s)}
      street = Street.create(attributes)
      puts ("%4d %s %s" % [street.street_code, street.abbrev, street.name]) 
      count += 1
      if count == 1
        puts "Starting Moscow on #{i} record"
      end
    end
    import_time = Time.now
    
    puts "It took #{import_time - start_time} seconds to import #{count} records. #{Time.now - import_time} to build index."
  end
  
  def self.exec_houses_schema
    ActiveRecord::Migration.create_table "houses" do |t|
      t.column "number", :string, :limit => 10
      t.column "street_code", :integer
      t.column "abbrev", :string, :limit => 10
      t.column "building", :integer
      t.column "index", :integer
      t.column "house_code", :integer
      t.column "street_id", :integer
    end
    ActiveRecord::Migration.add_index :houses, [:street_code, :house_code]
  end
  
  def self.houses_import(file = File.dirname(__FILE__)+"/../BASE/DOMA.DBF")
    start_time = Time.now
    file_unpack(file)
    table = Kladr::DBF::Table.new(file, :in_memory => false)
    table.columns.each {|c| c.name.replace(c.name.downcase) }
    
    exec_houses_schema rescue false
    
    puts "Table created, importing #{table.record_count} records"
    count = 0
    table_columns = House.columns.map(&:name)
    
    0.upto(table.record_count-1) do |i|
      record = table.record(i) 
      next unless record
      
      city_code = record.attributes["code"][0,2].to_i
      street_code = record.attributes["code"][11, 4].to_i
      house_code = record.attributes["code"][15, 4].to_i
      next unless city_code == 77
      next if street_code == 0
      attributes = {
        :street_code => street_code, :house_code => house_code, :abbrev => recode(record.attributes["socr"]).chars.downcase.to_s,
        :building => recode(record.attributes["korp"]), :index => record.attributes["index"].to_i
      }.reject {|field, value| !table_columns.include?(field.to_s)}
      attributes[:numbers] = recode(record.attributes["name"])
      street = Street.find_by_street_code(street_code)
      attributes[:street_id] = street.id if street
      houses = create_houses(attributes)
      count += houses.length
      if count == 1
        puts "Starting Moscow on #{i} record"
      end
    end
    puts "It took #{Time.now - start_time} seconds to import #{count} records."  
  end
  
  def self.extract_numbers(numbers)
    return [] unless numbers
    numbers.split(",").map do |part|
      if part.index("-")
        start_number, end_number = /(\d+)-(\d+)/.match(part).captures.map(&:to_i)
        step = part.index("(") ? 2 : 1
        res = []
        (start_number..end_number).step(step) {|i| res << i.to_s}
        res
      else
        part
      end
    end.flatten
  end
  
  def self.create_houses(attributes)
    numbers = extract_numbers(attributes.delete(:numbers))
    numbers.each do |number|
      house = House.create(attributes.merge(:number => number))
      puts ("%30s %4s" % [house.street && house.street.name || "-", house.number]) 
    end
  end
  
  def self.import
    street_import
    houses_import
  end  
  
end